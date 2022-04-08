/* -------------------------------------------------------------------------
 *
 * pg_ctl --- start/stops/restarts the openGauss server
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/bin/pg_ctl/pg_ctl.c
 *
 * -------------------------------------------------------------------------
 */

#ifdef WIN32
/*
 * Need this to get defines for restricted tokens and jobs. And it
 * has to be set before any header from the Win32 API is loaded.
 */
#define _WIN32_WINNT 0x0501
#endif

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"

#include <fcntl.h>
#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "libpq/pqsignal.h"
#include "getopt_long.h"
#include "miscadmin.h"
#include "flock.h"
#include "file_ops.h"
#include "backup.h"
#include "pg_build.h"
#include "streamutil.h"
#include "bin/elog.h"
#include "common/build_query/build_query.h"
#include "replication/replicainternal.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "hotpatch/hotpatch_client.h"
#include "hotpatch/hotpatch.h"
#include "pg_rewind.h"
#include "fetch.h"
#include "common/fe_memutils.h"
#include "logging.h"

#ifdef ENABLE_MOT
#include "fetchmot.h"
#endif

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 1024
#endif
#define PROG_NAME "gs_ctl"
#define PG_REWIND_VERSION "(PostgreSQL) 9.2.4"
#define PG_REWIND_VERSIONSTR "gs_rewind " DEF_GS_VERSION "\n"

#define PROC_NET_TCP "/proc/net/tcp"
#define LISTEN 10

#if defined(__CYGWIN__)
#include <sys/cygwin.h>
#include <windows.h>
/* Cygwin defines WIN32 in windows.h, but we don't want it. */
#undef WIN32
#endif

#ifdef ENABLE_UT
#define static
#endif

/* PID can be negative for standalone backend */
typedef long pgpid_t;

typedef enum { SMART_MODE, FAST_MODE, IMMEDIATE_MODE } ShutdownMode;

typedef enum {
    NO_COMMAND = 0,
    INIT_COMMAND,
    START_COMMAND,
    RESTART_COMMAND,
    STOP_COMMAND,
    RELOAD_COMMAND,
    STATUS_COMMAND,
    KILL_COMMAND,
    REGISTER_COMMAND,
    UNREGISTER_COMMAND,
    RUN_AS_SERVICE_COMMAND,
    FAILOVER_COMMAND,
    SWITCHOVER_COMMAND,
    NOTIFY_COMMAND,
    QUERY_COMMAND,
    BUILD_COMMAND,
    BUILD_QUERY_COMMAND,
    RESTORE_COMMAND,
    HOTPATCH_COMMAND,
    FINISH_REDO_COMMAND,
    ADD_MEMBER_COMMAND,
    REMOVE_MEMBER_COMMAND,
    CHANGE_ROLE_COMMAND,
    MINORITY_START_COMMAND,
    COPY_COMMAND
} CtlCommand;

typedef enum {
    SYNCHRONOUS_COMMIT_OFF,            /* asynchronous commit */
    SYNCHRONOUS_COMMIT_LOCAL_FLUSH,    /* wait for local flush only */
    SYNCHRONOUS_COMMIT_REMOTE_RECEIVE, /* wait for local flush and remote receive */
    SYNCHRONOUS_COMMIT_REMOTE_WRITE,   /* wait for local flush and remote write */
    SYNCHRONOUS_COMMIT_REMOTE_FLUSH,   /* wait for local and remote flush */
    SYNCHRONOUS_COMMIT_REMOTE_APPLY,   /* wait for local and remote replay */
    SYNCHRONOUS_BAD
} SyncCommitLevel;

typedef enum {
    UNKNOWN_OPERATION = 0,
    ADD_OPERATION,
    REMOVE_OPERATION,
    CHANGE_OPERATION
} MemberOperation;

#define MAX_PERCENT 100
#define FAIL_PERCENT -1

#define DEFAULT_WAIT 60
#define MAX_JOB_NAME 128

#define MAX_DISPLAY_STRING_LEN 1024
#define PG_CTL_LOCKFILE_SIZE 1024
#define FORMATTED_TS_LEN 128
static const int MAX_STOP_BARRIER_LEN = MAX_BARRIER_ID_LENGTH + 3;
static bool do_wait = false;
static bool wait_set = false;
static int wait_seconds = DEFAULT_WAIT;

static bool silent_mode = false;
static ShutdownMode shutdown_mode = FAST_MODE;
BuildMode build_mode = AUTO_BUILD; /* default */
static char* stop_mode = "fast";
static DemoteMode switch_mode = FastDemote; /* default */
static int sig = SIGTERM;                   /* default */
static CtlCommand ctl_command = NO_COMMAND;
char* pg_data = NULL;
static char* pg_config = NULL;
static char* pgdata_opt = NULL;
static char* post_opts = NULL;
static char* xlog_overwrite_opt = NULL;
const char* progname = "gs_ctl";
static char* log_file = NULL;
static char* key_cn = NULL;
char* slotname = NULL;
char* taskid = NULL;
char* conn_str = NULL;
static char* exec_path = NULL;
static char* register_servicename = "gaussdb";
static char g_hotpatch_action[g_max_length_act] = {0};
static char g_hotpatch_name[g_max_length_path] = {0};
static bool clear_backup_dir = false;
static char barrier_id[MAX_STOP_BARRIER_LEN] = {0};
static char* stop_barrier = NULL;
static char* argv0 = NULL;
static bool allow_core_files = false;
static time_t start_time;

static char standbymode_str[] = "-M standby";
static char* pgha_str = NULL;
static char* pgha_opt = NULL;
static char postopts_file[MAXPGPATH];
char pid_file[MAXPGPATH];
static char backup_file[MAXPGPATH];
static char recovery_file[MAXPGPATH];
static char recovery_done_file[MAXPGPATH];
static char failover_file[MAXPGPATH];
static char switchover_file[MAXPGPATH];
static char timeout_file[MAXPGPATH];
static char switchover_status_file[MAXPGPATH];
static char setrunmode_status_file[MAXPGPATH];
static char g_changeroleStatusFile[MAXPGPATH];
static char primary_file[MAXPGPATH];
static char standby_file[MAXPGPATH];
static char cascade_standby_file[MAXPGPATH];
static char pg_ctl_lockfile[MAXPGPATH];
static char pg_conf_file[MAXPGPATH];
static FILE* lockfile = NULL;
static FILE* g_hotpatch_lock_fd = NULL;
static char build_pid_file[MAXPGPATH];
static char g_hotpatch_cmd_file[MAXPGPATH];
static char g_hotpatch_tmp_cmd_file[MAXPGPATH];
static char g_hotpatch_ret_file[MAXPGPATH];
static char g_hotpatch_lockfile[MAXPGPATH];
char gaussdb_state_file[MAXPGPATH] = {0};
static char add_member_file[MAXPGPATH];
static char remove_member_file[MAXPGPATH];
static char change_role_file[MAXPGPATH];
static char* new_role = "passive";
static char start_minority_file[MAXPGPATH];
static unsigned int vote_num = 0;
static unsigned int xmode = 2;
static char postport_lock_file[MAXPGPATH];
PGconn* dbConn = NULL;
static MemberOperation member_operation = UNKNOWN_OPERATION;
static unsigned int new_node_port = 0;
static char new_node_ip[IP_LEN] = {0};
static unsigned int new_node_id = 0;
static int group = -1;
static int priority = -1;
static bool g_dcfEnabled = false;
bool no_need_fsync = false;
bool need_copy_upgrade_file = false;
pid_t process_id = 0;

const int g_length_stop_char = 2;
const int g_length_suffix = 3;
const static int INC_BUILD_RETRY_TIMES = 3;

bool g_is_obsmode = false;

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr) \
    do {                    \
        if (NULL != ptr) {  \
            free(ptr);      \
            ptr = NULL;     \
        }                   \
    } while (0)
#endif

/* max length of password */
#define MAX_PASSWORD_LENGTH 999

char ssl_cert_file[MAXPGPATH];
char ssl_key_file[MAXPGPATH];
char ssl_ca_file[MAXPGPATH];
char ssl_crl_file[MAXPGPATH];
char* ssl_cipher_file = "server.key.cipher";
char* ssl_rand_file = "server.key.rand";

char pgxc_node_name[MAXPGPATH];

static volatile pgpid_t postmasterPID = -1;

#if defined(WIN32) || defined(__CYGWIN__)
static DWORD pgctl_start_type = SERVICE_AUTO_START;
static SERVICE_STATUS status;
static SERVICE_STATUS_HANDLE hStatus = (SERVICE_STATUS_HANDLE)0;
static HANDLE shutdownHandles[2];

#define shutdownEvent shutdownHandles[0]
#define postmasterProcess shutdownHandles[1]
#endif

#ifdef PGXC
static char* pgxcCommand = NULL;
#endif

static XLogRecPtr querylsn = InvalidXLogRecPtr;
static bool islsnquery = false;
static bool needstartafterbuild = true;

static void do_advice(void);
static void do_help(void);
static void set_mode(char* modeopt);
static void set_sig(char* signame);
static void do_init(void);
static void do_start_set_para(void);
static void do_start(void);
static void do_stop(bool force);
static void do_restart(void);
static void do_reload(void);
static void do_finish_redo(void);
static void do_status(void);

static void do_kill(pgpid_t pid);
static void print_msg(const char* msg);
static void adjust_data_dir(void);
static void set_member_operation(const char* operationopt);

#if defined(WIN32) || defined(__CYGWIN__)
static bool pgwin32_IsInstalled(SC_HANDLE);
static char* pgwin32_CommandLine(bool);
static void pgwin32_doRegister(void);
static void pgwin32_doUnregister(void);
static void pgwin32_SetServiceStatus(DWORD);
static void WINAPI pgwin32_ServiceHandler(DWORD);
static void WINAPI pgwin32_ServiceMain(DWORD, LPTSTR*);
static void pgwin32_doRunAsService(void);
static int CreateRestrictedProcess(char* cmd, PROCESS_INFORMATION* processInfo, bool as_service);
#endif

static pgpid_t get_pgpid(void);
static pgpid_t start_postmaster(void);
static void read_post_opts(void);

static PGPing test_postmaster_connection(pgpid_t pm_pid, bool do_checkpoint, struct stat beforeStat);
static bool postmaster_is_alive(pid_t pid);

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
static void unlimit_core_size(void);
#endif

static void do_notify(uint32 term);
static bool get_conn_exe_sql(const char* sqlCommond);
static void do_query(void);
static int pg_ctl_lock(const char* lock_file, FILE** lockfile_fd);
static int pg_ctl_unlock(FILE* lockfile_fd);
static void do_build_query(void);
static void display_build_query(const GaussState* state);
static void display_query(GaussState* state, const char* errormsg);
static bool is_process_alive(pgpid_t pid);
static char* get_string_by_mode(ServerMode s_mode);
static char* get_string_by_state(DbState db_state);
void ReadDBStateFile(GaussState* state);
static void do_failover(uint32 term);
static void do_switchover(uint32 term);
static pgpid_t get_pgdbpid_for_stop(void);
static pgpid_t get_build_pid(void);
static void set_build_pid(pgpid_t pid);
static void close_connection(void);
static PGconn* get_connectionex(void);
static ServerMode get_runmode(void);
static void freefile(char** lines);
static char* get_localrole_string(ServerMode mode);
static bool do_actual_build(uint32 term = 0);
static bool do_incremental_build(uint32 term = 0);
static bool do_incremental_build_xlog();
static void do_build(uint32 term = 0);
static void do_restore(void);
static void do_overwrite(void);
static void do_full_restore(void);
static void kill_proton_force(void);
static void SigAlarmHandler(int arg);
int ExecuteCmd(const char* command, struct timeval timeout);

static int find_guc_optval(const char** optlines, const char* optname, char* optval);
static void read_ssl_confval(void);
static char* get_string_by_sync_mode(bool syncmode);
static void free_ctl();
extern int GetLengthAndCheckReplConn(const char* ConnInfoList);
extern BuildErrorCode gs_increment_build(const char* pgdata, const char* connstr, char* sysidentifier, uint32 timeline, uint32 term);
const char *BuildModeToString(BuildMode mode);

void check_input_for_security(char* input_env_value)
{
    if (input_env_value == NULL)
        return;

    const char* danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "!", NULL};
    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i]) != NULL) {
            pg_log(PG_WARNING,
                "Error: variable \"%s\" token invaild symbol \"%s\".\n",
                input_env_value,
                danger_character_list[i]);
            exit(1);
        }
    }
}

/*
 * Given an already-localized string, print it to stdout unless the
 * user has specified that no messages should be printed.
 */
static void print_msg(const char* msg)
{
    if (!silent_mode) {
        fputs(msg, stdout);
        fflush(stdout);
    }
}

static pgpid_t get_pgpid(void)
{
    FILE* pidf = NULL;
    long pid;

    pidf = fopen(pid_file, "r");
    if (pidf == NULL) {
        /* No pid file, not an error on startup */
        if (errno == ENOENT)
            return 0;
        else {
            pg_log(PG_WARNING, _(" could not open PID file \"%s\": %s\n"), pid_file, strerror(errno));
            exit(1);
        }
    }
    if (fscanf_s(pidf, "%ld", &pid) != 1) {
        pg_log(PG_WARNING, _(" invalid data in PID file \"%s\"\n"), pid_file);
        fclose(pidf);
        pidf = NULL;
        exit(1);
    }
    fclose(pidf);
    pidf = NULL;
    return (pgpid_t)pid;
}

static pgpid_t get_pgdbpid_for_stop(void)
{
    FILE* pidfp = NULL;
    long pid;
    struct stat st;

    pidfp = fopen(pid_file, "r");
    if (pidfp == NULL) {
        /* No pid file, not an error on startup */
        if (errno == ENOENT) {
            return 0;
        } else {
            pg_log(PG_WARNING, _(" could not open PID file \"%s\": %s\n"), pid_file, strerror(errno));
            exit(1);
        }
    }
    if (fscanf_s(pidfp, "%10ld", &pid) != 1) {
        pg_log(PG_WARNING, _(" invalid data in PID file \"%s\"\n"), pid_file);
        if (shutdown_mode == IMMEDIATE_MODE) {
            kill_proton_force();

            if (stat(pid_file, &st) != 0) {
                pg_log(PG_WARNING, _("could not stat directory or file pid_file %s: %s\n"), pid_file, strerror(errno));
            }

            /* if it is a symnol, chmod will change the auth of the true file */
            if (S_ISLNK(st.st_mode)) {
                pg_log(PG_WARNING, _("the file being chmod is a symbol link\n"));
            }

            if (chmod(pid_file, 0600) == -1) {
                pg_log(PG_WARNING, _(" could not set permissions of file \"%s\": %m\n"), pid_file);
            }
            if (unlink(pid_file) < 0) {
                pg_log(PG_WARNING, _("Delete %s failed ,please try to delete it manually again.\n"), pid_file);
            }
            fclose(pidfp);
            pidfp = NULL;
            return 0;
        }
        fclose(pidfp);
        pidfp = NULL;
        exit(1);
    }
    fclose(pidfp);
    pidfp = NULL;
    return (pgpid_t)pid;
}

static PGconn* get_connection(void)
{
#define MAXCONNINFO 1024
    char** optlines;
    long pmpid;
    int ret;

    if (dbConn != NULL) {
        if (PQstatus(dbConn) == CONNECTION_OK) {
            return dbConn;
        } else {
            close_connection();
        }
    }

    /* Try to read the postmaster.pid file */
    if ((optlines = readfile(pid_file)) == NULL || optlines[0] == NULL || optlines[1] == NULL || optlines[2] == NULL) {
        freefile(optlines);
        optlines = NULL;
        return NULL;
    }

    if (optlines[3] == NULL) {
        /* File is exactly three lines, must be pre-9.1 */
        pg_log(PG_WARNING, _("-w option is not supported when starting a pre-9.1 server\n"));
        freefile(optlines);
        optlines = NULL;
        return NULL;
    }

    if (optlines[4] == NULL || optlines[5] == NULL) {
        freefile(optlines);
        optlines = NULL;
        return NULL;
    }

    /* File is complete enough for us, parse it */
    pmpid = atol(optlines[LOCK_FILE_LINE_PID - 1]);
    if (pmpid > 0) {
        /*
         * OK, seems to be a valid pidfile from our child.
         */
        int portnum = 0;
        char* sockdir = NULL;
        char* hostaddr = NULL;
        char host_str[MAXPGPATH];
        char local_conninfo[MAXCONNINFO] = {0};
        char* cptr = NULL;
        errno_t rc = EOK;

        rc = memset_s(host_str, MAXPGPATH, 0, MAXPGPATH);
        securec_check_c(rc, "", "");

        /*
         * Extract port number and host string to use.
         * We used to prefer unix domain socket.
         * With thread pool, we prefer tcp port and connect to cn/dn ha port
         * so that we do not need to be queued by thread pool controller.
         */
        portnum = atoi(optlines[LOCK_FILE_LINE_PORT - 1]);
        sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
        hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

        if (hostaddr != NULL && hostaddr[0] != '\0' && hostaddr[0] != '\n') {
            rc = strncpy_s(host_str, sizeof(host_str), hostaddr, strlen(hostaddr));
            /* ha port equals normal port plus 1, required by om */
            portnum++;
        } else if (sockdir[0] == '/') {
            rc = strncpy_s(host_str, sizeof(host_str), sockdir, strlen(sockdir));
        }

        securec_check_c(rc, "", "");

        /* remove trailing newline */
        cptr = strchr(host_str, '\n');
        if (cptr != NULL)
            *cptr = '\0';

        /* Fail if couldn't get either sockdir or host addr */
        if (host_str[0] == '\0') {
            pg_log(PG_WARNING, _("\n-w option cannot use a relative socket directory specification\n"));
            freefile(optlines);
            optlines = NULL;
            return NULL;
        }

        /* If postmaster is listening on "*", use localhost */
        if (strcmp(host_str, "*") == 0) {
            rc = strncpy_s(host_str, sizeof(host_str), "localhost", sizeof(host_str) - 1);
            securec_check_c(rc, "", "");
        }

        /* add user and passwd for gs_ctl if needed */
        while (true) {
            if (register_username != NULL && register_password != NULL) {
                if (*register_username == '.') {
                    register_username += 2;
                }
                ret = snprintf_s(local_conninfo,
                    sizeof(local_conninfo),
                    sizeof(local_conninfo) - 1,
                    "dbname=postgres port=%d host='%s' user='%s' password='%s' "
                    "application_name=%s connect_timeout=5 options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
                    portnum,
                    host_str,
                    register_username,
                    register_password,
                    progname);
                securec_check_ss_c(ret, "\0", "\0");
            } else if (register_username == NULL && register_password != NULL) {
                ret = snprintf_s(local_conninfo,
                    sizeof(local_conninfo),
                    sizeof(local_conninfo) - 1,
                    "dbname=postgres port=%d host='%s' password='%s' "
                    "application_name=%s connect_timeout=5 options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
                    portnum,
                    host_str,
                    register_password,
                    progname);
                securec_check_ss_c(ret, "\0", "\0");
            } else if (register_username != NULL && register_password == NULL) {
                if (*register_username == '.') {
                    register_username += 2;
                }
                ret = snprintf_s(local_conninfo,
                    sizeof(local_conninfo),
                    sizeof(local_conninfo) - 1,
                    "dbname=postgres port=%d host='%s' user='%s' application_name=%s "
                    "connect_timeout=5 options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
                    portnum,
                    host_str,
                    register_username,
                    progname);
                securec_check_ss_c(ret, "\0", "\0");
            } else {
                ret = snprintf_s(local_conninfo,
                    sizeof(local_conninfo),
                    sizeof(local_conninfo) - 1,
                    "dbname=postgres port=%d host='%s' application_name=%s "
                    "connect_timeout=5 options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
                    portnum,
                    host_str,
                    progname);
                securec_check_ss_c(ret, "\0", "\0");
            }

            dbConn = PQconnectdb(local_conninfo);
            if (dbConn->status == CONNECTION_BAD && (strstr(PQerrorMessage(dbConn), "password") != NULL) &&
                register_password == NULL) {
                PQfinish(dbConn);
                if (register_username == NULL) {
                    register_password = simple_prompt("Password: ", MAX_PASSWORD_LENGTH, false);
                } else {
                    char* prompt_text = NULL;
                    prompt_text = (char*)pg_malloc(strlen(register_username) + MAX_PASSWORD_LENGTH);
                    ret = sprintf_s(prompt_text, strlen(register_username) + MAX_PASSWORD_LENGTH, _("Password for user %s: "), register_username);
                    securec_check_ss_c(ret, "\0", "\0");
                    register_password = simple_prompt(prompt_text, MAX_PASSWORD_LENGTH, false);
                    free(prompt_text);
                    prompt_text = NULL;
                }
            } else {
                break;
            }
        }

        rc = memset_s(local_conninfo, MAXCONNINFO, 0, sizeof(local_conninfo));
        securec_check_c(rc, "", "");
    }

    freefile(optlines);
    optlines = NULL;
    return dbConn;
}

static void close_connection()
{
    PQfinish(dbConn);
    dbConn = NULL;
}

static PGconn* get_connectionex(void)
{
#define MAXTRYTIME 3
    PGconn* conn = NULL;
    int i = 0;
    struct stat statbuf;

    for (;;) {
        conn = get_connection();
        /* connect sucess,break loop */
        if (PQstatus(conn) == CONNECTION_OK)
            break;

        /* doesn't exist postport.lock,need not to try again,directly break */
        if (stat(postport_lock_file, &statbuf) != 0)
            break;

        /* most try MAXTRYTIME times */
        if (++i >= MAXTRYTIME) {
            break;
        }

        close_connection();
        /* postport.lock exist and connect fail,need try again */
        pg_usleep(1000000); /* 1 sec */
    }

    return conn;
}

static ServerMode get_runmode(void)
{
#define MAXRUNMODE 64
    PGconn* conn = NULL;
    PGresult* res = NULL;
    const char* sql_string = "select local_role from pg_stat_get_stream_replications();";
    char* val = NULL;
    char run_mode[MAXRUNMODE] = {0};
    GaussState state;
    errno_t tnRet = EOK;

    conn = get_connectionex();
    if (PQstatus(conn) != CONNECTION_OK) {
        tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
        securec_check_c(tnRet, "\0", "\0");

        if (ctl_command == BUILD_QUERY_COMMAND) {
            pg_log(PG_WARNING, _("could not connect to the local server: connection failed!\n"));
            close_connection();
            conn = NULL;
            return UNKNOWN_MODE;
        }
        pg_log(PG_PROGRESS, _("Getting state from gaussdb.state!\n"));
        ReadDBStateFile(&state);
        return state.mode;
    }

    /* Get local role from the local server. */
    res = PQexec(conn, sql_string);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        pg_log(PG_WARNING, _("could not get local role from the local server: %s"), PQerrorMessage(conn));
        close_connection();
        conn = NULL;
        return UNKNOWN_MODE;
    }
    if (PQnfields(res) != 1 || PQntuples(res) != 1) {
        int ntuples = PQntuples(res);
        int nfields = PQnfields(res);

        PQclear(res);
        pg_log(PG_WARNING,
            _("invalid response from primary server: "
              "Expected 1 tuple with 1 fields, got %d tuples with %d fields."),
            ntuples,
            nfields);
        close_connection();
        conn = NULL;
        return UNKNOWN_MODE;
    }

    if ((val = PQgetvalue(res, 0, 0)) != NULL) {
        tnRet = strncpy_s(run_mode, MAXRUNMODE, val, strlen(val));
        securec_check_c(tnRet, "\0", "\0");

        run_mode[MAXRUNMODE - 1] = '\0';
    }
    PQclear(res);

    close_connection();
    conn = NULL;

    if (!strncmp(run_mode, "Normal", MAXRUNMODE))
        return NORMAL_MODE;
    if (!strncmp(run_mode, "Primary", MAXRUNMODE))
        return PRIMARY_MODE;
    if (!strncmp(run_mode, "Standby", MAXRUNMODE))
        return STANDBY_MODE;
    if (!strncmp(run_mode, "Cascade Standby", MAXRUNMODE))
        return CASCADE_STANDBY_MODE;
    if (!strncmp(run_mode, "Main Standby", MAXRUNMODE))
        return STANDBY_MODE;
    if (!strncmp(run_mode, "Pending", MAXRUNMODE))
        return PENDING_MODE;
    if (!strncmp(run_mode, "Unknown", MAXRUNMODE))
        return UNKNOWN_MODE;

    return UNKNOWN_MODE;
}

static void freefile(char** lines)
{
    char** line = NULL;
    if (lines == NULL)
        return;
    line = lines;
    while (*line != NULL) {
        free(*line);
        *line = NULL;
        line++;
    }
    free(lines);
}

/*
 * start/test/stop routines
 */

/*
 * Start the postmaster and return its PID.
 *
 * Currently, on Windows what we return is the PID of the shell process
 * that launched the postmaster (and, we trust, is waiting for it to exit).
 * So the PID is usable for "is the postmaster still running" checks,
 * but cannot be compared directly to postmaster.pid.
 *
 * On Windows, we also save aside a handle to the shell process in
 * "postmasterProcess", which the caller should close when done with it.
 */
static pgpid_t start_postmaster(void)
{
    /* make sure version_file length is big enough when template_file using realpath */
    const int MAX_REALPATH_LEN = 4096;
    char cmd[MAXPGPATH] = {0};
    char version_file[MAX_REALPATH_LEN] = {0};
    char template_file[MAXPGPATH] = {0};
    char version_str[MAXPGPATH] = {0};
    char ch;
    float undocumented_version = 0;
    struct stat buf;
    FILE* fd = NULL;
    int ret = 0;

#ifndef WIN32
    pgpid_t pm_pid;

    /* Flush stdio channels just before fork, to avoid double-output problems */
    (void)fflush(stdout);
    (void)fflush(stderr);

    pm_pid = fork();
    if (pm_pid < 0) {
        /* fork failed */
        pg_log(PG_WARNING, _(" could not start server: %s\n"), strerror(errno));
        exit(1);
    }
    if (pm_pid > 0) {
        /* fork succeeded, in parent */
        return pm_pid;
    }

    /* fork succeeded, in child */

    /*
     * If possible, detach the postmaster process from the launching process
     * group and make it a group leader, so that it doesn't get signaled along
     * with the current group that launched it.
     */
#ifdef HAVE_SETSID
    if (setsid() < 0) {
        pg_log(PG_WARNING, _("%s: could not start server due to setsid() failure: %s\n"),
               progname, strerror(errno));
        exit(1);
    }
#endif

    if (pg_host != NULL && *(pg_host) != '\0') {
        ret = snprintf_s(template_file, MAXPGPATH, MAXPGPATH - 1, "%s/binary_upgrade/old_upgrade_version", pg_host);
        securec_check_ss_c(ret, "\0", "\0");
        if (realpath(template_file, version_file) == NULL && version_file[0] == '\0') {
            pg_log(PG_WARNING, _("could not get correct path or the abs path is too long!\n"));
            exit(1);
        }

        if (stat(version_file, &buf) < 0) {
            if (errno != ENOENT) {
                pg_log(PG_WARNING, _("could not stat file %s: %s\n"), version_file, strerror(errno));
                exit(1);
            }
        } else {
            fd = fopen(version_file, "r");
            if (fd == NULL) {
                if (errno == ENOENT) {
                    pg_log(PG_WARNING, _("file %s is not exist\n"), version_file);
                } else {
                    pg_log(PG_WARNING, _("open file %s failed: %s\n"), version_file, strerror(errno));
                }
                exit(1);
            }

            if (fscanf_s(fd, "%s%c", version_str, MAXPGPATH, &ch, 1) != 2 || ch != '\n') {
                pg_log(PG_WARNING, _("invalid data in file %s"), version_file);
            }
            if (fscanf_s(fd, "%f%c", &undocumented_version, &ch, 1) != 2 || ch != '\n') {
                pg_log(PG_WARNING, _("invalid data in file %s"), version_file);
            }
            undocumented_version *= 1000;
            (void)fclose(fd);
            fd = NULL;
        }
    }
    /*
     * Since there might be quotes to handle here, it is easier simply to pass
     * everything to a shell to process them.  Use exec so that the postmaster
     * has the same PID as the current child process.
     */
    if (log_file != NULL) {
#ifdef ENABLE_MULTIPLE_NODES
        if (undocumented_version) {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" -u %u %s %s %s %s %s < \"%s\" >> \"%s\" 2>&1",
                exec_path,
                (uint32)undocumented_version,
                pgxcCommand,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                stop_barrier ? stop_barrier : "",
                DEVNULL,
                log_file);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" %s %s %s %s %s < \"%s\" >> \"%s\" 2>&1",
                exec_path,
                pgxcCommand,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                stop_barrier ? stop_barrier : "",
                DEVNULL,
                log_file);
            securec_check_ss_c(ret, "\0", "\0");
        }
#else
        if (undocumented_version) {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" -u %u %s %s %s< \"%s\" >> \"%s\" 2>&1",
                exec_path,
                (uint32)undocumented_version,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                DEVNULL,
                log_file);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" %s %s %s< \"%s\" >> \"%s\" 2>&1",
                exec_path,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                DEVNULL,
                log_file);
            securec_check_ss_c(ret, "\0", "\0");
        }
#endif
    } else {
#ifdef ENABLE_MULTIPLE_NODES
        if (undocumented_version) {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" -u %u %s %s %s %s %s < \"%s\" 2>&1",
                exec_path,
                (uint32)undocumented_version,
                pgxcCommand,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                stop_barrier ? stop_barrier : "",
                DEVNULL);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" %s %s %s %s %s < \"%s\" 2>&1",
                exec_path,
                pgxcCommand,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                stop_barrier ? stop_barrier : "",
                DEVNULL);
            securec_check_ss_c(ret, "\0", "\0");
        }
#else
        if (undocumented_version) {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" -u %u %s %s %s < \"%s\" 2>&1",
                exec_path,
                (uint32)undocumented_version,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                DEVNULL);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cmd,
                MAXPGPATH,
                MAXPGPATH - 1,
                "exec \"%s\" %s %s %s < \"%s\" 2>&1",
                exec_path,
                pgdata_opt,
                post_opts,
                pgha_opt ? pgha_opt : "",
                DEVNULL);
            securec_check_ss_c(ret, "\0", "\0");
        }
#endif
    }

    (void)execl("/bin/sh", "/bin/sh", "-c", cmd, (char*)NULL);

    /* exec failed */
    pg_log(PG_WARNING, _(" could not start server: %s\n"), strerror(errno));
    exit(1);

#else  /* WIN32 */

    /*
     * As with the Unix case, it's easiest to use the shell (CMD.EXE) to
     * handle redirection etc.  Unfortunately CMD.EXE lacks any equivalent of
     * "exec", so we don't get to find out the postmaster's PID immediately.
     */
    PROCESS_INFORMATION pi;

    if (log_file != NULL) {
        ret = snprintf_s(cmd,
            MAXPGPATH,
            MAXPGPATH - 1,
            "CMD /C " SYSTEMQUOTE "\"%s\" %s %s %s < \"%s\" >> \"%s\" 2>&1" SYSTEMQUOTE,
            exec_path,
            pgdata_opt,
            post_opts,
            pgha_opt ? pgha_opt : "",
            DEVNULL,
            log_file);
        securec_check_ss_c(ret, "\0", "\0");
    } else {
        ret = snprintf_s(cmd,
            MAXPGPATH,
            MAXPGPATH - 1,
            "CMD /C " SYSTEMQUOTE "\"%s\" %s %s %s < \"%s\" 2>&1" SYSTEMQUOTE,
            exec_path,
            pgdata_opt,
            post_opts,
            pgha_opt ? pgha_opt : "",
            DEVNULL);
        securec_check_ss_c(ret, "\0", "\0");
    }

    if (!CreateRestrictedProcess(cmd, &pi, false)) {
        pg_log(PG_WARNING, _(" could not start server: %s\n"), strerror(errno));
        exit(1);
    }
    /* Don't close command process handle here; caller must do so */
    postmasterProcess = pi.hProcess;
    CloseHandle(pi.hThread);
    return pi.dwProcessId; /* Shell's PID, not postmaster's! */
#endif /* WIN32 */
}

/*
 * Find the pgport and try a connection
 *
 * On Unix, pm_pid is the PID of the just-launched postmaster.  On Windows,
 * it may be the PID of an ancestor shell process, so we can't check the
 * contents of postmaster.pid quite as carefully.
 *
 * On Windows, the static variable postmasterProcess is an implicit argument
 * to this routine; it contains a handle to the postmaster process or an
 * ancestor shell process thereof.
 *
 * Note that the checkpoint parameter enables a Windows service control
 * manager checkpoint, it's got nothing to do with database checkpoints!!
 */
static PGPing test_postmaster_connection(pgpid_t pm_pid, bool do_checkpoint, struct stat beforeStat)
{
    PGPing ret = PQPING_NO_RESPONSE;
    char connstr[MAXPGPATH * 2 + 256];
    GaussState state;
    struct stat afterStat;
    int i;
    int nRet;

    /* if requested wait time is zero, return "still starting up" code */
    if (wait_seconds <= 0)
        return PQPING_REJECT;

    connstr[0] = '\0';

    for (i = 0; i < wait_seconds; i++) {
        /* Do we need a connection string? */
        if (connstr[0] == '\0') {
            /* ----------
             * The number of lines in postmaster.pid tells us several things:
             *
             * # of lines
             *		0	lock file created but status not written
             *		2	pre-9.1 server, shared memory not created
             *		3	pre-9.1 server, shared memory created
             *		5	9.1+ server, ports not opened
             *		6	9.1+ server, shared memory not created
             *		7	9.1+ server, shared memory created
             *
             * This code does not support pre-9.1 servers.	On Unix machines
             * we could consider extracting the port number from the shmem
             * key, but that (a) is not robust, and (b) doesn't help with
             * finding out the socket directory.  And it wouldn't work anyway
             * on Windows.
             *
             * If we see less than 6 lines in postmaster.pid, just keep
             * waiting.
             * ----------
             */
            char** optlines;

            /* Try to read the postmaster.pid file */
            if ((optlines = readfile(pid_file)) != NULL && optlines[0] != NULL && optlines[1] != NULL &&
                optlines[2] != NULL) {
                if (optlines[3] == NULL) {
                    /* File is exactly three lines, must be pre-9.1 */
                    pg_log(PG_WARNING, _(" -w option is not supported when starting a pre-9.1 server\n"));
                    freefile(optlines);
                    optlines = NULL;
                    return PQPING_NO_ATTEMPT;
                } else if (optlines[4] != NULL && optlines[5] != NULL) {
                    /* File is complete enough for us, parse it */
                    pgpid_t pmpid;
                    time_t pmstart;

                    /*
                     * Make sanity checks.  If it's for the wrong PID, or the
                     * recorded start time is before pg_ctl started, then
                     * either we are looking at the wrong data directory, or
                     * this is a pre-existing pidfile that hasn't (yet?) been
                     * overwritten by our child postmaster.  Allow 2 seconds
                     * slop for possible cross-process clock skew.
                     */
                    pmpid = atol(optlines[LOCK_FILE_LINE_PID - 1]);
                    pmstart = atol(optlines[LOCK_FILE_LINE_START_TIME - 1]);
                    if (pmstart >= start_time - 2 &&
#ifndef WIN32
                        pmpid == pm_pid
#else
                        /* Windows can only reject standalone-backend PIDs */
                        pmpid > 0
#endif
                    ) {
                        /*
                         * OK, seems to be a valid pidfile from our child.
                         */
                        int portnum = 0;
                        char* sockdir = NULL;
                        char* hostaddr = NULL;
                        char host_str[MAXPGPATH];
                        errno_t rc = 0;

                        rc = memset_s(host_str, MAXPGPATH, 0, MAXPGPATH);
                        securec_check_c(rc, "", "");

                        /*
                         * Extract port number and host string to use.
                         * We used to prefer unix domain socket.
                         * With thread pool, we prefer tcp port and connect to cn/dn ha port
                         * so that we do not need to be queued by thread pool controller.
                         */
                        check_input_for_security(optlines[LOCK_FILE_LINE_PORT - 1]);
                        check_input_for_security(optlines[LOCK_FILE_LINE_SOCKET_DIR - 1]);
                        check_input_for_security(optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1]);
                        portnum = atoi(optlines[LOCK_FILE_LINE_PORT - 1]);
                        sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
                        hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

                        /*
                         * While unix_socket_directory can accept relative
                         * directories, libpq's host parameter must have a
                         * leading slash to indicate a socket directory.  So,
                         * ignore sockdir if it's relative, and try to use TCP
                         * instead.
                         */
                        if (hostaddr != NULL && hostaddr[0] != '\0' && hostaddr[0] != '\n') {
                            rc = strncpy_s(host_str, sizeof(host_str), hostaddr, strlen(hostaddr));
                            /* ha port equals normal port plus 1, required by om */
                            portnum++;
                        } else if (sockdir[0] == '/') {
                            rc = strncpy_s(host_str, sizeof(host_str), sockdir, strlen(sockdir));
                        }

                        securec_check_c(rc, "", "");

                        /* remove trailing newline */
                        if (strchr(host_str, '\n') != NULL)
                            *strchr(host_str, '\n') = '\0';

                        /* Fail if couldn't get either sockdir or host addr */
                        if (host_str[0] == '\0') {
                            pg_log(PG_WARNING, _(" -w option cannot use a relative socket directory specification\n"));
                            freefile(optlines);
                            optlines = NULL;
                            return PQPING_NO_ATTEMPT;
                        }

                        /* If postmaster is listening on "*", use localhost */
                        if (strcmp(host_str, "*") == 0) {
                            rc = strcpy_s(host_str, MAXPGPATH, "localhost");
                            securec_check_c(rc, "\0", "\0");
                        }

                        /*
                         * We need to set connect_timeout otherwise on Windows
                         * the Service Control Manager (SCM) will probably
                         * timeout first.
                         */
                        nRet = snprintf_s(connstr,
                            sizeof(connstr),
                            sizeof(connstr) - 1,
                            "dbname=postgres port=%d host='%s' connect_timeout=5 "
                            "application_name=%s options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
                            portnum,
                            host_str,
                            progname);
                        securec_check_ss_c(nRet, "\0", "\0");
                    }
                }
            }
            freefile(optlines);
            optlines = NULL;
        }

        /* If we have a connection string, ping the server */
        if (connstr[0] != '\0') {
            ret = PQping(connstr);
            if (ret == PQPING_OK || ret == PQPING_NO_ATTEMPT)
                break;
        }

        /*
         * Check whether the child postmaster process is still alive.  This
         * lets us exit early if the postmaster fails during startup.
         *
         * On Windows, we may be checking the postmaster's parent shell, but
         * that's fine for this purpose.
         */
#ifndef WIN32
        {
            int exitstatus;

            if (waitpid((pid_t)pm_pid, &exitstatus, WNOHANG) == (pid_t)pm_pid) {
                pg_log(PG_WARNING, _("waitpid %lu failed, exitstatus is %d, ret is %d\n"), pm_pid, exitstatus, ret);
                return PQPING_NO_RESPONSE;
            } else {
                if (stat(gaussdb_state_file, &afterStat) != 0 && errno != ENOENT) {
                    pg_log(PG_WARNING,
                        _("could not stat file gaussdb_state_file %s: %s\n"),
                        gaussdb_state_file,
                        strerror(errno));
                } else if (errno != ENOENT) {
                    if (beforeStat.st_mtim.tv_sec != afterStat.st_mtim.tv_sec ||
                        beforeStat.st_mtim.tv_nsec != afterStat.st_mtim.tv_nsec) {
                        nRet = memset_s(&state, sizeof(state), 0, sizeof(state));
                        securec_check_c(nRet, "\0", "\0");
                        ReadDBStateFile(&state);
                        switch (state.state) {
                            case NORMAL_STATE:
                            case NEEDREPAIR_STATE:
                            case WAITING_STATE:
                            case DEMOTING_STATE:
                            case PROMOTING_STATE:
                            case BUILDING_STATE:
                            case CATCHUP_STATE:
                                return PQPING_OK;
                            case COREDUMP_STATE:
                                pg_log(PG_WARNING, _(" gaussDB state is %s\n"), get_string_by_state(state.state));
                                return PQPING_NO_RESPONSE;
                            case STARTING_STATE:
                            case UNKNOWN_STATE:
                            default:
                                /* nothing to do */
                                break;
                        }
                        beforeStat = afterStat;
                    }
                }
            }   
        }
#else
        if (WaitForSingleObject(postmasterProcess, 0) == WAIT_OBJECT_0)
            return PQPING_NO_RESPONSE;
#endif

        /* No response, or startup still in process; wait */
#if defined(WIN32)
        if (do_checkpoint) {
            /*
             * Increment the wait hint by 6 secs (connection timeout + sleep)
             * We must do this to indicate to the SCM that our startup time is
             * changing, otherwise it'll usually send a stop signal after 20
             * seconds, despite incrementing the checkpoint counter.
             */
            status.dwWaitHint += 6000;
            status.dwCheckPoint++;
            SetServiceStatus(hStatus, (LPSERVICE_STATUS)&status);
        } else
#endif
            pg_log(PG_PRINT, _("."));

        pg_usleep(1000000); /* 1 sec */
    }

    /* return result of last call to PQping */
    return ret;
}

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
static void unlimit_core_size(void)
{
    struct rlimit lim;

    getrlimit(RLIMIT_CORE, &lim);
    if (lim.rlim_max == 0) {
        pg_log(PG_WARNING, _(" cannot set core file size limit; disallowed by hard limit\n"));
        return;
    } else if (lim.rlim_max == RLIM_INFINITY || lim.rlim_cur < lim.rlim_max) {
        lim.rlim_cur = lim.rlim_max;
        setrlimit(RLIMIT_CORE, &lim);
    }
}
#endif

static void read_post_opts(void)
{
    errno_t tnRet = 0;
    if (post_opts == NULL) {
        post_opts = ""; /* default */
        if (ctl_command == RESTART_COMMAND) {
            char** optlines;

            optlines = readfile(postopts_file);
            if (optlines == NULL) {
                pg_log(PG_WARNING, _(" could not read file \"%s\"\n"), postopts_file);
                exit(1);
            } else if (optlines[0] == NULL || optlines[1] != NULL) {
                pg_log(PG_WARNING, _(" option file \"%s\" must have exactly one line\n"), postopts_file);
                freefile(optlines);
                optlines = NULL;
                exit(1);
            } else {
                int len;
                char* optline = NULL;
                char* arg1 = NULL;
                char* dest = NULL;
                char* destleft = NULL;

                check_input_for_security(optlines[0]);
                optline = optlines[0];
                /* trim off line endings */
                len = strcspn(optline, "\r\n");
                optline[len] = '\0';

                /*
                 * Are we at the first option, as defined by space and
                 * double-quote?
                 */
                if ((arg1 = strstr(optline, " \"")) != NULL) {
                    *arg1 = '\0';         /* terminate so we get only program
                                           * name */
#ifdef ENABLE_MULTIPLE_NODES
                    post_opts = arg1 + 1; /* point past whitespace */
#else
                    post_opts = xstrdup(arg1 + 1);
#endif
                }
                if (exec_path == NULL) {
                    exec_path = xstrdup(optline);
                }

                if (NULL != pgxcCommand) {
                    dest = strstr(post_opts, "\"--datanode\"");
                    if (dest == NULL) {
                        dest = strstr(post_opts, "\"--coordinator\"");
                    }
                    if (dest != NULL) {
                        destleft = strstr(dest + 1, "\" \"");
                        if (destleft == NULL) {
                            *dest = '\0';
                        } else {
                            char* destleft_tmp = strdup(destleft);

                            if (destleft_tmp != NULL) {
                                size_t destlen = strlen(destleft_tmp);
                                destlen -= 2;
                                tnRet = strncpy_s(dest, destlen + 1, destleft_tmp + 2, destlen);
                                securec_check_c(tnRet, "\0", "\0");
                                dest[destlen] = '\0';

                                free(destleft_tmp);
                                destleft_tmp = NULL;
                            } else {
                                *dest = '\0';
                            }
                        }
                    }
                }

                /* when restart without -M xxx parameter ,then return obtain the -M xxx in post_opts */
                if (pgha_opt == NULL) {
                    freefile(optlines);
                    optlines = NULL;
                    return;
                }
                /*
                 * else use -M xxx in pgha_opt,and discarding the -M xxx in post_opts
                 * find "-M" "xxxx"  like this string in post_opts
                 * in string post_opts, the index from begin to end is "-M" "xxxxx"
                 */
                dest = strstr(post_opts, "\"-M\"");
                if (dest == NULL) {
                    freefile(optlines);
                    optlines = NULL;
                    return;
                }
                destleft = strstr(dest + 5, "\" \"");
                if (destleft == NULL) {
                    *dest = '\0';
                } else {
                    size_t destlen = strlen(destleft);
                    tnRet = strncpy_s(dest, destlen + 1, destleft, destlen);
                    securec_check_c(tnRet, "\0", "\0");
                    dest[destlen] = '\0';
                }
            }
            freefile(optlines);
            optlines = NULL;
        }
    }
}

/*
 * SIGINT signal handler used while waiting for postmaster to start up.
 * Forwards the SIGINT to the postmaster process, asking it to shut down,
 * before terminating pg_ctl itself. This way, if the user hits CTRL-C while
 * waiting for the server to start up, the server launch is aborted.
 */
static void trap_sigint_during_startup(int sig)
{
    if (postmasterPID != -1) {
       if (kill(postmasterPID, SIGINT) != 0)
            pg_log(PG_WARNING, _("%s: could not send stop signal (PID: %ld): %s\n"),
                   progname, postmasterPID, strerror(errno));
    }

	/*
     * Clear the signal handler, and send the signal again, to terminate the
     * process as normal.
     */
    pqsignal(SIGINT, SIG_DFL);
    raise(SIGINT);
}

static char* find_other_exec_or_die(const char* argv0, const char* target, const char* versionstr)
{
    int ret;
    char* found_path = NULL;
    errno_t rc = EOK;

    found_path = (char*)pg_malloc(MAXPGPATH);

    if ((ret = find_other_exec(argv0, target, versionstr, found_path)) < 0) {
        char full_path[MAXPGPATH] = {0};

        if (find_my_exec(argv0, full_path) < 0) {
            rc = strncpy_s(full_path, sizeof(full_path), progname, strlen(progname));
            securec_check_c(rc, "", "");
        }

        if (ret == -1)
            pg_log(PG_WARNING,
                _("The program \"%s\" is needed by %s "
                  "but was not found in the\n"
                  "same directory as \"%s\".\n"
                  "Check your installation.\n"),
                target,
                full_path,
                progname);
        else
            pg_log(PG_WARNING,
                _("The program \"%s\" was found by \"%s\"\n"
                  "but was not the same version as %s.\n"
                  "Check your installation.\n"),
                target,
                full_path,
                progname);
        exit(1);
    }

    return found_path;
}

static void do_init(void)
{
    char cmd[MAXPGPATH];
    int ret;

    if (exec_path == NULL)
#ifdef ENABLE_MULTIPLE_NODES
        exec_path = find_other_exec_or_die(argv0, "gs_initdb", "gs_initdb (PostgreSQL) " PG_VERSION "\n");
#else
        exec_path = find_other_exec_or_die(argv0, "gs_initdb", "gs_initdb (openGauss) " PG_VERSION "\n");
#endif

    if (pgdata_opt == NULL)
        pgdata_opt = "";

    if (post_opts == NULL)
        post_opts = "";

    if (!silent_mode) {
        ret = snprintf_s(
            cmd, MAXPGPATH, MAXPGPATH - 1, SYSTEMQUOTE "\"%s\" %s%s" SYSTEMQUOTE, exec_path, pgdata_opt, post_opts);
        securec_check_ss_c(ret, "\0", "\0");
    } else {
        ret = snprintf_s(cmd,
            MAXPGPATH,
            MAXPGPATH - 1,
            SYSTEMQUOTE "\"%s\" %s%s > \"%s\"" SYSTEMQUOTE,
            exec_path,
            pgdata_opt,
            post_opts,
            DEVNULL);
        securec_check_ss_c(ret, "\0", "\0");
    }

    if (system(cmd) != 0) {
        pg_log(PG_WARNING, _(" database system initialization failed\n"));
        exit(1);
    }
}

static int get_instance_port(const char* filename)
{
    char** optlines;
    int portnum = -1;
    int opt_index = 0;
    char portstr[MAX_PARAM_LEN] = {0};
    errno_t rc = EOK;

    if (filename == NULL) {
        pg_log(PG_WARNING, _("the parameter filename is NULL in function get_instance_port()"));
        exit(1);
    }

    if ((optlines = readfile(filename)) != NULL) {
        int optvalue_off = 0;
        int optvalue_len = 0;
        int lines_index = 0;

        lines_index = find_gucoption((const char**)optlines, "port", NULL, NULL, &optvalue_off, &optvalue_len);
        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(portstr,
                MAX_PARAM_LEN,
                optlines[lines_index] + optvalue_off,
                (size_t)Min(optvalue_len, MAX_PARAM_LEN - 1));
            securec_check_c(rc, "", "");
            portnum = atoi(portstr);
        }

        while (optlines[opt_index] != NULL) {
            free(optlines[opt_index]);
            optlines[opt_index] = NULL;
            opt_index++;
        }
        free(optlines);
        optlines = NULL;
    } else {
        pg_log(PG_WARNING, _("%s cannot be opened.\n"), filename);
        exit(1);
    }

    return portnum;
}

static int do_check_port(uint32 portnum)
{
    FILE* fp = NULL;
    char buf[MAXPGPATH] = {0};
    int rc = 0;
    uint32 ls = 0;
    char localip[MAXPGPATH] = {0};
    uint32 localport = 0;
    char peerip[MAXPGPATH] = {0};
    uint32 peerport = 0;
    uint32 status = 0;
    char other[MAXPGPATH] = {0};
    char displaystr[MAX_DISPLAY_STRING_LEN] = {0};
    FILE* fd = NULL;

    fp = fopen(PROC_NET_TCP, "r");
    if (fp == NULL) {
        pg_log(PG_WARNING, _("can not open file \"%s\"\n"), PROC_NET_TCP);
        return -1;
    }

    if (NULL == fgets(buf, MAXPGPATH - 1, fp)) {
        pg_log(PG_WARNING, _("can not read file \"%s\"\n"), PROC_NET_TCP);
        fclose(fp);
        fp = NULL;
        return -1;
    }

    while (fgets(buf, MAXPGPATH - 1, fp) != NULL) {
        localport = 0;
        status = 0;

        rc = sscanf_s(buf,
            "%u: %[0-9A-Fa-f]:%X %[0-9A-Fa-f]:%X %X %s",
            &ls,
            localip,
            MAXPGPATH,
            &localport,
            peerip,
            MAXPGPATH,
            &peerport,
            &status,
            other,
            MAXPGPATH);
        if (rc != 7) {
            pg_log(PG_WARNING, _("get value by sscanf_s return error:%d"), rc);
            continue;
        }

        if (localport == portnum && status == LISTEN) {
            fclose(fp);
            fp = NULL;

            pg_log(PG_WARNING,
                _("port:%d already in use. /proc/net/tcp:\n%s \n%s"),
                localport,
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ",
                buf);

            char command[MAXPGPATH] = {0};
            rc = snprintf_s(command,
                MAXPGPATH,
                MAXPGPATH - 1,
                SYSTEMQUOTE "lsof -i:%u | grep -E \'COMMAND|LISTEN\'" SYSTEMQUOTE,
                localport);
            securec_check_ss_c(rc, "\0", "\0");

            fd = popen(command, "r");
            if (fd == NULL) {
                pg_log(PG_WARNING, _("run popen command failed. %s\n"), command);
                return 0;
            }

            pg_log(PG_WARNING, _("CheckPort: popen(command:%s).\n"), command);
            while (fgets(displaystr, sizeof(displaystr), fd) != NULL) {
                write_stderr(_("%s\n"), displaystr);
            }

            pclose(fd);
            fd = NULL;
            return 1;
        }
    }

    fclose(fp);
    fp = NULL;
    return 0;
}

static void do_start_set_para(void)
{
/* No -D or -D already added during server start */
    if (ctl_command == RESTART_COMMAND || pgdata_opt == NULL) {
        if (pgdata_opt != NULL && (char*)pgdata_opt != (char*)"") {
            free(pgdata_opt);
            pgdata_opt = NULL;
        }
        pgdata_opt = "";
    }

    if (exec_path == NULL)
        exec_path = find_other_exec_or_die(argv0, "gaussdb", PG_BACKEND_VERSIONSTR);
}

static void do_start(void)
{
    pgpid_t old_pid = 0;
    pgpid_t pm_pid;
    int ret;
    int portnum;
    struct stat beforeStat;

    if (ctl_command != RESTART_COMMAND) {
        old_pid = get_pgpid();
        if (old_pid < 0) {
            old_pid = -old_pid;
        }
        if (old_pid != 0 && postmaster_is_alive((pid_t)old_pid)
#ifndef WIN32
            && IsMyPostmasterPid((pid_t)old_pid, pg_config)
#endif
        ) {
            pg_log(PG_WARNING,
                _(" another server might be running; "
                  "Please use the restart command\n"));
            exit(0);
        }
    }

    read_post_opts();

    do_start_set_para();

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
    if (allow_core_files)
        unlimit_core_size();
#endif

        /*
         * If possible, tell the postmaster our parent shell's PID (see the
         * comments in CreateLockFile() for motivation).  Windows hasn't got
         * getppid() unfortunately.
         */
#ifndef WIN32
    {
        static char env_var[32];

        ret = snprintf_s(env_var, sizeof(env_var), sizeof(env_var) - 1, "PG_GRANDPARENT_PID=%d", (int)getppid());
        securec_check_ss_c(ret, "\0", "\0");
        putenv(env_var);
    }
#endif

    /* get port from conf file to check port conflict before serer started */
    portnum = get_instance_port(pg_conf_file);

    if (portnum > 0 && do_check_port(portnum) > 0)
        pg_log(PG_PROGRESS, _("port conflict when start server\n"));

    if (stat(gaussdb_state_file, &beforeStat) != 0 && errno != ENOENT) {
        pg_log(PG_WARNING, _("could not stat file gaussdb_state_file %s: %s\n"), gaussdb_state_file, strerror(errno));
    }

    pm_pid = start_postmaster();

    if (do_wait) {
        /*
         * If the user interrupts the startup (e.g. with CTRL-C), we'd like to
         * abort the server launch.  Install a signal handler that will
         * forward SIGINT to the postmaster process, while we wait.
         *
         * (We don't bother to reset the signal handler after the launch, as
         * we're about to exit, anyway.)
         */
        postmasterPID = pm_pid;
        pqsignal(SIGINT, trap_sigint_during_startup);

        pg_log(PG_PROGRESS, _("waiting for server to start...\n"));
        switch (test_postmaster_connection(pm_pid, false, beforeStat)) {
            case PQPING_OK:
                pg_log(PG_PRINT, _("\n"));
                pg_log(PG_PROGRESS, _(" done\n"));
                pg_log(PG_PROGRESS, _("server started (%s)\n"), pg_data);
                break;
            case PQPING_REJECT:
                pg_log(PG_PRINT, _("\n"));
                pg_log(PG_PROGRESS, _("stopped waiting\n"));
                pg_log(PG_PROGRESS, _("server is still starting up\n"));
                break;
            case PQPING_NO_RESPONSE:
                pg_log(PG_PRINT, _("\n"));
                pg_log(PG_PROGRESS, _("stopped waiting\n"));
                pg_log(PG_PROGRESS,
                    _("could not start server\n"
                      "Examine the log output.\n"));
                if (ctl_command == BUILD_COMMAND) {
                    set_build_pid(0);
                }
                exit(1);
                break;
            case PQPING_NO_ATTEMPT:
                pg_log(PG_PRINT, _("\n"));
                pg_log(PG_PROGRESS, _("failed\n"));
                pg_log(PG_PROGRESS, _(" could not wait for server because of misconfiguration\n"));
                if (ctl_command == BUILD_COMMAND)
                    set_build_pid(0);
                exit(1);
            default:
                break;
        }
    } else {
        pg_log(PG_PROGRESS, _("server starting\n"));
    }

#ifdef WIN32
    /* Now we don't need the handle to the shell process anymore */
    CloseHandle(postmasterProcess);
    postmasterProcess = INVALID_HANDLE_VALUE;
#endif
}

/*
 * Stop gaussdb server.
 * Support three shutdown mode. if force is true, shut down in
 * immediate mode after failed in other two mode.
 */
static void do_stop(bool force)
{
    int cnt;
    pgpid_t pid, tpid;
    struct stat statbuf;

    pid = get_pgdbpid_for_stop();
    if (shutdown_mode == FAST_MODE) {
        sig = SIGINT;
    }
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist.\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        if (force || (shutdown_mode == IMMEDIATE_MODE)) {
            kill_proton_force();
            return;
        }
        exit(1);
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot stop server; "
              "single-user server is running (PID: %ld)"),
            pid);
        exit(1);
    }
    tpid = pid;
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING,
            _(" cannot stop server; "
              "single-user server is running (PID: %ld)  %s\n"),
            pid,
            strerror(errno));
        if (force || (shutdown_mode == IMMEDIATE_MODE)) {
            kill_proton_force();
            if (-1 == chmod(pid_file, 0600)) {
                pg_log(PG_WARNING, _(" could not set permissions of file \"%s\": %m\n"), pid_file);
                return;
            }
        }
        if (errno == 3) {
            exit(0);
        }
        exit(1);
    }

    if (!do_wait) {
        print_msg(_("server shutting down\n"));
        return;
    } else {
        /*
         * If backup_label exists, an online backup is running. Warn the user
         * that smart shutdown will wait for it to finish. However, if
         * recovery.conf is also present, we're recovering from an online
         * backup instead of performing one.
         */
        if (shutdown_mode == SMART_MODE && stat(backup_file, &statbuf) == 0 && stat(recovery_file, &statbuf) != 0) {
            print_msg(_("WARNING: online backup mode is active\n"
                        "Shutdown will not complete until pg_stop_backup() is called.\n\n"));
        }

        print_msg(_("waiting for server to shut down..."));

        for (cnt = 0; cnt < wait_seconds; cnt++) {
            if (((pid = get_pgpid()) != 0) ||
                (postmaster_is_alive((pid_t)tpid) && IsMyPostmasterPid((pid_t)tpid, pg_config))) {
                print_msg(".");
                pg_usleep(1000000); /* 1 sec */
            } else
                break;
        }

        if (pid != 0) { /* pid file still exists */
            if (force) {
                kill_proton_force();

                if (stat(pid_file, &statbuf) != 0) {
                    pg_log(
                        PG_WARNING, _("could not stat directory or file pid_file %s: %s\n"), pid_file, strerror(errno));
                }

                /* if it is a symnol, chmod will change the auth of the true file */
                if (S_ISLNK(statbuf.st_mode)) {
                    pg_log(PG_WARNING, _("the file being chmod is a symbol link\n"));
                }

                if (-1 == chmod(pid_file, 0600)) {
                    pg_log(PG_WARNING, _(" could not set permissions of file \"%s\": %m\n"), pid_file);
                }
                return;
            } else {
                print_msg(_(" failed\n"));
                pg_log(PG_WARNING, _(" server does not shut down (%s)\n"), pg_data);
            }

            if (shutdown_mode == SMART_MODE)
                pg_log(PG_PRINT,
                    _("HINT: The \"-m fast\" option immediately disconnects sessions rather than\n"
                      "waiting for session-initiated disconnection.\n"));
            exit(1);
        }
        print_msg(_(" done\n"));

        print_msg(_("server stopped\n"));
    }
}

/*
 *	restart/reload routines
 */
static void do_restart(void)
{
    int cnt;
    pgpid_t pid;
    pgpid_t tPid;
    struct stat statbuf;

    pid = get_pgdbpid_for_stop();
    if (shutdown_mode == FAST_MODE) {
        sig = SIGINT;
    }

    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _("Is server running?\n"));
        pg_log(PG_WARNING, _("starting server anyway\n"));
        if (shutdown_mode == IMMEDIATE_MODE) {
            kill_proton_force();
        }
        do_start();
        return;
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        if (postmaster_is_alive((pid_t)pid)) {
            pg_log(PG_WARNING,
                _(" cannot restart server; "
                  "single-user server is running (PID: %ld)\n"),
                pid);
            pg_log(PG_WARNING, _("Please terminate the single-user server and try again.\n"));
            return;
        }
    }

    tPid = pid;
    if (postmaster_is_alive((pid_t)pid)
#ifndef WIN32
        && IsMyPostmasterPid((pid_t)pid, pg_config)
#endif
    ) {
        if (kill((pid_t)pid, sig) != 0) {
            pg_log(PG_WARNING, _(" could not send stop signal (PID: %ld): %s\n"), pid, strerror(errno));
            if (IMMEDIATE_MODE == shutdown_mode) {
                kill_proton_force();
            }
            start_time = time(NULL);
            do_start();
            return;
        }

        /*
         * If backup_label exists, an online backup is running. Warn the user
         * that smart shutdown will wait for it to finish. However, if
         * recovery.conf is also present, we're recovering from an online
         * backup instead of performing one.
         */
        if (shutdown_mode == SMART_MODE && stat(backup_file, &statbuf) == 0 && stat(recovery_file, &statbuf) != 0) {
            print_msg(_("WARNING: online backup mode is active\n"
                        "Shutdown will not complete until pg_stop_backup() is called.\n\n"));
        }

        print_msg(_("waiting for server to shut down..."));

        /* always wait for restart */
        for (cnt = 0; cnt < wait_seconds; cnt++) {
            if ((pid = get_pgpid()) == 0 && !postmaster_is_alive(tPid))
                break;
            pg_usleep(1000000);
        }

        if (pid != 0) { /* pid file still exists */
            print_msg(_(" failed\n"));

            pg_log(PG_WARNING, _(" server does not shut down\n"));
            if (shutdown_mode == SMART_MODE) {
                pg_log(PG_PRINT,
                    _("HINT: The \"-m fast\" option immediately disconnects sessions rather than\n"
                      "waiting for session-initiated disconnection.\n"));
            }
            if (shutdown_mode == IMMEDIATE_MODE) {
                kill_proton_force();
            }
            start_time = time(NULL);
            do_start();
            return;
        }
        if (tPid != 0 && postmaster_is_alive(tPid))
            print_msg(_("postmaster.pid is delete, while the prcoess is still alive\n"));
        print_msg(_(" done\n"));
        print_msg(_("server stopped\n"));
    } else {
        pg_log(PG_WARNING, _(" old server process (PID: %ld) seems to be gone\n"), pid);
        pg_log(PG_WARNING, _("starting server anyway\n"));
    }

    start_time = time(NULL);
    do_start();
}

static void do_reload(void)
{
    pgpid_t pid;

    pid = get_pgpid();
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        exit(1);
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot reload server; "
              "single-user server is running (PID: %ld)\n"),
            pid);
        pg_log(PG_WARNING, _("Please terminate the single-user server and try again.\n"));
        exit(1);
    }

    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _(" could not send reload signal (PID: %ld): %s\n"), pid, strerror(errno));
        exit(1);
    }

    print_msg(_("server signaled\n"));
}

static void
do_finish_redo(void)
{
    FILE *fofile = NULL;
    char redo_path[MAXPGPATH];
    pgpid_t pid;
    bool do_finish = true;
    int ret = snprintf_s(redo_path, MAXPGPATH, MAXPGPATH-1, "%s/finish_redo_file", pg_data);
    securec_check_ss_c(ret, "", "");
    if ((fofile = fopen(redo_path, "w")) == NULL)
    {
        pg_log(PG_WARNING, _("fail to open finish redo file\n"));
        exit(1);
    }
    if (fwrite(&do_finish, sizeof(bool), 1, fofile) != 1)
    {
        pg_log(PG_WARNING, _("fail to write finish redo file\n"));
    }
    if (fclose(fofile))
    {
        pg_log(PG_WARNING, _("fail to close finish redo file\n"));
    }
    fofile = NULL;

    pid = get_pgpid();

    if (pid == 0) /* no pid file */
    {
        pg_log(PG_WARNING, _("PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        exit(1);
    }
    else if (pid < 0) /* standalone backend, not postmaster */
    {
        pid = -pid;
        pg_log(PG_WARNING, _("cannot finish redo server: single-user server is running (PID: %ld)\n"), pid);
        exit(1);
    }

    sig = SIGUSR1;
    if (kill((pid_t) pid, sig) != 0)
    {
        pg_log(PG_WARNING, _("could not send finish redo signal (PID: %ld): %s\n"), pid, strerror(errno));
        if (unlink(redo_path) != 0)
            pg_log(PG_WARNING, _("could not remove finish redo file \"%s\": %s\n"), redo_path, strerror(errno));
        exit(1);
    }
}

static void do_failover(uint32 term)
{
    FILE* fofile = NULL;
    int cnt = 0;
    pgpid_t pid;
    ServerMode run_mode;
    ServerMode origin_run_mode;

    int ret;
    char term_path[MAXPGPATH];
    ret = snprintf_s(term_path, MAXPGPATH, MAXPGPATH - 1, "%s/term_file", pg_data);
    securec_check_ss_c(ret, "\0", "\0");

    pg_log(PG_WARNING, _("failover term (%u)\n"), term);

    if ((fofile = fopen(term_path, "w")) == NULL) {
        pg_log(PG_WARNING, _("could not open file successfully\n"));
        exit(1);
    }
    if (fwrite(&term, sizeof(uint32), 1, fofile) != 1) {
        pg_log(PG_WARNING, _("could not write file successfully\n"));
    }
    if (fclose(fofile)) {
        pg_log(PG_WARNING, _("file is closed\n"));
    }
    fofile = NULL;

    pid = get_pgpid();
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _(" Is server running?\n"));
        exit(1);
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot failover server;"
              "single-user server is running (PID: %ld)\n"),
            pid);
        exit(1);
    }

    origin_run_mode = run_mode = get_runmode();
    if (run_mode == PRIMARY_MODE) {
        pg_log(PG_WARNING, _(" failover completed (%s)\n"), pg_data);
        return;
    } else if (run_mode == UNKNOWN_MODE) {
        pg_log(PG_WARNING,
            _(" cannot failover server:"
              "server mode is unknown\n"));
        exit(1);
    }
    /* failover executed only in standby server */
    else if (run_mode != STANDBY_MODE && run_mode != CASCADE_STANDBY_MODE) {
        pg_log(PG_WARNING,
            _(" cannot failover server; "
              "server is not in standby or cascade standby mode\n"));
        exit(1);
    }

    if (!do_wait) {
        pg_log(PG_WARNING, _(" server starting failover\n"));
    } else {
        pg_log(PG_WARNING, _(" waiting for server to failover...\n"));
    }

    for (cnt = 0;; cnt++) {
        /* cascade standby only need to trigger once */
        if (cnt > 0 && do_wait && origin_run_mode == CASCADE_STANDBY_MODE) {
            pg_log(PG_PRINT, ".");
            pg_usleep(1000000); /* 1 sec */

            if (get_runmode() == STANDBY_MODE)
                break;
            else if (cnt >= wait_seconds) {
                break;
            }
            continue;
        }

        if ((fofile = fopen(failover_file, "w")) == NULL) {
            pg_log(
                PG_WARNING, _(" could not create failover signal file \"%s\": %s\n"), failover_file, strerror(errno));
            exit(1);
        }
        if (fclose(fofile)) {
            pg_log(PG_WARNING, _(" could not write failover signal file \"%s\": %s\n"), failover_file, strerror(errno));
            fofile = NULL;
            exit(1);
        }
        fofile = NULL;

        sig = SIGUSR1;
        if (kill((pid_t)pid, sig) != 0) {
            pg_log(PG_WARNING, _(" could not send failover signal (PID: %ld): %s\n"), pid, strerror(errno));
            if (unlink(failover_file) != 0)
                pg_log(PG_WARNING,
                    _(" could not remove failover signal file \"%s\": %s\n"),
                    failover_file,
                    strerror(errno));
            exit(1);
        }

        if (!do_wait) {
            return;
        } else if (cnt >= wait_seconds) {
            break;
        }

        pg_log(PG_PRINT, ".");
        pg_usleep(1000000); /* 1 sec */

        if (get_runmode() == PRIMARY_MODE)
            break;
    }

    if ((origin_run_mode == STANDBY_MODE && get_runmode() != PRIMARY_MODE) ||
        (origin_run_mode == CASCADE_STANDBY_MODE && get_runmode() != STANDBY_MODE)) {
        pg_log(PG_WARNING, _(" failed\n"));
        pg_log(PG_WARNING, _(" failover failed (%s)\n"), pg_data);
        exit(1);
    }
    pg_log(PG_WARNING, _(" done\n"));
    pg_log(PG_WARNING, _(" failover completed (%s)\n"), pg_data);
}

static void do_notify(uint32 term)
{
    FILE* notifile = NULL;
    FILE* fofile = NULL;
    pgpid_t pid;
    int cnt;
    char* notify_file = NULL;
    ServerMode run_mode = UNKNOWN_MODE;
    ServerMode notify_mode = UNKNOWN_MODE;
    int ret;
    char term_path[MAXPGPATH];

    if (pgha_str == NULL) {
        pg_log(PG_WARNING, _("the parameter of notify must be specified\n"));
        exit(1);
    }
    pid = get_pgpid();
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_PRINT, _("Is server running?\n"));
        exit(1);
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot notify server; "
              "single-user server is running (PID: %ld)\n"),
            pid);
        exit(1);
    }
    if ((pgha_str != NULL) && 0 == strncmp(pgha_str, "standby", strlen("standby")) &&
        pgha_str[strlen("standby")] == '\0') {
        notify_file = xstrdup(standby_file);
        notify_mode = STANDBY_MODE;
    } else if ((pgha_str != NULL) && 0 == strncmp(pgha_str, "primary", strlen("primary")) &&
               pgha_str[strlen("primary")] == '\0') {
        notify_file = xstrdup(primary_file);
        notify_mode = PRIMARY_MODE;
    } else if ((pgha_str != NULL) && 0 == strncmp(pgha_str, "cascade_standby", strlen("cascade_standby")) &&
               pgha_str[strlen("cascade_standby")] == '\0') {
        notify_file = xstrdup(cascade_standby_file);
        notify_mode = CASCADE_STANDBY_MODE;
    } else {
        pg_log(PG_WARNING, _(" the parameter of notify is not recognized\n"));
        exit(1);
    }

    run_mode = get_runmode();
    if (run_mode == UNKNOWN_MODE) {
        pg_log(PG_WARNING,
            _(" cannot notify server: "
              "server mode is unknown\n"));
        free(notify_file);
        notify_file = NULL;
        exit(1);
    }
    if (run_mode != PENDING_MODE &&
        !(run_mode == NORMAL_MODE && strncmp(primary_file, notify_file, MAXPGPATH - 1) == 0)) {
        pg_log(PG_WARNING,
            _(" only support notify server in pending mode; "
              "or notify server to primary in normal mode"));
        free(notify_file);
        notify_file = NULL;
        exit(1);
    }

    if (notify_mode == PRIMARY_MODE) {
        pg_log(PG_WARNING, _("notify primary term (%u)"), term);

        ret = snprintf_s(term_path, MAXPGPATH, MAXPGPATH - 1, "%s/term_file", pg_data);
        securec_check_ss_c(ret, "\0", "\0");

        if ((fofile = fopen(term_path, "w")) == NULL) {
            pg_log(PG_WARNING, _("failed to open file \"%s\": %s\n"), term_path, strerror(errno));
            free(notify_file);
            notify_file = NULL;
            exit(1);
        }
        if (fwrite(&term, sizeof(uint32), 1, fofile) != 1) {
            pg_log(PG_WARNING, _("failed to write file \"%s\": %s\n"), term_path, strerror(errno));
        }
        if (fclose(fofile)) {
            pg_log(PG_WARNING, _("failed to close file \"%s\": %s\n"), term_path, strerror(errno));
        }
        fofile = NULL;
    }

    if ((notifile = fopen(notify_file, "w")) == NULL) {
        pg_log(PG_WARNING, _(" could not create notify signal file \"%s\": %s"), notify_file, strerror(errno));
        free(notify_file);
        notify_file = NULL;
        exit(1);
    }
    if (fclose(notifile)) {
        pg_log(PG_WARNING, _(" could not write notify signal file \"%s\": %s"), notify_file, strerror(errno));
        free(notify_file);
        notify_file = NULL;
        exit(1);
    }
    notifile = NULL;

    sig = SIGUSR1;
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _(" could not send notify signal (PID: %ld): %s"), pid, strerror(errno));
        if (unlink(notify_file) != 0)
            pg_log(PG_WARNING, _(" could not remove notify signal file \"%s\": %s"), notify_file, strerror(errno));
        free(notify_file);
        notify_file = NULL;
        exit(1);
    }

    free(notify_file);
    notify_file = NULL;

    if (!do_wait) {
        pg_log(PG_WARNING, _("server starting notify"));
    } else {
        pg_log(PG_WARNING, _("waiting for server to response..."));
        for (cnt = 0; cnt < wait_seconds; cnt++) {
            if ((run_mode = get_runmode()) != notify_mode) {
                pg_log(PG_PRINT, ".");
                pg_usleep(1000000); /* 1 sec */
            } else
                break;
        }
        pg_log(PG_PRINT, _(" "));
        if (run_mode != notify_mode) {
            pg_log(PG_WARNING, _("failed"));
            pg_log(PG_WARNING,
                _(" notify server failed, notify %s, current is %s\n"),
                get_localrole_string(notify_mode),
                get_localrole_string(run_mode));
            exit(1);
        }
        pg_log(PG_WARNING, _("done"));
        pg_log(PG_WARNING, _("notify server completed, mode change to %s\n"), get_localrole_string(notify_mode));
    }
}

static int pg_ctl_lock(const char* lock_file, FILE** lockfile_fd)
{
    int ret;
    struct stat statbuf;
    int fildes = 0;
    char standard_lock_file[g_max_length_std_path] = {0};

    ret = snprintf_s(standard_lock_file, sizeof(standard_lock_file), sizeof(standard_lock_file) - 1, "%s", lock_file);
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(standard_lock_file);

    /* If pg_ctl.lock dose not exist,create it */
    if (stat(standard_lock_file, &statbuf) != 0) {
        char content[PG_CTL_LOCKFILE_SIZE] = {0};
        *lockfile_fd = fopen(standard_lock_file, PG_BINARY_W);
        if (*lockfile_fd == NULL) {
            pg_log(PG_WARNING, _("can't create lock file \"%s\" : %s"), standard_lock_file, strerror(errno));
            exit(1);
        } else {
            fildes = fileno(*lockfile_fd);
            if (fchmod(fildes, S_IRUSR | S_IWUSR) == -1) {
                pg_log(PG_WARNING, _("can't chmod lock file \"%s\" : %s"), standard_lock_file, strerror(errno));
                /* Close file and Nullify the pointer for retry */
                fclose(*lockfile_fd);
                *lockfile_fd = NULL;
                exit(1);
            }
            if (fwrite(content, PG_CTL_LOCKFILE_SIZE, 1, *lockfile_fd) != 1) {
                fclose(*lockfile_fd);
                *lockfile_fd = NULL;
                pg_log(PG_WARNING, _("can't write lock file \"%s\" : %s"), standard_lock_file, strerror(errno));
                exit(1);
            }
            fclose(*lockfile_fd);
            *lockfile_fd = NULL;
        }
    }
    if ((*lockfile_fd = fopen(standard_lock_file, PG_BINARY_W)) == NULL) {
        pg_log(PG_WARNING, _(" could open lock file \"%s\" : %s\n"), standard_lock_file, strerror(errno));
        exit(1);
    }

    ret = flock(fileno(*lockfile_fd), LOCK_EX | LOCK_NB, 0, START_LOCATION, PG_CTL_LOCKFILE_SIZE);

    return ret;
}

static int pg_ctl_unlock(FILE* lockfile_fd)
{
    int ret = -1;

    if (lockfile_fd != NULL) {
        ret = flock(fileno(lockfile_fd), LOCK_UN, 0, START_LOCATION, PG_CTL_LOCKFILE_SIZE);
        fclose(lockfile_fd);
        lockfile_fd = NULL;
    }

    return ret;
}

static void do_lsn_query(void)
{
    char returnmsg[XLOG_READER_MAX_MSGLENTH] = {0};
    XLogRecPtr max_lsn = InvalidXLogRecPtr;
    TimeLineID tli = 1;
    pg_crc32 maxLsnCrc = 0;

    max_lsn = FindMaxLSN(pg_data, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc);
    pg_log(PG_PRINT, "LSN query result:\n");
    pg_log(PG_PRINT, "   MAX_LSN: %X/%X\n", (uint32)(max_lsn >> 32), (uint32)max_lsn);
    recordReadTest(pg_data, querylsn, tli);
    return;
}

static void do_query(void)
{
#define NumCommands 3
    PGconn* conn = NULL;
    GaussState state;
    pid_t pid = 0;
    int commandIndex = 0;
    errno_t tnRet = 0;

    const char* infoTitle[NumCommands] = {"HA state:", "Senders info:", "Receiver info:"};
    const char* sqlCommands[NumCommands] = {"SELECT local_role,static_connections,db_state,detail_information "
                                            "FROM pg_stat_get_stream_replications();",
        "SELECT sender_pid,local_role,peer_role,peer_state, "
        "state,sender_sent_location,sender_write_location, "
        "sender_flush_location,sender_replay_location, "
        "receiver_received_location,receiver_write_location, "
        "receiver_flush_location,receiver_replay_location, "
        "sync_percent,sync_state,sync_priority, "
        "sync_most_available,channel "
        "FROM pg_stat_get_wal_senders();",
        "SELECT receiver_pid,local_role,peer_role,peer_state, "
        "state,sender_sent_location,sender_write_location, "
        "sender_flush_location,sender_replay_location, "
        "receiver_received_location,receiver_write_location, "
        "receiver_flush_location,receiver_replay_location, "
        "sync_percent,channel "
        "FROM pg_stat_get_wal_receiver();"};

    const char *paxosInfoTitle[NumCommands - 1] = {"HA state:","Paxos replication info:"};
    const char *paxosSqlCommands[NumCommands - 1] = {"SELECT local_role,static_connections,db_state,detail_information "
        "FROM pg_stat_get_stream_replications();",
        "SELECT paxos_write_location,paxos_commit_location, "
        "local_write_location,local_flush_location, "
        "local_replay_location,dcf_replication_info "
        "FROM get_paxos_replication_info();"
        };

    if (islsnquery) {
        do_lsn_query();
        return;
    }
    pid = get_pgpid();
    if (pid == 0) { /* No pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        exit(1);
    } else if (!is_process_alive(pid)) {
        pg_log(PG_WARNING, _(" could not connect to the local server, the postmaster process %d is not running"), pid);
        exit(1);
    } else if (!IsMyPostmasterPid(pid, pg_config)) {
        pg_log(PG_WARNING, _(" The process recorded in PID file \"%s\" is not current server\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        exit(1);
    }

    /* connect to database */
    conn = get_connectionex();
    if ((conn == NULL) || (PQstatus(conn) != CONNECTION_OK)) {
        tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
        securec_check_c(tnRet, "\0", "\0");

        ReadDBStateFile(&state);
        display_query(&state, conn != NULL ? (const char*)PQerrorMessage(conn) : (const char*)NULL);
        PQfinish(conn);
        conn = NULL;
        return;
    }

    if (!GetPaxosValue(pg_conf_file)) {
        /* print query results */
        for (commandIndex = 0; commandIndex < NumCommands; commandIndex++) {
            pg_log(PG_PRINT, _(" %-20s\n"), infoTitle[commandIndex]);
            (void)exe_sql(conn, sqlCommands[commandIndex]);
        }
    } else {
        for (commandIndex = 0; commandIndex < NumCommands - 1; commandIndex++) {
            pg_log(PG_PRINT, _(" %-20s\n"), paxosInfoTitle[commandIndex]);
            (void)exe_sql(conn, paxosSqlCommands[commandIndex]);
        }
    }

    PQfinish(conn);
    conn = NULL;
}

static void do_switchover(uint32 term)
{
    FILE* sofile = NULL;
    pgpid_t pid;
    ServerMode run_mode;
    ServerMode origin_run_mode;
    int mode = switch_mode;
    int wait_count = 0;
    int ret;
    char term_path[MAXPGPATH];

    pg_log(PG_WARNING, _("switchover term (%u)\n"), term);

    ret = snprintf_s(term_path, MAXPGPATH, MAXPGPATH - 1, "%s/term_file", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    if ((sofile = fopen(term_path, "w")) == NULL) {
        pg_log(PG_WARNING, _("fail to open file \"%s\": %s\n"), term_path, strerror(errno));
        exit(1);
    }
    if (fwrite(&term, sizeof(uint32), 1, sofile) != 1) {
        pg_log(PG_WARNING, _("fail to write file \"%s\": %s\n"), term_path, strerror(errno));
        fclose(sofile);
        exit(1);
    }
    if (fclose(sofile)) {
        pg_log(PG_WARNING, _("fail to close file \"%s\": %s\n"), term_path, strerror(errno));
        exit(1);
    }
    sofile = NULL;

    if (shutdown_mode == IMMEDIATE_MODE || switch_mode == SmartDemote) {
        pg_log(PG_WARNING, _("datanode does not support smart or immediate switchover\n"));
        exit(1);
    }

    pid = get_pgpid();
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        exit(1);
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot switchover server; "
              "single-user server is running (PID: %ld)\n"),
            pid);
        exit(1);
    }

    origin_run_mode = run_mode = get_runmode();
    if (run_mode == PRIMARY_MODE) {
        pg_log(PG_WARNING, _("switchover completed (%s)\n"), pg_data);
        return;
    } else if (UNKNOWN_MODE == run_mode) {
        pg_log(PG_WARNING,
            ("cannot switchover server: "
             "server mode is unknown\n"));
        exit(1);
    }
    /* switchover executed only in standby server */
    else if (run_mode != STANDBY_MODE && run_mode != CASCADE_STANDBY_MODE) {
        pg_log(PG_WARNING,
            _(" cannot switchover server; "
              "server is not in standby or cascade standby mode\n"));
        exit(1);
    }
    if ((sofile = fopen(switchover_file, "w")) == NULL) {
        pg_log(
            PG_WARNING, _(" could not create switchover signal file \"%s\": %s\n"), switchover_file, strerror(errno));
        exit(1);
    }
    if (fwrite(&mode, sizeof(int), 1, sofile) != 1) {
        pg_log(PG_WARNING, _(" could not write switchover signal file \"%s\": %s\n"), switchover_file, strerror(errno));
        fclose(sofile);
        sofile = NULL;
        exit(1);
    }
    if (fclose(sofile)) {
        pg_log(PG_WARNING, _(" could not close switchover signal file \"%s\": %s\n"), switchover_file, strerror(errno));
        sofile = NULL;
        exit(1);
    }
    sofile = NULL;

    sig = SIGUSR1;
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _(" could not send switchover signal (PID: %ld): %s\n"), pid, strerror(errno));
        if (unlink(switchover_file) != 0)
            pg_log(PG_WARNING, _(" could not send switchover signal (PID: %ld): %s\n"), pid, strerror(errno));
        exit(1);
    }

    if (!do_wait) {
        pg_log(PG_WARNING, _("server starting switchover\n"));
        return;
    } else {
        if (g_dcfEnabled) {
            /* 
             * Write timout into timeout_file so that timeout can be read in DCF mode.
             * dcf_promote_leader take this timeout as its param.
             */
            if ((sofile = fopen(timeout_file, "w")) != NULL) {
                fwrite(&wait_seconds, sizeof(wait_seconds), 1, sofile);
                fclose(sofile);
                sofile = NULL;
            }
        }
        int failed_count = 0;
        pg_log(PG_WARNING, _("waiting for server to switchover..."));

        /*
         * We sleep 1 second if the current runmode isn't primary and then try
         * again, after wait_seconds, gs_ctl will exit.
         */
        bool isGetStatus = false; /* Mark if we get promote status in DCF mode */
        while (wait_count++ < wait_seconds) {
            /* Check promote status in DCF mode */
            if (g_dcfEnabled && !isGetStatus && (sofile = fopen(switchover_status_file, "r")) != NULL) {
                int status = 0;
                pg_log(PG_PRINT, ".");
                /* Sleep 1s in case the data hasn't been flushed to disk */
                pg_usleep(1000000); /* 1 sec */
                wait_count++;
                int ret = fread(&status, sizeof(status), 1, sofile);
                if (ret != 1) {
                    fclose(sofile);
                    sofile = NULL;
                    pg_log(PG_WARNING, _("Read %s failed!\n"), switchover_status_file);
                    exit(1);
                }
                fclose(sofile);
                sofile = NULL;
                isGetStatus = true;
                unlink(switchover_status_file);
                /* DCF promote failed */
                if (status != 0) {
                    pg_log(PG_WARNING, _("DCF switchover failed with status %d!\n"), status);
                    exit(1);
                }
            }
            if ((run_mode = get_runmode()) == origin_run_mode) {
                pg_log(PG_PRINT, ".");
                pg_usleep(1000000); /* 1 sec */
            }
            /*
             * we query the status of server, if connection is failed, it will
             * retry 3 times.
             */
            else if (failed_count < 3) {
                failed_count++;
                pg_log(PG_PRINT, ".");
                pg_usleep(1000000); /* 1 sec */
            } else {
                break;
            }
        }
        pg_log(PG_PRINT, _("\n"));
        if ((origin_run_mode == STANDBY_MODE && run_mode != PRIMARY_MODE) ||
            (origin_run_mode == CASCADE_STANDBY_MODE && run_mode != STANDBY_MODE)) {
            pg_log(PG_WARNING, _("\n switchover timeout after %d seconds. please manually check the cluster status.\n"), wait_seconds);
        } else {
            pg_log(PG_PROGRESS, _("done\n"));
            pg_log(PG_PROGRESS, _("switchover completed (%s)\n"), pg_data);
        }
    }
}

/*
 *	utility routines
 */
static bool postmaster_is_alive(pid_t pid)
{
    /*
     * Test to see if the process is still there.  Note that we do not
     * consider an EPERM failure to mean that the process is still there;
     * EPERM must mean that the given PID belongs to some other userid, and
     * considering the permissions on $PGDATA, that means it's not the
     * postmaster we are after.
     *
     * Don't believe that our own PID or parent shell's PID is the postmaster,
     * either.	(Windows hasn't got getppid(), though.)
     */
    if (pid == getpid()) {
        return false;
    }
#ifndef WIN32
    if (pid == getppid()) {
        return false;
    }
#endif
    if (kill(pid, 0) == 0) {
        return true;
    }
    return false;
}

static void do_status(void)
{
    pgpid_t pid;

    pid = get_pgpid();
    /* Is there a pid file? */
    if (pid != 0) {
        /* standalone backend? */
        if (pid < 0) {
            pid = -pid;
            if (postmaster_is_alive((pid_t)pid)) {
                printf(_("%s: single-user server is running (PID: %ld)\n"), progname, pid);
                return;
            }
        } else
        /* must be a postmaster */
        {
#ifndef WIN32
            if (postmaster_is_alive((pid_t)pid) && !IsMyPostmasterPid(pid, pg_config)) {
                printf(_("%s: The process recorded in PID file \"%s\" is not current server\n"), progname, pid_file);
                printf(_("Is server running?\n"));
                exit(1);
            }
#endif
            if (postmaster_is_alive((pid_t)pid)) {
                char** optlines = NULL;
                uint32 iter = 0;

                printf(_("%s: server is running (PID: %ld)\n"), progname, pid);

                optlines = readfile(postopts_file);
                if (optlines != NULL) {
                    for (; *(optlines + iter) != NULL; iter++)
                        fputs(*(optlines + iter), stdout);
                    freefile(optlines);
                    optlines = NULL;
                }
                return;
            }
        }
    }
    printf(_("no server running\n"));

    /*
     * The Linux Standard Base Core Specification 3.1 says this should return
     * '3'
     * http://refspecs.freestandards.org/LSB_3.1.1/LSB-Core-generic/LSB-Core-ge
     * neric/iniscrptact.html
     */
    exit(3);
}

static void do_kill(pgpid_t pid)
{
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _(" could not send signal %d (PID: %ld): %s\n"), sig, pid, strerror(errno));
        exit(1);
    }
}

static void do_build_query(void)
{
    pgpid_t build_pid = 0;
    pgpid_t master_pid = 0;
    GaussState gstate;
    ServerMode mode = UNKNOWN_MODE;
    errno_t tnRet = 0;

    master_pid = get_pgpid();
    if (master_pid > 0 && postmaster_is_alive((pid_t)master_pid))
        mode = get_runmode();

    tnRet = memset_s(&gstate, sizeof(gstate), 0, sizeof(gstate));
    securec_check_c(tnRet, "\0", "\0");
    build_pid = get_build_pid();
    if (build_pid < 0) {
        gstate.mode = mode;
        gstate.conn_num = get_replconn_number(pg_conf_file);
        gstate.build_info.process_schedule = FAIL_PERCENT;
        display_build_query((const GaussState*)&gstate);
        exit(1);
    } else if (build_pid == 0) {
        /*
         * Build pid is 0 means that current build has been
         * completed. If the master is alive, but we can not
         * access it to get a valid mode, use dbstate mode instead.
         */
        ReadDBStateFile(&gstate);
        if (0 == master_pid || !postmaster_is_alive(master_pid) || mode != UNKNOWN_MODE)
            gstate.mode = mode;
        gstate.conn_num = get_replconn_number(pg_conf_file);
        gstate.state = NORMAL_STATE;
        gstate.build_info.process_schedule = MAX_PERCENT;
        display_build_query((const GaussState*)&gstate);
    } else {
        /*
         * If the build process is not alive, it means that current
         * build has been stopped or failed. Maybe the server is
         * alive, so show the server mode instead of dbstate mode.
         */
        ReadDBStateFile(&gstate);
        if (!is_process_alive(build_pid)) {
            gstate.mode = mode;
            gstate.build_info.process_schedule = FAIL_PERCENT;
        }
        display_build_query((const GaussState*)&gstate);
    }
}

static void display_build_query(const GaussState* state)
{
#define INFO_LEN 32
    /* Ha state options */
    const char* role_opts = "local_role";
    const char* conn_opts = "static_connections";
    const char* state_opts = "db_state";
    const char* syncmode_opts = "sync_mode";

    const char* build_mode_opts = "build_mode";
    const char* data_synced_opts = "data_synchronized";
    const char* total_data_opts = "estimated_total_data";
    const char* process_opts = "process_schedule";
    const char* remain_time_opts = "estimated_remaining_time";

    char* local_role = NULL;
    char db_state[INFO_LEN] = {0};
    char* dataSize = NULL;
    char* dataSizeTotl = NULL;
    char* estimatedTime = NULL;
    errno_t tnRet = EOK;

    if (state == NULL) {
        pg_log(PG_WARNING, _(" display querybuild failed could not get information from gaussdb.state\n"));
        exit(1);
    }
    local_role = get_string_by_mode(state->mode);

    if (state->build_info.process_schedule == FAIL_PERCENT) {
        tnRet = strncpy_s(db_state, INFO_LEN, "Build failed", INFO_LEN - 1);
        securec_check_c(tnRet, "\0", "\0");
    } else if (state->state == BUILDING_STATE) {
        tnRet = strncpy_s(db_state, INFO_LEN, "Building", INFO_LEN - 1);
        securec_check_c(tnRet, "\0", "\0");
    } else {
        tnRet = strncpy_s(db_state, INFO_LEN, "Build completed", INFO_LEN - 1);
        securec_check_c(tnRet, "\0", "\0");
    }
    db_state[INFO_LEN - 1] = '\0';

    /* printf HA state */
    pg_log(PG_PRINT, _(" HA state:\n"));
    pg_log(PG_PRINT, _("        %-30s: %s\n"), role_opts, local_role);
    pg_log(PG_PRINT, _("        %-30s: %d\n"), conn_opts, state->conn_num);
    pg_log(PG_PRINT, _("        %-30s: %s\n"), state_opts, db_state);
    pg_log(PG_PRINT, _("        %-30s: %s\n"), syncmode_opts, get_string_by_sync_mode(state->sync_stat));

    /* printf build state */
    if (state->state == BUILDING_STATE && state->build_info.process_schedule != FAIL_PERCENT) {
        pg_log(PG_PRINT, _(" Build state:\n"));
        if (state->build_info.build_mode == FULL_BUILD)
            pg_log(PG_PRINT, _("        %-30s: Full\n"), build_mode_opts);
        else if (state->build_info.build_mode == INC_BUILD)
            pg_log(PG_PRINT, _("        %-30s: Incremental\n"), build_mode_opts);
        dataSize = show_datasize(state->build_info.total_size);
        dataSizeTotl = show_datasize(state->build_info.total_done);
        estimatedTime = show_estimated_time(state->build_info.estimated_time);
        pg_log(PG_PRINT, _("        %-30s: %s\n"), data_synced_opts, dataSizeTotl);
        pg_log(PG_PRINT, _("        %-30s: %s\n"), total_data_opts, dataSize);
        pg_log(PG_PRINT, _("        %-30s: %d%%\n"), process_opts, state->build_info.process_schedule);
        pg_log(PG_PRINT,
            _("        %-30s: %s\n"),
            remain_time_opts,
            estimatedTime);
        free(dataSize);
        free(dataSizeTotl);
        free(estimatedTime);
    }
    pg_log(PG_PRINT, _("\n"));
}

static void display_query(GaussState* state, const char* errormsg)
{
#define MAX_INFO 1024
    /* Ha state options */
    const char* role_opts = "local_role";
    const char* conn_opts = "static_connections";
    const char* state_opts = "db_state";
    const char* detail_opts = "detail_information";
    const char* syncmode_opts = "sync_mode";
    const char* default_reason = "Access denied";

    int static_connections = 0;
    char detail_info[MAX_INFO] = {0};
    errno_t tnRet = 0;

    if (state == NULL) {
        pg_log(PG_PRINT, _("%s: display query failed, could not get information from gaussdb.state\n"), progname);
        exit(1);
    }

    if ((errormsg == NULL) || (*errormsg == '\0')) {
        tnRet = strncpy_s(detail_info, MAX_INFO, default_reason, strlen(default_reason) + 1);
        securec_check_c(tnRet, "\0", "\0");
    } else {
        tnRet = strncpy_s(detail_info, MAX_INFO, errormsg, strlen(errormsg) + 1);
        securec_check_c(tnRet, "\0", "\0");
    }

    static_connections = state->conn_num;

    /* print state header */
    pg_log(PG_PRINT, _(" HA state:\n"));
    /* print local role */
    pg_log(PG_PRINT, _("        %-30s: %s\n"), role_opts, get_string_by_mode(state->mode));
    /* print static connections */
    pg_log(PG_PRINT, _("        %-30s: %d\n"), conn_opts, static_connections);
    /* print db state */
    pg_log(PG_PRINT, _("        %-30s: %s\n"), state_opts, get_string_by_state(state->state));
    /* print detail information */
    pg_log(PG_PRINT, _("        %-30s: %s\n"), detail_opts, detail_info);
    /* print sync mode */
    pg_log(PG_PRINT, _("        %-30s: %s\n"), syncmode_opts, get_string_by_sync_mode(state->sync_stat));

    if (!GetPaxosValue(pg_conf_file)) {
        pg_log(PG_PRINT, _("\n"));
        /* print no info about sender and receiver */
        pg_log(PG_PRINT, _(" Senders info:\n"));
        pg_log(PG_PRINT, _("        No information\n"));
        pg_log(PG_PRINT, _(" Receiver info:\n"));
        pg_log(PG_PRINT, _("        No information\n"));
    }
}

static bool is_process_alive(pgpid_t pid)
{
    if (pid == getpid())
        return false;
#ifndef WIN32
    if (pid == getppid())
        return false;
#endif
    if (kill(pid, 0) == 0)
        return true;
    return false;
}

static char* get_string_by_mode(ServerMode s_mode)
{
    switch (s_mode) {
        case NORMAL_MODE:
            return "Normal";
        case PRIMARY_MODE:
            return "Primary";
        case STANDBY_MODE:
            return "Standby";
        case CASCADE_STANDBY_MODE:
            return "Cascade Standby";
        case PENDING_MODE:
            return "Pending";
        default:
            return "Unknown";
    }
}
static char* get_string_by_state(DbState db_state)
{
    switch (db_state) {
        case NORMAL_STATE:
            return "Normal";
        case UNKNOWN_STATE:
            return "Unknown";
        case NEEDREPAIR_STATE:
            return "Need repair";
        case STARTING_STATE:
            return "Starting";
        case WAITING_STATE:
            return "Wait promoting";
        case DEMOTING_STATE:
            return "Demoting";
        case PROMOTING_STATE:
            return "Promoting";
        case BUILDING_STATE:
            return "Building";
        case CATCHUP_STATE:
            return "Catchup";
        case COREDUMP_STATE:
            return "Coredump";
        default:
            return "Unknown";
    }
}

static char* get_string_by_sync_mode(bool syncmode)
{
    if (syncmode) {
        return "Sync";
    } else {
        return "Async";
    }
}

void ReadDBStateFile(GaussState* state)
{
    FILE* statef = NULL;

    if (state == NULL) {
        pg_log(PG_WARNING, _(" Could not get information from gaussdb.state\n"));
        return;
    }

    statef = fopen(gaussdb_state_file, "r");
    if (statef == NULL) {
        if (errno == ENOENT) {
            pg_log(PG_WARNING, _(" file \"%s\" is not exist\n"), gaussdb_state_file);
        } else {
            pg_log(PG_WARNING, _(" open file \"%s\" failed : %s\n"), gaussdb_state_file, strerror(errno));
        }
        exit(1);
    }
    if (0 == fread(state, 1, sizeof(GaussState), statef)) {
        pg_log(PG_WARNING, _(" read file \"%s\" failed\n"), gaussdb_state_file);
    }
    fclose(statef);
    statef = NULL;
}

#if defined(WIN32) || defined(__CYGWIN__)

static bool pgwin32_IsInstalled(SC_HANDLE hSCM)
{
    SC_HANDLE hService = OpenService(hSCM, register_servicename, SERVICE_QUERY_CONFIG);
    bool bResult = (hService != NULL);

    if (bResult)
        CloseServiceHandle(hService);
    return bResult;
}

static char* pgwin32_CommandLine(bool registration)
{
    static char cmdLine[MAXPGPATH];
    int ret = 0;
    char* cmdLineEnd = NULL;

#ifdef __CYGWIN__
    char buf[MAXPGPATH];
#endif

    if (registration) {
        ret = find_my_exec(argv0, cmdLine);
        if (ret != 0) {
            pg_log(PG_WARNING, _(" could not find own program executable\n"));
            exit(1);
        }
    } else {
        ret = find_other_exec(argv0, "gaussdb", PG_BACKEND_VERSIONSTR, cmdLine);
        if (ret != 0) {
            pg_log(PG_WARNING, _(" could not find gaussdb program executable\n"));
            exit(1);
        }
    }

#ifdef __CYGWIN__
    /* need to convert to windows path */
#if CYGWIN_VERSION_DLL_MAJOR >= 1007
    cygwin_conv_path(CCP_POSIX_TO_WIN_A, cmdLine, buf, sizeof(buf));
#else
    cygwin_conv_to_full_win32_path(cmdLine, buf);
#endif
    ret = strcpy_s(cmdLine, sizeof(cmdLine), buf);
    securec_check_c(ret, "", "");
#endif

    if (NULL != registration) {
        if (pg_strcasecmp(cmdLine + strlen(cmdLine) - 4, ".exe") != 0) {
            /* If commandline does not end in .exe, append it */
            ret = strcat_s(cmdLine, sizeof(cmdLine), ".exe");
            securec_check_c(ret, "\0", "\0");
        }
        ret = strcat_s(cmdLine, sizeof(cmdLine), " runservice -N \"");
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(cmdLine, sizeof(cmdLine), register_servicename);
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(cmdLine, sizeof(cmdLine), "\"");
        securec_check_c(ret, "\0", "\0");
    }

    if (pg_config != NULL) {
        ret = strcat_s(cmdLine, sizeof(cmdLine), " -D \"");
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(cmdLine, sizeof(cmdLine), pg_config);
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(cmdLine, sizeof(cmdLine), "\"");
        securec_check_c(ret, "\0", "\0");
    }

    if (registration != NULL && do_wait != NULL) {
        ret = strcat_s(cmdLine, sizeof(cmdLine), " -w");
        securec_check_c(ret, "\0", "\0");
    }

    if (registration && wait_seconds != DEFAULT_WAIT) {
        /* concatenate */
        cmdLineEnd = cmdLine + strlen(cmdLine);
        ret = snprintf_s(cmdLine + strlen(cmdLine),
            MAXPGPATH - strlen(cmdLine),
            MAXPGPATH - strlen(cmdLine) - 1,
            " -t %d",
            wait_seconds);
        securec_check_ss_c(ret, "\0", "\0");
    }

    if (registration && silent_mode) {
        ret = strcat_s(cmdLine, sizeof(cmdLine), " -s");
        securec_check_c(ret, "\0", "\0");
    }

    if (post_opts != NULL) {
        ret = strcat_s(cmdLine, sizeof(cmdLine), " ");
        securec_check_c(ret, "\0", "\0");
        if (registration) {
            ret = strcat_s(cmdLine, sizeof(cmdLine), " -o \"");
            securec_check_c(ret, "\0", "\0");
        }
        ret = strcat_s(cmdLine, sizeof(cmdLine), post_opts);
        securec_check_c(ret, "\0", "\0");
        if (registration) {
            ret = strcat_s(cmdLine, sizeof(cmdLine), "\"");
            securec_check_c(ret, "\0", "\0");
        }
    }

    return cmdLine;
}

static void pgwin32_doRegister(void)
{
    SC_HANDLE hService = NULL;
    SC_HANDLE hSCM = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
    if (hSCM == NULL) {
        pg_log(PG_WARNING, _(" could not open service manager\n"));
        exit(1);
    }
    if (pgwin32_IsInstalled(hSCM)) {
        CloseServiceHandle(hSCM);
        pg_log(PG_WARNING, _(" service \"%s\" already registered\n"), register_servicename);
        exit(1);
    }

    if ((hService = CreateService(hSCM,
        register_servicename,
        register_servicename,
        SERVICE_ALL_ACCESS,
        SERVICE_WIN32_OWN_PROCESS,
        pgctl_start_type,
        SERVICE_ERROR_NORMAL,
        pgwin32_CommandLine(true),
        NULL,
        NULL,
        "RPCSS\0",
        register_username,
        register_password)) == NULL) {
        CloseServiceHandle(hSCM);
        pg_log(PG_WARNING,
            _(" could not register service \"%s\": error code %lu\n"),
            register_servicename,
            GetLastError());
        exit(1);
    }
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);
}

static void pgwin32_doUnregister(void)
{
    SC_HANDLE hService = NULL;
    SC_HANDLE hSCM = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
    if (hSCM == NULL) {
        pg_log(PG_WARNING, _(" could not open service manager\n"));
        exit(1);
    }
    if (!pgwin32_IsInstalled(hSCM)) {
        CloseServiceHandle(hSCM);
        pg_log(PG_WARNING, _(" service \"%s\" not registered\n"), register_servicename);
        exit(1);
    }

    if ((hService = OpenService(hSCM, register_servicename, DELETE)) == NULL) {
        CloseServiceHandle(hSCM);
        pg_log(PG_WARNING, _(" could not open service \"%s\": error code %lu\n"), register_servicename, GetLastError());
        exit(1);
    }
    if (!DeleteService(hService)) {
        CloseServiceHandle(hService);
        CloseServiceHandle(hSCM);
        pg_log(PG_WARNING,
            _(" could not unregister service \"%s\": error code %lu\n"),
            register_servicename,
            GetLastError());
        exit(1);
    }
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);
}

static void pgwin32_SetServiceStatus(DWORD currentState)
{
    status.dwCurrentState = currentState;
    SetServiceStatus(hStatus, (LPSERVICE_STATUS)&status);
}

static void WINAPI pgwin32_ServiceHandler(DWORD request)
{
    switch (request) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:

            /*
             * We only need a short wait hint here as it just needs to wait
             * for the next checkpoint. They occur every 5 seconds during
             * shutdown
             */
            status.dwWaitHint = 10000;
            pgwin32_SetServiceStatus(SERVICE_STOP_PENDING);
            SetEvent(shutdownEvent);
            return;

        case SERVICE_CONTROL_PAUSE:
            /* Win32 config reloading */
            status.dwWaitHint = 5000;
            kill(postmasterPID, SIGHUP);
            return;

            /* These could be used to replace other signals etc */
        case SERVICE_CONTROL_CONTINUE:
        case SERVICE_CONTROL_INTERROGATE:
        default:
            break;
    }
}

static void WINAPI pgwin32_ServiceMain(DWORD argc, LPTSTR* argv)
{
    PROCESS_INFORMATION pi;
    DWORD ret;
    errno_t tnRet = 0;

    /* Initialize variables */
    status.dwWin32ExitCode = S_OK;
    status.dwCheckPoint = 0;
    status.dwWaitHint = 60000;
    status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    status.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN | SERVICE_ACCEPT_PAUSE_CONTINUE;
    status.dwServiceSpecificExitCode = 0;
    status.dwCurrentState = SERVICE_START_PENDING;

    tnRet = memset_s(&pi, sizeof(pi) 0, sizeof(pi));
    securec_check_c(tnRet, "\0", "\0");

    read_post_opts();

    /* Register the control request handler */
    if ((hStatus = RegisterServiceCtrlHandler(register_servicename, pgwin32_ServiceHandler)) ==
        (SERVICE_STATUS_HANDLE)0)
        return;

    if ((shutdownEvent = CreateEvent(NULL, true, false, NULL)) == NULL)
        return;

    /* Start the postmaster */
    pgwin32_SetServiceStatus(SERVICE_START_PENDING);
    if (!CreateRestrictedProcess(pgwin32_CommandLine(false), &pi, true)) {
        pgwin32_SetServiceStatus(SERVICE_STOPPED);
        return;
    }
    postmasterPID = pi.dwProcessId;
    postmasterProcess = pi.hProcess;
    CloseHandle(pi.hThread);

    if (do_wait) {
        write_eventlog(EVENTLOG_INFORMATION_TYPE, _("Waiting for server startup...\n"));
        if (test_postmaster_connection(postmasterPID, true) != PQPING_OK) {
            write_eventlog(EVENTLOG_ERROR_TYPE, _("Timed out waiting for server startup\n"));
            pgwin32_SetServiceStatus(SERVICE_STOPPED);
            return;
        }
        write_eventlog(EVENTLOG_INFORMATION_TYPE, _("Server started and accepting connections\n"));
    }

    pgwin32_SetServiceStatus(SERVICE_RUNNING);

    /* Wait for quit... */
    ret = WaitForMultipleObjects(2, shutdownHandles, FALSE, INFINITE);

    pgwin32_SetServiceStatus(SERVICE_STOP_PENDING);
    switch (ret) {
        case WAIT_OBJECT_0: /* shutdown event */
            kill(postmasterPID, SIGINT);

            /*
             * Increment the checkpoint and try again Abort after 12
             * checkpoints as the postmaster has probably hung
             */
            while (WaitForSingleObject(postmasterProcess, 5000) == WAIT_TIMEOUT && status.dwCheckPoint < 12)
                status.dwCheckPoint++;
            break;

        case (WAIT_OBJECT_0 + 1): /* postmaster went down */
            break;

        default:
            /* shouldn't get here? */
            break;
    }

    CloseHandle(shutdownEvent);
    CloseHandle(postmasterProcess);

    pgwin32_SetServiceStatus(SERVICE_STOPPED);
}

static void pgwin32_doRunAsService(void)
{
    SERVICE_TABLE_ENTRY st[] = {{register_servicename, pgwin32_ServiceMain}, {NULL, NULL}};
    if (StartServiceCtrlDispatcher(st) == 0) {

        pg_log(
            PG_WARNING, _(" could not start service \"%s\": error code %lu\n"), register_servicename, GetLastError());
        exit(1);
    }
}

/*
 * Mingw headers are incomplete, and so are the libraries. So we have to load
 * a whole lot of API functions dynamically. Since we have to do this anyway,
 * also load the couple of functions that *do* exist in minwg headers but not
 * on NT4. That way, we don't break on NT4.
 */
typedef BOOL(WINAPI* __CreateRestrictedToken)(
    HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);
typedef BOOL(WINAPI* __IsProcessInJob)(HANDLE, HANDLE, PBOOL);
typedef HANDLE(WINAPI* __CreateJobObject)(LPSECURITY_ATTRIBUTES, LPCTSTR);
typedef BOOL(WINAPI* __SetInformationJobObject)(HANDLE, JOBOBJECTINFOCLASS, LPVOID, DWORD);
typedef BOOL(WINAPI* __AssignProcessToJobObject)(HANDLE, HANDLE);
typedef BOOL(WINAPI* __QueryInformationJobObject)(HANDLE, JOBOBJECTINFOCLASS, LPVOID, DWORD, LPDWORD);

/* Windows API define missing from some versions of MingW headers */
#ifndef DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE 0x1
#endif

/*
 * Create a restricted token, a job object sandbox, and execute the specified
 * process with it.
 *
 * Returns 0 on success, non-zero on failure, same as CreateProcess().
 *
 * On NT4, or any other system not containing the required functions, will
 * launch the process under the current token without doing any modifications.
 *
 * NOTE! Job object will only work when running as a service, because it's
 * automatically destroyed when pg_ctl exits.
 */
static int CreateRestrictedProcess(char* cmd, PROCESS_INFORMATION* processInfo, bool as_service)
{
    int r;
    BOOL b;
    STARTUPINFO si;
    HANDLE origToken = NULL;
    HANDLE restrictedToken = NULL;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    SID_AND_ATTRIBUTES dropSids[2];
    int ret;

    /* Functions loaded dynamically */
    __CreateRestrictedToken _CreateRestrictedToken = NULL;
    __IsProcessInJob _IsProcessInJob = NULL;
    __CreateJobObject _CreateJobObject = NULL;
    __SetInformationJobObject _SetInformationJobObject = NULL;
    __AssignProcessToJobObject _AssignProcessToJobObject = NULL;
    __QueryInformationJobObject _QueryInformationJobObject = NULL;
    HANDLE Kernel32Handle = NULL;
    HANDLE Advapi32Handle = NULL;

    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);

    Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
    if (Advapi32Handle != NULL) {
        _CreateRestrictedToken = (__CreateRestrictedToken)GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
    }

    if (_CreateRestrictedToken == NULL) {
        /*
         * NT4 doesn't have CreateRestrictedToken, so just call ordinary
         * CreateProcess
         */
        pg_log(PG_WARNING, _(" WARNING: cannot create restricted tokens on this platform\n"));
        if (Advapi32Handle != NULL)
            FreeLibrary(Advapi32Handle);
        return CreateProcess(NULL, cmd, NULL, NULL, FALSE, 0, NULL, NULL, &si, processInfo);
    }

    /* Open the current token to use as a base for the restricted one */
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken)) {
        pg_log(PG_WARNING, _(" could not open process token: error code %lu\n"), GetLastError());
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
        pg_log(PG_WARNING, _(" could not allocate SIDs: error code %lu\n"), GetLastError());
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
        pg_log(PG_WARNING, _(" could not create restricted token: error code %lu\n"), GetLastError());
        return 0;
    }

#ifndef __CYGWIN__
    AddUserToTokenDacl(restrictedToken);
#endif

    r = CreateProcessAsUser(
        restrictedToken, NULL, cmd, NULL, NULL, TRUE, CREATE_SUSPENDED, NULL, NULL, &si, processInfo);

    Kernel32Handle = LoadLibrary("KERNEL32.DLL");
    if (Kernel32Handle != NULL) {
        _IsProcessInJob = (__IsProcessInJob)GetProcAddress(Kernel32Handle, "IsProcessInJob");
        _CreateJobObject = (__CreateJobObject)GetProcAddress(Kernel32Handle, "CreateJobObjectA");
        _SetInformationJobObject = (__SetInformationJobObject)GetProcAddress(Kernel32Handle, "SetInformationJobObject");
        _AssignProcessToJobObject =
            (__AssignProcessToJobObject)GetProcAddress(Kernel32Handle, "AssignProcessToJobObject");
        _QueryInformationJobObject =
            (__QueryInformationJobObject)GetProcAddress(Kernel32Handle, "QueryInformationJobObject");
    }

    /* Verify that we found all functions */
    if (_IsProcessInJob == NULL || _CreateJobObject == NULL || _SetInformationJobObject == NULL ||
        _AssignProcessToJobObject == NULL || _QueryInformationJobObject == NULL) {
        /*
         * IsProcessInJob() is not available on < WinXP, so there is no need
         * to log the error every time in that case
         */
        OSVERSIONINFO osv;

        osv.dwOSVersionInfoSize = sizeof(osv);
        if (!GetVersionEx(&osv) ||                                 /* could not get version */
            (osv.dwMajorVersion == 5 && osv.dwMinorVersion > 0) || /* 5.1=xp, 5.2=2003, etc */
            osv.dwMajorVersion > 5)                                /* anything newer should have the API */

            /*
             * Log error if we can't get version, or if we're on WinXP/2003 or
             * newer
             */
            pg_log(PG_WARNING, _(" WARNING: could not locate all job object functions in system API\n"));
    } else {
        BOOL inJob;

        if (_IsProcessInJob(processInfo->hProcess, NULL, &inJob)) {
            if (!inJob) {
                /*
                 * Job objects are working, and the new process isn't in one,
                 * so we can create one safely. If any problems show up when
                 * setting it, we're going to ignore them.
                 */
                HANDLE job = NULL;
                char jobname[MAX_JOB_NAME];

                ret = sprintf_s(jobname, MAX_JOB_NAME, "PostgreSQL_%lu", processInfo->dwProcessId);
                securec_check_ss_c(ret, "\0", "\0");

                job = _CreateJobObject(NULL, jobname);
                if (job) {
                    JOBOBJECT_BASIC_LIMIT_INFORMATION basicLimit;
                    JOBOBJECT_BASIC_UI_RESTRICTIONS uiRestrictions;
                    JOBOBJECT_SECURITY_LIMIT_INFORMATION securityLimit;
                    OSVERSIONINFO osv;

                    ZeroMemory(&basicLimit, sizeof(basicLimit));
                    ZeroMemory(&uiRestrictions, sizeof(uiRestrictions));
                    ZeroMemory(&securityLimit, sizeof(securityLimit));

                    basicLimit.LimitFlags =
                        JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION | JOB_OBJECT_LIMIT_PRIORITY_CLASS;
                    basicLimit.PriorityClass = NORMAL_PRIORITY_CLASS;
                    _SetInformationJobObject(job, JobObjectBasicLimitInformation, &basicLimit, sizeof(basicLimit));

                    uiRestrictions.UIRestrictionsClass =
                        JOB_OBJECT_UILIMIT_DESKTOP | JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
                        JOB_OBJECT_UILIMIT_EXITWINDOWS | JOB_OBJECT_UILIMIT_READCLIPBOARD |
                        JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS | JOB_OBJECT_UILIMIT_WRITECLIPBOARD;

                    if (as_service) {
                        osv.dwOSVersionInfoSize = sizeof(osv);
                        if (!GetVersionEx(&osv) || osv.dwMajorVersion < 6 ||
                            (osv.dwMajorVersion == 6 && osv.dwMinorVersion == 0)) {
                            /*
                             * On Windows 7 (and presumably later),
                             * JOB_OBJECT_UILIMIT_HANDLES prevents us from
                             * starting as a service. So we only enable it on
                             * Vista and earlier (version <= 6.0)
                             */
                            uiRestrictions.UIRestrictionsClass |= JOB_OBJECT_UILIMIT_HANDLES;
                        }
                    }
                    _SetInformationJobObject(
                        job, JobObjectBasicUIRestrictions, &uiRestrictions, sizeof(uiRestrictions));

                    securityLimit.SecurityLimitFlags = JOB_OBJECT_SECURITY_NO_ADMIN | JOB_OBJECT_SECURITY_ONLY_TOKEN;
                    securityLimit.JobToken = restrictedToken;
                    _SetInformationJobObject(
                        job, JobObjectSecurityLimitInformation, &securityLimit, sizeof(securityLimit));

                    _AssignProcessToJobObject(job, processInfo->hProcess);
                }
            }
        }
    }

    CloseHandle(restrictedToken);

    ResumeThread(processInfo->hThread);

    FreeLibrary(Kernel32Handle);

    /*
     * We intentionally don't close the job object handle, because we want the
     * object to live on until pg_ctl shuts down.
     */
    return r;
}
#endif

static void do_advice(void)
{
    pg_log(PG_PRINT, _("Try \"%s --help\" for more information.\n"), progname);
}

#ifndef ENABLE_MULTIPLE_NODES
#ifndef ENABLE_LITE_MODE
static void doDCFAddCmdHelp(void)
{
    printf(_("  %s member         [-O OPERATION] [-u DCF-NODE-ID] [-i DCF-NODE-IP] "
        "[-e DCF-NODE-PORT] [-D PRIMARY-DATADIR]\n"),
        progname);
    printf(_("  %s changerole     [-R DCF-ROLE] [-D DATADIR] [-t SEC]\n"), progname);
    printf(_("  %s setrunmode     [-x XMODE] [-v VOTE-NUM] [-D PRIMARY-DATADIR]\n"), progname);
}

static void doDCFOptionHelp(void)
{
    printf(_("\nOptions for DCF:\n"));
    printf(_("  -O, --operation=OPERATION          Operation of adding or removing or change a DN configuration.\n"));
    printf(_("  -u, --nodeid=DCF-NODE-ID           It is required for member command.\n"));
    printf(_("  -i, --ip=DCF-NODE-IP               It is required when OPERATION of member command is \"add\".\n"));
    printf(_("  -e, --port=DCF-NODE-PORT           It is required when OPERATION of member command is \"add\".\n"));
    printf(_("  -R, --role=DCF-NODE-ROLE           The option is \"follower\" or \"passive\".\n"));
    printf(_("  -v, --votenum=VOTE-NUM             It is required when XMODE is \"minority\".\n"));
    printf(_("  -x, --xmode=XMODE                  The option can be \"minority\" or \"normal\" mode in DCF.\n"));
    printf(_("  -G,                                The option is a int type to set group number in DCF.\n"));
    printf(_("  --priority,                        The option is a int type to set priority number in DCF.\n"));
}

static void doDCFOptionDesHelp(void)
{
    printf(_("\nOPERATION are:\n"));
    printf(_("  add            add a member to DCF configuration.\n"));
    printf(_("  remove         remove a member from DCF configuration.\n"));
    printf(_("  change         change DCF node configuration.\n"));
    printf(_("\nXMODE are:\n"));
    printf(_("  minority       the leader in DCF can reach consensus when getting less than half nodes' response.\n"));
    printf(_("  normal         the leader in DCF can reach consensus when getting more than half nodes' response.\n"));
    printf(_("\nDCF-NODE-ROLE are:\n"));
    printf(_("  follower       follower role in DCF\n"));
    printf(_("  passive        passive role in DCF\n"));

}
#endif
#endif

static void do_help(void)
{
    printf(_("%s is a utility to initialize, start, stop, or control a openGauss server.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s init[db]               [-D DATADIR] [-s] [-o \"OPTIONS\"]\n"), progname);
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  %s start   [-w] [-t SECS] [-Z NODE-TYPE] [-D DATADIR] [-s] [-l FILENAME] [-o \"OPTIONS\"] [-M "
             "SERVERMODE]\n"),
        progname);
    printf(_("  %s restart [-w] [-t SECS] [-Z NODE-TYPE] [-D DATADIR] [-s] [-m SHUTDOWN-MODE]\n"
             "                 [-o \"OPTIONS\"]\n"),
        progname);
    printf(_("  %s build   [-D DATADIR] [-Z NODE-TYPE] [-b BUILD_MODE] [-r SECS] [-C CONNECTOR] [-q]\n"), progname);
    printf(_("  %s restore [-D DATADIR] [-Z NODE-TYPE] [-s] [--remove-backup]\n"), progname);
#else
    printf(
        _("  %s start   [-w] [-t SECS] [-D DATADIR] [-s] [-l FILENAME] [-o \"OPTIONS\"] [-M SERVERMODE]\n"), progname);
    printf(_("  %s restart [-w] [-t SECS] [-D DATADIR] [-s] [-m SHUTDOWN-MODE]\n"
             "                 [-o \"OPTIONS\"]\n"),
        progname);
    printf(_("  %s build   [-D DATADIR] [-b MODE] [-r SECS] [-C CONNECTOR] [-q] [-M SERVERMODE]\n"), progname);
#endif

    printf(_("  %s stop    [-W] [-t SECS] [-D DATADIR] [-s] [-m SHUTDOWN-MODE]\n"), progname);
    printf(_("  %s reload  [-D DATADIR] [-s]\n"), progname);
    printf(_("  %s status  [-D DATADIR]\n"), progname);
    printf(_("  %s finishredo [-D DATADIR] [-s]\n"), progname);
    (void)printf(
        _("  %s failover               [-W] [-t SECS] [-D DATADIR] [-U USERNAME] [-P PASSWORD] [-T TERM]\n"), progname);
    (void)printf(_("  %s switchover             [-W] [-D DATADIR] [-m SWITCHOVER-MODE] [-U USERNAME] [-P PASSWORD] [-f]\n"),
        progname);
    (void)printf(_("  %s query   [-D DATADIR] [-U USERNAME] [-P PASSWORD] [-L lsn]\n"), progname);
    (void)printf(_("  %s notify   -M SERVERMODE [-D DATADIR] [-U USERNAME] [-P PASSWORD]\n"), progname);
    printf(_("  %s kill    SIGNALNAME PID\n"), progname);
#if defined(WIN32) || defined(__CYGWIN__)
    printf(_("  %s register   [-N SERVICENAME] [-U USERNAME] [-P PASSWORD] [-D DATADIR]\n"
             "                    [-S START-TYPE] [-w] [-t SECS] [-o \"OPTIONS\"]\n"),
        progname);
    printf(_("  %s unregister [-N SERVICENAME]\n"), progname);
#endif
    (void)printf(_("  %s querybuild   [-D DATADIR]\n"), progname);
    printf(_("  %s copy   [-D DATADIR] [-Q COPYMODE]\n"), progname);
#if defined(ENABLE_MULTIPLE_NODES) || (defined(ENABLE_PRIVATEGAUSS) && (!defined(ENABLE_LITE_MODE)))
    (void)printf(_("  %s hotpatch  [-D DATADIR] [-a ACTION] [-n NAME]\n"), progname);
#endif
#ifndef ENABLE_MULTIPLE_NODES
#ifndef ENABLE_LITE_MODE
    doDCFAddCmdHelp();
#endif
#endif

    printf(_("\nCommon options:\n"));
    printf(_("  -b,  --mode=MODE	 the mode of building the datanode or coordinator."
             "MODE can be \"full\", \"incremental\", "
             "\"auto\", \"standby_full\", \"copy_secure_files\", \"copy_upgrade_file\", \"cross_cluster_full\", "
             "\"cross_cluster_incremental\", \"cross_cluster_standby_full\"\n"));
    printf(_("  -D, --pgdata=DATADIR   location of the database storage area\n"));
    printf(_("  -s, --silent           only print errors, no informational messages\n"));
    printf(_("  -t, --timeout=SECS     seconds to wait when using -w option\n"));
    printf(_("  -V, --version          output version information, then exit\n"));
    printf(_("  -w                     wait until operation completes\n"));
    printf(_("  -W                     do not wait until operation completes\n"));
    printf(_("  -M                     the database start as the appointed  mode\n"));
    printf(_("  -T                     Failover requires a term\n"));
    printf(_("  -q                     do not start automatically after build finishing, needed start by caller\n"));
    printf(_("  -d                     more debug info will be print\n"));
    printf(_("  -L                     query lsn:XX/XX validity and show the max_lsn\n"));
    (void)printf(_("  -P PASSWORD            password of account to connect local server\n"));
    (void)printf(_("  -U USERNAME            user name of account to connect local server\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  -Z NODE-TYPE           can be \"coordinator\" or \"datanode\" (openGauss)\n"));
    printf(_("  --obsmode                  build to obs, only support full build and restore from obs\n"));
    printf(_("  -K, --slotname=SLOTNAME    which obs slot to be build or restore\n"));
    printf(_("  -k, --keycn=KEYCN          which cn used to restore\n"));
    printf(_("  -I, --taskid=TASKID        obs build task result file in obs\n"));
    printf(_("  -E, --dbport=DBPORT        database port\n"));
    printf(_("  -X, --barrierid=BARRIERID  recovery target for standby\n"));
#else
    printf(_("  -Z NODE-TYPE           can be \"single_node\"\n"));
#endif
    printf(_("  -?, -h, --help             show this help, then exit\n"));
    printf(_("(The default is to wait for shutdown, start and restart.)\n\n"));
    printf(_("If the -D option is omitted, the environment variable PGDATA is used.\n"));

    printf(_("\nOptions for start or restart:\n"));
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
    printf(_("  -c, --core-files       allow openGauss to produce core files\n"));
#else
    printf(_("  -c, --core-files       not applicable on this platform\n"));
#endif
    printf(_("  -l, --log=FILENAME     write (or append) server log to FILENAME\n"));
    printf(_("  -o OPTIONS             command line options to pass to openGauss\n"
             "                         (openGauss server executable) or gs_initdb\n"));
    printf(_("  -p PATH-TO-POSTGRES    normally not necessary\n"));
    printf(_("\nOptions for stop or restart:\n"));
    printf(_("  -m, --mode=MODE        MODE can be \"fast\" or \"immediate\"\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("\nOptions for restore:\n"));
    printf(_("  --remove-backup        Remove the pg_rewind_bak dir after restore with \"restore\" command\n"));
#endif
    printf(_("\nOptions for xlog copy:\n"));
    printf(_(
        "  -Q, --mode=MODE        MODE can be \"copy_from_local\", \"force_copy_from_local\", \"copy_from_share\"\n"));

#if defined(ENABLE_MULTIPLE_NODES) || (defined(ENABLE_PRIVATEGAUSS) && (!defined(ENABLE_LITE_MODE)))
    printf(_("\nOptions for hotpatch:\n"));
    printf(
        _("  -a ACTION  patch command, ACTION can be \"load\" \"unload\" \"active\" \"deactive\" \"info\" \"list\"\n"));
    printf(_("  -n NAME    patch name, NAME should be patch name with path\n"));
#endif
#ifndef ENABLE_MULTIPLE_NODES
#ifndef ENABLE_LITE_MODE
    doDCFOptionHelp();
    doDCFOptionDesHelp();
#endif
#endif

    printf(_("\nShutdown modes are:\n"));
    printf(_("  fast        quit directly, with proper shutdown\n"));
    printf(_("  immediate   quit without complete shutdown; will lead to recovery on restart\n"));

    (void)printf(_("\nSwitchover modes are:\n"));
    (void)printf(_("  -f          quit directly, with proper shutdown and do not perform checkpoint\n"));
    (void)printf(_("  fast        demote primary directly, with proper shutdown\n"));

    printf(_("\nSERVERMODE are:\n"));
    printf(_("  primary         database system run as a primary server, send xlog to standby server\n"));
    printf(_("  standby         database system run as a standby server, receive xlog from primary server\n"));
    printf(_("  cascade_standby database system run as a cascade standby server, receive xlog from standby server\n"));
    printf(_("  pending         database system run as a pending server, wait for promoting to primary or demoting to "
             "standby\n"
             "			       Only used in start command\n"));
    printf(_("\nAllowed signal names for kill:\n"));
    printf("  ABRT HUP INT QUIT TERM USR1 USR2\n");

#if defined(WIN32) || defined(__CYGWIN__)
    printf(_("\nOptions for register and unregister:\n"));
    printf(_("  -N SERVICENAME  service name with which to register openGauss server\n"));
    printf(_("  -P PASSWORD     password of account to register openGauss server\n"));
    printf(_("  -U USERNAME     user name of account to register openGauss server\n"));
    printf(_("  -S START-TYPE   service start type to register openGauss server\n"));

    printf(_("\nStart types are:\n"));
    printf(_("  auto       start service automatically during system startup (default)\n"));
    printf(_("  demand     start service on demand\n"));

#endif
    printf(_("\nBuild connection option:\n"));
    printf(_("  -r, --recvtimeout=INTERVAL    time that receiver waits for communication from server (in seconds)\n"));
    printf(_("  -C, connector    CN/DN connect to specified CN/DN for build\n"));

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf("\nReport bugs to GaussDB support.\n");
#else
    printf("\nReport bugs to openGauss community by raising an issue.\n");
#endif

}

static void set_mode(char* modeopt)
{
    if (strcmp(modeopt, "f") == 0 || strcmp(modeopt, "fast") == 0) {
        shutdown_mode = FAST_MODE;
        stop_mode = "fast";
        switch_mode = FastDemote;
        sig = SIGINT;
    } else if (strcmp(modeopt, "i") == 0 || strcmp(modeopt, "immediate") == 0) {
        shutdown_mode = IMMEDIATE_MODE;
        stop_mode = "immediate";
        sig = SIGQUIT;
    } else {
        pg_log(PG_WARNING, _(" unrecognized shutdown mode \"%s\"\n"), modeopt);
        exit(1);
    }
}

static void set_member_operation(const char* operationopt)
{
    if (strcmp(operationopt, "add") == 0) {
        member_operation = ADD_OPERATION;
    } else if (strcmp(operationopt, "remove") == 0) {
        member_operation = REMOVE_OPERATION;
    } else if (strcmp(operationopt, "change") == 0) {
        member_operation = CHANGE_OPERATION;
    } else {
        pg_log(PG_WARNING, _("unrecognized member operation \"%s\"\n"), operationopt);
        exit(1);
    }
}

static void set_sig(char* signame)
{
    if (strcmp(signame, "HUP") == 0)
        sig = SIGHUP;
    else if (strcmp(signame, "INT") == 0)
        sig = SIGINT;
    else if (strcmp(signame, "QUIT") == 0)
        sig = SIGQUIT;
    else if (strcmp(signame, "ABRT") == 0)
        sig = SIGABRT;
    else if (strcmp(signame, "TERM") == 0)
        sig = SIGTERM;
    else if (strcmp(signame, "USR1") == 0)
        sig = SIGUSR1;
    else if (strcmp(signame, "USR2") == 0)
        sig = SIGUSR2;
    else {
        pg_log(PG_WARNING, _(" unrecognized signal name \"%s\"\n"), signame);
        do_advice();
        exit(1);
    }
}

#if defined(WIN32) || defined(__CYGWIN__)
static void set_starttype(char* starttypeopt)
{
    if (strcmp(starttypeopt, "a") == 0 || strcmp(starttypeopt, "auto") == 0)
        pgctl_start_type = SERVICE_AUTO_START;
    else if (strcmp(starttypeopt, "d") == 0 || strcmp(starttypeopt, "demand") == 0)
        pgctl_start_type = SERVICE_DEMAND_START;
    else {
        pg_log(PG_WARNING, _(" unrecognized start type \"%s\"\n"), starttypeopt);
        do_advice();
        exit(1);
    }
}
#endif

/*
 * Brief        @@GaussDB@@
 * Description: check whether the connect info is legal
 * Return:      return true if success,false if the connect info is illegal
 */
bool CheckLegalityOfConnInfo(void)
{
    char* ConnectInfo = NULL;
    char* ConnectInfoTmp = NULL;

    ConnectInfo = strdup(conn_str);
    if (ConnectInfo == NULL) {
        return false;
    }

    /* let the tmp info equals connect info to check the base legality */
    ConnectInfoTmp = ConnectInfo;

    while (*ConnectInfoTmp != '\0') {
        if (*ConnectInfoTmp != ' ') {
            break;
        }
        ConnectInfoTmp++;
    }

    /* begin legality check */
    if (*ConnectInfoTmp == '\0' || GetLengthAndCheckReplConn(ConnectInfo) == 0) {
        free(ConnectInfo);
        ConnectInfo = NULL;
        return false;
    }

    free(ConnectInfo);
    ConnectInfo = NULL;
    return true;
}

/*
 * adjust_data_dir
 *
 * If a configuration-only directory was specified, find the real data dir.
 */
static void adjust_data_dir(void)
{
    char cmd[MAXPGPATH], filename[MAXPGPATH], *my_exec_path = NULL;
    FILE* fd = NULL;
    int nRet = 0;

    /* do nothing if we're working without knowledge of data dir */
    if (pg_config == NULL) {
        return;
    }

    /* If there is no postgresql.conf, it can't be a config-only dir */
    nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/postgresql.conf", pg_config);
    securec_check_ss_c(nRet, "\0", "\0");

    canonicalize_path(filename);
    if ((fd = fopen(filename, "r")) == NULL) {
        return;
    }
    fclose(fd);
    fd = NULL;

    /* If PG_VERSION exists, it can't be a config-only dir */
    nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/PG_VERSION", pg_config);
    securec_check_ss_c(nRet, "\0", "\0");

    canonicalize_path(filename);
    if ((fd = fopen(filename, "r")) != NULL) {
        fclose(fd);
        fd = NULL;
        return;
    }

    /* Must be a configuration directory, so find the data directory */
    /* we use a private my_exec_path to avoid interfering with later uses */
    if (exec_path == NULL) {
        my_exec_path = find_other_exec_or_die(argv0, "gaussdb", PG_BACKEND_VERSIONSTR);
    } else {
        my_exec_path = xstrdup(exec_path);
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* we just try to read config file by gaussdb, so we pretend to be datanode */
    nRet = snprintf_s(cmd,
        MAXPGPATH,
        MAXPGPATH - 1,
        SYSTEMQUOTE "\"%s\" %s%s --datanode -C data_directory" SYSTEMQUOTE,
        my_exec_path,
        pgdata_opt != NULL ? pgdata_opt : "",
        post_opts != NULL ? post_opts : "");
#else
    nRet = snprintf_s(cmd,
        MAXPGPATH,
        MAXPGPATH - 1,
        SYSTEMQUOTE "\"%s\" %s%s -C data_directory" SYSTEMQUOTE,
        my_exec_path,
        pgdata_opt ? pgdata_opt : "",
        post_opts ? post_opts : "");
#endif

    securec_check_ss_c(nRet, "\0", "\0");

    fd = popen(cmd, "r");
    if (fd == NULL) {
        pg_log(PG_WARNING, _(" could not determine the data directory using command \"%s\"\n"), cmd);
        exit(1);
    }

    if (fgets(filename, sizeof(filename), fd) == NULL) {
        pg_log(PG_WARNING, _(" could not determine the data directory using command \"%s\"\n"), cmd);
        pclose(fd);
        fd = NULL;
        exit(1);
    }

    pclose(fd);
    fd = NULL;
    free(my_exec_path);
    my_exec_path = NULL;

    /* Remove trailing newline */
    if (strchr(filename, '\n') != NULL) {
        *strchr(filename, '\n') = '\0';
    }

    free(pg_data);
    pg_data = NULL;
    pg_data = xstrdup(filename);
    canonicalize_path(pg_data);
}

static char* get_localrole_string(ServerMode mode)
{
    switch (mode) {
        case UNKNOWN_MODE:
            return "Unknown";
        case NORMAL_MODE:
            return "Normal";
        case PRIMARY_MODE:
            return "Primary";
        case STANDBY_MODE:
            return "Standby";
        case CASCADE_STANDBY_MODE:
            return "Cascade Standby";
        case PENDING_MODE:
            return "Pending";
        default:
            return "Unknown";
    }
}

static void do_build_stop(pgpid_t pid)
{
    ServerMode runmode = UNKNOWN_MODE;
    PGconn* gsconn = NULL;
    GaussState state;
    errno_t tnRet = 0;

    if (!postmaster_is_alive(pid)
#ifndef WIN32
        || !IsMyPostmasterPid((pid_t)pid, pg_config)
#endif
    ) {
        /*
         * Server has just been stopped. But if pid is wrony, maybe process
         * is still alive, so we will kill it force first.
         */
        kill_proton_force();
        return;
    }

    /* server mode check */
    gsconn = get_connectionex();
    if (PQstatus(gsconn) == CONNECTION_OK) {
        runmode = get_runmode();
    }

    if (runmode == UNKNOWN_MODE) {
        tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
        securec_check_c(tnRet, "\0", "\0");

        ReadDBStateFile(&state);
        runmode = state.mode;
    }

    /* Standby DN build from Primary DN or CN/DN build from CN */
    if (runmode != STANDBY_MODE && runmode != CASCADE_STANDBY_MODE && (conn_str == NULL)) {
        pg_log(PG_WARNING, _("The local server run as %s,build cannot be executed.\n"), get_localrole_string(runmode));
        exit(1);
    }

    do_wait = true;
    if (build_mode == FULL_BUILD || build_mode == CROSS_CLUSTER_FULL_BUILD || 
        build_mode == STANDBY_FULL_BUILD || build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD) {
        shutdown_mode = IMMEDIATE_MODE;
        sig = SIGQUIT;
        do_stop(true);
    } else {
        shutdown_mode = FAST_MODE;
        sig = SIGINT;
        do_stop(true);
    }
}

static void createRewindFile(char* prefixPath)
{
    struct tm result;
    time_t currTime = time(NULL);
    time_t intTime = 0;
    FILE* fd = NULL;
    uint32_t writeSize = 0;
    int errorno = 0;
    char rewindPath[MAXPGPATH] = {0};

    errorno = snprintf_s(rewindPath, MAXPGPATH, MAXPGPATH - 1, "%s/rewind_lable", pg_data);
    securec_check_ss_c(errorno, "\0", "\0");

    char buf[MAXPGPATH] = {0};
    if (NULL == localtime_r(&currTime, &result)) {
        pg_log(PG_WARNING, _("%s: create rewind file failed(%s).\n"), progname, rewindPath);
        return;
    }

    intTime = mktime(&result);
    errorno = snprintf_s(buf, MAXPGPATH, MAXPGPATH - 1, "%d", intTime);
    securec_check_ss_c(errorno, "\0", "\0");

    fd = fopen(rewindPath, "w+");
    if (fd == NULL) {
        pg_log(PG_WARNING, _("%s: open rewind file failed(%s).\n"), progname, rewindPath);
        return;
    }

    writeSize = fwrite(buf, 1, strlen(buf), fd);
    if (writeSize != strlen(buf)) {
        pg_log(PG_WARNING, _("%s: write rewind file failed(%s).\n"), progname, rewindPath);
        fclose(fd);
        fd = NULL;
        return;
    }
    fflush(fd);

    fclose(fd);
    fd = NULL;
}

static void BackupHbaConf()
{
    char path[MAXPGPATH];
    char newpath[MAXPGPATH];
    struct stat st;
    int ret = EOK;
    ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_hba.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    ret = snprintf_s(newpath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_hba.conf.old", pg_data);
    securec_check_ss_c(ret, "\0", "\0");

    if (build_mode == AUTO_BUILD || build_mode == FULL_BUILD || build_mode == INC_BUILD ||
        build_mode == STANDBY_FULL_BUILD) {
        if (stat(path, &st) == 0 && stat(newpath, &st) != 0) {
            copy_file(path, newpath);
        }
    }
}

void ResetBuildInfo()
{
    increment_return_code = BUILD_SUCCESS;
    if (datadir_target != NULL) {
        pg_free(datadir_target);
        datadir_target = NULL;
    }

    if (connstr_source != NULL) {
        pg_free(connstr_source);
        connstr_source = NULL;
    }
    replication_type = RT_WITH_DUMMY_STANDBY;
}

static bool DoIncBuild(uint32 term)
{
    bool buildSuccess = false;
    for (int i = 0; i < INC_BUILD_RETRY_TIMES; ++i) {
        if (conn_str == NULL) {
            buildSuccess = do_incremental_build(term);
        } else {
            buildSuccess = do_incremental_build_xlog();
        }
        if (buildSuccess) {
            break;
        }
        ResetBuildInfo();
    }

    return buildSuccess;
}

static bool DoAutoBuild(uint32 term)
{
    bool buildSuccess = DoIncBuild(term);
    if (!buildSuccess) {
        pg_log(PG_WARNING, _("inc build failed, change to full build.\n"));
        buildSuccess = do_actual_build(term);
    }
    return buildSuccess;
}

static bool DoStandbyBuild(uint32 term)
{
    bool buildSuccess = false;
    if (conn_str == NULL) {
        pg_log(PG_WARNING, "%s: change build mode to CROSS_CLUSTER_INC_BUILD.\n", progname);
        build_mode = CROSS_CLUSTER_INC_BUILD;
        for (int i = 0; i < INC_BUILD_RETRY_TIMES; ++i) {
            buildSuccess = do_incremental_build(term);
            if (buildSuccess) {
                break;
            }
            ResetBuildInfo();
        }
    }

    if (!buildSuccess) {
        if (conn_str != NULL) {
            buildSuccess = do_actual_build(term);
        } else {
            pg_log(PG_WARNING, "%s:cross inc build failed, change build mode to standby full build.\n", progname);
            build_mode = STANDBY_FULL_BUILD;
            buildSuccess = do_actual_build(term);
            if (!buildSuccess) {
                pg_log(PG_WARNING, "%s:standby full build failed, change build mode to cross full build.\n", progname);
                build_mode = CROSS_CLUSTER_FULL_BUILD;
                buildSuccess = do_actual_build(term);
            }
            if (!buildSuccess) {
                pg_log(PG_WARNING,
                    "%s:standby full build failed, change build mode to cross standby full build.\n", progname);
                build_mode = CROSS_CLUSTER_STANDBY_FULL_BUILD;
                buildSuccess = do_actual_build(term);
            }
        }
    }
    return buildSuccess;
}

static bool DoCopySecureFileBuild(uint32 term)
{
    bool buildSuccess = false;
    buildSuccess = CopySecureFilesMain(pg_data, term);
    if (!buildSuccess) {
        pg_log(PG_WARNING, _("%s failed(%s).\n"), BuildModeToString(build_mode), pg_data);
    }
    pg_log(PG_WARNING, _("%s build completed(%s).\n"), BuildModeToString(build_mode), pg_data);
    return buildSuccess;
}

static void do_build(uint32 term)
{
    /* Prevent incremental build in DCF mode */
    if (g_dcfEnabled && ((build_mode == INC_BUILD) || (build_mode == AUTO_BUILD))) {
        pg_log(PG_WARNING, _("Can't run incremental build in DCF mode!\n"));
        exit(1);
    }

    pgpid_t pid = 0;

    pid = get_pgpid();

    /* if the connect info is illegal, exit */
    if ((conn_str != NULL) && (CheckLegalityOfConnInfo() == false)) {
        pg_log(PG_WARNING, "%s: Invalid datanode/coordinator connector: %s.\n", progname, conn_str);
        exit(1);
    }
    if (pid > 0 && build_mode != COPY_SECURE_FILES_BUILD) {
        do_build_stop(pid);
    } else if (pid <= 0 && build_mode != COPY_SECURE_FILES_BUILD) {
        kill_proton_force();
    }

    createRewindFile(pg_data);
    bool buildSuccess = false;
    BackupHbaConf();
    /* Standby DN auto build */
    if ((build_mode == AUTO_BUILD)) {
        buildSuccess = DoAutoBuild(term);
    }
    /* Standby DN full build from Primary DN or CN/DN full build from CN */
    else if ((build_mode == FULL_BUILD) || (build_mode == CROSS_CLUSTER_FULL_BUILD)) {
        buildSuccess = do_actual_build(term);
    }
    /* CN/DN incremental build */
    else if (((build_mode == INC_BUILD) || (build_mode == CROSS_CLUSTER_INC_BUILD))) {
        buildSuccess = DoIncBuild(term);
    }
    /* standby DN full build from standby DN */
    else if (build_mode == STANDBY_FULL_BUILD || build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD) {
        buildSuccess = DoStandbyBuild(term);
    }
    /* disaster cluster copy secure file from remote */
    else if (build_mode == COPY_SECURE_FILES_BUILD) {
        buildSuccess = DoCopySecureFileBuild(term);
    }

    if (!buildSuccess) {
        exit(1);
    }
}

static void do_restore(void)
{
    pgpid_t pid = get_pgpid();
    if (pid != 0 && postmaster_is_alive((pid_t)pid)
#ifndef WIN32
        && IsMyPostmasterPid((pid_t)pid, pg_config)
#endif
    ) {
        pg_log(PG_WARNING, _("terminating restore process due to gaussdb is still alive"));
        return;
    }

    restore_target_dir(pg_data, clear_backup_dir);
    return;
}

static void do_xlog_copy(void)
{
    pgpid_t pid = get_pgpid();
    if (pid != 0 && postmaster_is_alive((pid_t)pid)
#ifndef WIN32
        && IsMyPostmasterPid((pid_t)pid, pg_config)
#endif
    ) {
        pg_log(PG_WARNING, _("terminating xlog copy process due to gaussdb is still alive"));
        return;
    }

    if (xlog_overwrite_opt != NULL) {
        do_overwrite();
    } else {
        pg_log(PG_WARNING, _("no copy mode specified.\n"));
    }

    return;
}

static void read_pgxc_node_name(void)
{
    char config_file[MAXPGPATH] = {0};
    char** optlines = NULL;
    int ret = EOK;

    ret = snprintf_s(config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    config_file[MAXPGPATH - 1] = '\0';
    optlines = readfile(config_file);

    (void)find_guc_optval((const char**)optlines, "pgxc_node_name", pgxc_node_name);

    freefile(optlines);
    optlines = NULL;
}

bool get_conn_exe_sql(const char* sqlCommond)
{
    bool result = true;
    PGconn* conn = NULL;
    conn = get_connectionex();
    if (PQstatus(conn) != CONNECTION_OK) {
        pg_log(PG_WARNING, _("could not connect to the local server: connection failed!\n"));
        PQfinish(conn);
        conn = NULL;
        return false;
    }
    if (exe_sql(conn, sqlCommond) == false) {
        pg_log(PG_WARNING, _("failed to execute sql [%s]!\n"), sqlCommond);
        result = false;
    }

    PQfinish(conn);
    conn = NULL;
    return result;
}

static void do_full_backup(uint32 term)
{
    int ret = 0;
    char cwd[MAXPGPATH];
    char sql_cmd[MAXPGPATH] = {0};
    char tar_cmd[MAXPGPATH] = {0};
    char tar_file_path[MAXPGPATH] = {0};
    char backup_file_path[MAXPGPATH] = {0};
    struct timeval timeOut;
    timeOut.tv_sec = 10;
    timeOut.tv_usec = 0;
    PGconn* conn = NULL;

    if (getcwd(cwd, MAXPGPATH) == NULL) {
        pg_fatal(_("could not identify current directory: %s"), gs_strerror(errno));
        exit(1);
    }
    pg_log(PG_WARNING, _("current workdir is (%s).\n"), cwd);

    pgpid_t pid = get_pgpid();
    if (pid == 0) {
        pg_log(PG_WARNING, _("terminating backup process due to gaussdb is not alive"));
        exit(1);
    }

    read_pgxc_node_name();

    pgpid_t build_pid = getpid();
    if (get_build_pid() == build_pid) {
        pg_log(PG_WARNING, _("backup process is runing"));
        update_obs_build_status("build start");
        return;
    }

    set_build_pid(build_pid);

    conn = get_connection();
    if (PQstatus(conn) != CONNECTION_OK) {
        pg_log(PG_WARNING, _("terminating backup process due to connect gaussdb failed"));
        close_connection();
        exit(1);
    }

    update_obs_build_status("build start");

    ret = snprintf_s(tar_cmd, MAXPGPATH, MAX_PATH_LEN - 1, "cd %s;tar -zcf base.tar.gz obs_backup", pg_data);
    securec_check_ss_c(ret, "\0", "\0");

    ret = snprintf_s(sql_cmd, MAXPGPATH, MAX_PATH_LEN - 1,
        "select * from gs_upload_obs_file('%s', 'base.tar.gz', '%s/base.tar.gz')", slotname, pgxc_node_name);
    securec_check_ss_c(ret, "\0", "\0");

    bool buildSuccess = backup_main(pg_data, term, false);
    if (!buildSuccess) {
        pg_log(PG_WARNING, _("build failed \n"));
        close_connection();
        exit(1);
    }

    if (ExecuteCmd(tar_cmd, timeOut)) {
        pg_log(PG_WARNING, _("execute command [%s] failed \n"), tar_cmd);
        close_connection();
        exit(1);
    }

    if (exe_sql(conn, sql_cmd) == false) {
        pg_log(PG_WARNING, _("failed to upload backup file!\n"));
        close_connection();
        exit(1);
    }

    ret = snprintf_s(tar_file_path, MAXPGPATH, MAX_PATH_LEN - 1, "%s/base.tar.gz", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    delete_target_file(tar_file_path);

    ret = snprintf_s(backup_file_path, MAXPGPATH, MAX_PATH_LEN - 1, "%s/obs_backup", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    if (is_file_exist(backup_file_path)) {
        delete_all_file(backup_file_path, true);
    }

    update_obs_build_status("build done");
    set_build_pid(0);

    return;
}

static char* get_gausshome()
{
    errno_t tnRet = 0;
    char* gausshome = getenv("GAUSSHOME");
    check_input_for_security(gausshome);
    if (gausshome != NULL) {
        gausshome = xstrdup(gausshome);
        char lcdata[MAXPGPATH] = {0};

        if (!is_absolute_path(gausshome)) {
            if (getcwd(lcdata, sizeof(lcdata)) != NULL) {
                int len = strlen(lcdata);
                if (len + g_length_stop_char + strlen(lcdata) < MAXPGPATH) {
                    lcdata[len] = '/';
                    lcdata[len + 1] = '\0';
                    tnRet = strncat_s(lcdata, MAXPGPATH, gausshome, strlen(gausshome));
                    securec_check_c(tnRet, "\0", "\0");
                }
            } else {
                tnRet = strncpy_s(lcdata, MAXPGPATH, gausshome, strlen(gausshome));
                securec_check_c(tnRet, "\0", "\0");
            }
        } else {
            tnRet = strncpy_s(lcdata, MAXPGPATH, gausshome, strlen(gausshome));
            securec_check_c(tnRet, "\0", "\0");
        }

        canonicalize_path(lcdata);
        return xstrdup(lcdata);
    } else {
        return NULL;
    }
}

static void update_local_key_cn()
{
    FILE* keyCn = NULL;
    char path[MAXPGPATH] = {0};
    char temppath[MAXPGPATH] = {0};
    char* gausshome = get_gausshome();
    int ret;

    if (key_cn == NULL || gausshome == NULL) {
        pg_log(PG_WARNING, _("failed get key cn or gausshome \n"));
        return;
    }

    ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/bin/hadr_key_cn", gausshome);
    securec_check_ss_c(ret, "\0", "\0");
    pfree(gausshome);

    ret = snprintf_s(temppath, MAXPGPATH, MAXPGPATH - 1, "%s.temp", path);
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(temppath);
    keyCn = fopen(temppath, "w");
    if (keyCn == NULL) {
        return;
    }
    if (chmod(temppath, S_IRUSR | S_IWUSR) == -1) {
        /* Close file and Nullify the pointer for retry */
        fclose(keyCn);
        keyCn = NULL;
        return;
    }
    if (0 == (fwrite(key_cn, 1, strlen(key_cn), keyCn))) {
        fclose(keyCn);
        keyCn = NULL;
        return;
    }
    fclose(keyCn);

    (void)rename(temppath, path);

}

static void do_full_restore(void)
{
    char sql_cmd[MAXPGPATH] = {0};
    int ret = 0;
    char tar_cmd[MAXPGPATH] = {0};
    char tar_file_path[MAXPGPATH] = {0};
    GaussState state;
    char cwd[MAXPGPATH];
    struct timeval timeOut;
    timeOut.tv_sec = 10;
    timeOut.tv_usec = 0;

    if (getcwd(cwd, MAXPGPATH) == NULL) {
        pg_fatal(_("could not identify current directory: %s"), gs_strerror(errno));
        exit(1);
    }
    pg_log(PG_WARNING, _("current workdir is (%s).\n"), cwd);

    ret = snprintf_s(tar_file_path, MAXPGPATH, MAX_PATH_LEN - 1, "%s/base.tar.gz", pg_data);
    securec_check_ss_c(ret, "\0", "\0");

    pgpid_t pid = get_pgpid();
    if (pid == 0 && is_file_exist(tar_file_path) == false) {
        pg_log(PG_WARNING, _("terminating backup process due to gaussdb is not alive"));
        return;
    }

    set_build_pid(getpid());

    state.mode = STANDBY_MODE;
    state.conn_num = 2;
    state.state = BUILDING_STATE;
    state.sync_stat = false;
    state.build_info.build_mode = FULL_BUILD;
    UpdateDBStateFile(gaussdb_state_file, &state);
    pg_log(PG_WARNING,
        _("set gaussdb state file when full restore in obs mode:"
          "db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(FULL_RESTORE).\n"));

    read_ssl_confval();

    ret = snprintf_s(sql_cmd, MAXPGPATH, MAX_PATH_LEN - 1,
        "select * from gs_download_obs_file('%s', '%s/base.tar.gz', 'base.tar.gz')", slotname, key_cn);
    securec_check_ss_c(ret, "\0", "\0");

    ret = snprintf_s(tar_cmd, MAXPGPATH, MAX_PATH_LEN - 1, "tar -zvxf %s/base.tar.gz -C %s --strip-components 1",
        pg_data, pg_data);
    securec_check_ss_c(ret, "\0", "\0");



    /* download backup from obs */
    if (pid != 0) {
        if (get_conn_exe_sql(sql_cmd) == false) {
            pg_log(PG_WARNING, _("failed to down backup file!\n"));
            return;
        }
    } else if (is_file_exist(tar_file_path) == false) {
        pg_log(PG_WARNING, _("there it no backup file to restore!\n"));
        return;
    }

    shutdown_mode = FAST_MODE;
    sig = SIGINT;
    do_wait = true;
    do_stop(true);

    createRewindFile(pg_data);

    /* delete data/ and  pg_tblspc/, but keep .config */
    delete_datadir(pg_data);
    pg_log(PG_WARNING, _("delete data dir success\n"));

    if (ExecuteCmd(tar_cmd, timeOut)) {
        pg_log(PG_WARNING, _("execute command [%s] failed \n"), tar_cmd);
        return;
    }
    pg_log(PG_WARNING, _("Decompress data success\n"));

    if (needstartafterbuild == true) {
        do_start();
    }

    delete_target_file(tar_file_path);
    update_local_key_cn();
    set_build_pid(0);

    return;
}

static void CheckBuildParameter()
{
    if ((pgha_str != NULL) && (strncmp(pgha_str, "standby", strlen("standby")) != 0) &&
        (strncmp(pgha_str, "cascade_standby", strlen("cascade_standby")) != 0) &&
        (strncmp(pgha_str, "hadr_main_standby", strlen("hadr_main_standby")) != 0)) {
        pg_log(PG_WARNING, _(" the parameter of build is not recognized\n"));
        exit(1);
    }
}

static void find_nested_pgconf(const char** optlines, char* opt_name)
{
    const char* p = NULL;
    int i = 0;
    size_t paramlen = 0;
    paramlen = (size_t)strnlen(opt_name, MAX_PARAM_LEN);
    for (i = 0; optlines[i] != NULL; i++) {
        p = optlines[i];
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (pg_strncasecmp(p, opt_name, paramlen) != 0) {
            continue;
        }
        while (isspace((unsigned char)*p)) {
            p++;
        }

        pg_log(PG_WARNING,
            _("There is nested config file in postgresql.conf: %sWhich is not supported by build. "
              "Please move out the nested config files from %s and comment the 'include' config in postgresql.conf.\n"
              "You can add option '-q' to disable autostart during build and restore the change manually "
              "before starting gaussdb.\n"),
              p, 
              pg_data);
        exit(1);
    }
}

static void check_nested_pgconf(void)
{
    char config_file[MAXPGPATH] = {0};
    char** optlines = NULL;
    int ret = EOK;
    static char* optname[] = {"include ", "include_if_exists "};

    ret = snprintf_s(config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    config_file[MAXPGPATH - 1] = '\0';
    optlines = readfile(config_file);

    if (optlines == NULL) {
        pg_log(PG_WARNING, _("%s cannot be opened.\n"), config_file);
        exit(1);
    }

    for (int i = 0; i < (int)lengthof(optname); i++) {
        find_nested_pgconf((const char**)optlines, optname[i]);
    }

    freefile(optlines);
    optlines = NULL;
}

/*
 * build_mode:
 *		AUTO_BUILD: do gs_rewind first, after failed 3 times, do full
                    build instead.
 *		INC_BUILD: do gs_rewind only.
 */
static bool do_incremental_build(uint32 term)
{
    errno_t tnRet = 0;
    BuildErrorCode status = BUILD_SUCCESS;
    PGresult* res = NULL;
    errno_t errorno = EOK;
    char* sysidentifier = NULL;
    uint32 timeline;
    char connstrSource[MAXPGPATH] = {0};

    CheckBuildParameter();

    check_nested_pgconf();
    set_build_pid(getpid());

    /* 
     * Save connection info from command line or openGauss file.
     */
    get_conninfo(pg_conf_file);

    /* Find a available connection. */
    streamConn = check_and_conn(standby_connect_timeout, standby_recv_timeout, term);
    if (streamConn == NULL) {
        pg_log(PG_WARNING, _("could not connect to server.\n"));
        return false;
    }

    /* Concate connection str to primary host for performing rewind. */
    errorno = sprintf_s(connstrSource,
        sizeof(connstrSource),
        "host=%s port=%s dbname=postgres application_name=gs_rewind connect_timeout=5  rw_timeout=600",
        (streamConn->pghost != NULL) ? streamConn->pghost : streamConn->pghostaddr,
        streamConn->pgport);
    securec_check_ss_c(errorno, "\0", "\0");

    /*
     * Run IDENTIFY_SYSTEM so we can get sys identifier and timeline.
     */
    res = PQexec(streamConn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _("could not identify system: %s"), PQerrorMessage(streamConn));
        PQfinish(streamConn);
        streamConn = NULL;
        PQclear(res);
        return false;
    }
    if (PQntuples(res) != 1 || PQnfields(res) != 4) {
        pg_log(PG_WARNING, _("could not identify system, got %d rows and %d fields\n"), PQntuples(res), PQnfields(res));
        PQfinish(streamConn);
        streamConn = NULL;
        PQclear(res);
        return false;
    }
    sysidentifier = pg_strdup(PQgetvalue(res, 0, 0));
    timeline = atoi(PQgetvalue(res, 0, 1));
    PQclear(res);
    if (streamConn != NULL) {
        PQfinish(streamConn);
        streamConn = NULL;
    }

    /* Pretend to be gs_rewind and perform rewind. */
    progname = "gs_rewind";
    status = gs_increment_build(pg_data, connstrSource, sysidentifier, timeline, term);
    libpqDisconnect();

    if (sysidentifier != NULL) {
        pg_free(sysidentifier);
        sysidentifier = NULL;
    }
    if (bgchild > 0) {
        (void)kill(bgchild, SIGTERM);
    }

#ifdef ENABLE_MOT
    if (status == BUILD_SUCCESS) {
        /* try and fetch the mot checkpoint */
        if (streamConn == NULL) {
            streamConn = check_and_conn(standby_connect_timeout, standby_recv_timeout, term);
            if (streamConn == NULL) {
                pg_log(PG_WARNING, _("fetch MOT checkpoint: could not connect to server.\n"));
                return false;
            }
        }

        pg_log(PG_PROGRESS, "fetching MOT checkpoint\n");

        char* motChkptDir = GetMotCheckpointDir(pg_data);
        FetchMotCheckpoint(motChkptDir ? (const char*)motChkptDir : (const char*)pg_data, streamConn, progname, true);
        if (motChkptDir) {
            free(motChkptDir);
        }

        if (streamConn != NULL) {
            PQfinish(streamConn);
            streamConn = NULL;
        }
    }
#endif

    progname = "gs_ctl";

    if (status == BUILD_SUCCESS) {
        /* cascade standby will use use pgha_opt directly */
        if (pgha_opt == NULL || (strstr(pgha_opt, "cascade_standby") == NULL &&
            strstr(pgha_opt, "hadr_main_standby") == NULL)) {
            /* pg_ctl start -M standby */
            FREE_AND_RESET(pgha_opt);
            pgha_opt = (char*)pg_malloc(sizeof(standbymode_str));
            tnRet = snprintf_s(pgha_opt, sizeof(standbymode_str), sizeof(standbymode_str) - 1, "%s", standbymode_str);
            securec_check_ss_c(tnRet, "\0", "\0");
        }

        if (needstartafterbuild == true) {
            do_start();
        }
        set_build_pid(0);
        return true;
    } else if (status == BUILD_FATAL || status == BUILD_ERROR) {
        pg_log(PG_WARNING, _("inc build failed.\n"));
        return false;
    }

    return false;
}

/*
 * @@GaussDB@@
 * Brief			:
 * Description		:
 * Notes			:
 */
static int find_guc_optval(const char** optlines, const char* optname, char* optval)
{
    int offset = 0;
    int len = 0;
    int lineno = 0;
    char def_optname[64];
    int ret;
    errno_t rc = EOK;

    lineno = find_gucoption(optlines, (const char*)optname, NULL, NULL, &offset, &len);
    if (lineno != INVALID_LINES_IDX) {
        rc = strncpy_s(optval, MAX_VALUE_LEN, optlines[lineno] + offset + 1, (size_t)(Min(len - 1, MAX_VALUE_LEN) - 1));
        securec_check_c(rc, "", "");
        return lineno;
    }

    ret = snprintf_s(def_optname, sizeof(def_optname), sizeof(def_optname) - 1, "#%s", optname);
    securec_check_ss_c(ret, "\0", "\0");
    lineno = find_gucoption(optlines, (const char*)def_optname, NULL, NULL, &offset, &len);
    if (lineno != INVALID_LINES_IDX) {
        rc = strncpy_s(optval, MAX_VALUE_LEN, optlines[lineno] + offset + 1, (size_t)(Min(len - 1, MAX_VALUE_LEN) - 1));
        securec_check_c(rc, "", "");
        return lineno;
    }
    return INVALID_LINES_IDX;
}

/*
 * @@GaussDB@@
 * Brief			:
 * Description		:
 * Notes			:
 */
static void read_ssl_confval(void)
{
    char config_file[MAXPGPATH] = {0};
    char** optlines = NULL;
    int ret = EOK;

    ret = snprintf_s(config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    config_file[MAXPGPATH - 1] = '\0';
    optlines = readfile(config_file);

    (void)find_guc_optval((const char**)optlines, "ssl_cert_file", ssl_cert_file);
    (void)find_guc_optval((const char**)optlines, "ssl_key_file", ssl_key_file);
    (void)find_guc_optval((const char**)optlines, "ssl_ca_file", ssl_ca_file);
    (void)find_guc_optval((const char**)optlines, "ssl_crl_file", ssl_crl_file);

    freefile(optlines);
    optlines = NULL;
}

static void do_overwrite()
{
    char cmd[MAX_PATH_LEN] = {0};
    int nRet = 0;
    
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "gaussdb --single -Q %s postgres", xlog_overwrite_opt);
    securec_check_ss_c(nRet, "\0", "\0");

    if (xlog_overwrite_opt != NULL && strcmp(xlog_overwrite_opt, "copy_from_local") == 0) {
        pg_log(PG_WARNING,
            _("start to overwrite xlog from local pg_xlog to shared storage.\n"));
    } else if (xlog_overwrite_opt != NULL && strcmp(xlog_overwrite_opt, "force_copy_from_local") == 0) {
        pg_log(PG_WARNING,
            _("start to force overwrite xlog local pg_xlog to shared storage.\n"));
    } else if (xlog_overwrite_opt != NULL && strcmp(xlog_overwrite_opt, "copy_from_share") == 0) {
        pg_log(PG_WARNING,
            _("start to overwrite xlog from shared storage to local pg_xlog.\n"));
    } else {
        pg_log(PG_WARNING, _(" unrecognized xlog copy mode \"%s\"\n"), xlog_overwrite_opt);
        FREE_AND_RESET(xlog_overwrite_opt);
        return;
    }
    FREE_AND_RESET(xlog_overwrite_opt);

    nRet = system(cmd);
    if (nRet != 0) {
        pg_log(PG_WARNING,
            _("overwrite xlog: fail.\n"));
    } else {
        pg_log(PG_WARNING,
            _("overwrite xlog: success.\n"));
    }
}

const char *BuildModeToString(BuildMode mode)
{
    switch (mode) {
        case NONE_BUILD:
            return "none build";
            break;
        case AUTO_BUILD:
            return "auto build";
            break;
        case FULL_BUILD:
            return "full build";
            break;
        case INC_BUILD:
            return "inc build";
            break;
        case STANDBY_FULL_BUILD:
            return "standby full build";
            break;
        case CROSS_CLUSTER_FULL_BUILD:
            return "cross cluster full build";
            break;
        case CROSS_CLUSTER_INC_BUILD:
            return "cross cluster inc build";
            break;
        case CROSS_CLUSTER_STANDBY_FULL_BUILD:
            return "cross cluster standby full build";
            break;
        case COPY_SECURE_FILES_BUILD:
            return "copy secure files build";
        default:
            return "unkwon";
            break;
    }
    return "unkwon";
}

static bool do_actual_build(uint32 term)
{
    GaussState state;
    errno_t tnRet = 0;
    char cwd[MAXPGPATH];

    if (getcwd(cwd, MAXPGPATH) == NULL) {
        pg_fatal(_("could not identify current directory: %s"), gs_strerror(errno));
        exit(1);
    }
    pg_log(PG_WARNING, _("current workdir is (%s).\n"), cwd);

    check_nested_pgconf();
    set_build_pid(getpid());

    replconn_num = get_replconn_number(pg_conf_file);

    tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check_c(tnRet, "\0", "\0");

    state.mode = STANDBY_MODE;
    state.conn_num = replconn_num;
    state.state = BUILDING_STATE;
    state.sync_stat = false;
    state.build_info.build_mode = FULL_BUILD;
    UpdateDBStateFile(gaussdb_state_file, &state);
    pg_log(PG_WARNING,
           _("set gaussdb state file when %s build:"
             "db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(FULL_BUILD).\n"),
           BuildModeToString(build_mode));

    read_ssl_confval();

    bool is_from_standby = (build_mode == STANDBY_FULL_BUILD || build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD);
    bool buildSuccess = backup_main(pg_data, term, is_from_standby);
    if (!buildSuccess) {
        pg_log(PG_WARNING, _("%s failed(%s).\n"), BuildModeToString(build_mode), pg_data);
        return buildSuccess;
    }

    pg_log(PG_WARNING, _("%s build completed(%s).\n"), BuildModeToString(build_mode), pg_data);

    /* xlog overwrite */
    if (xlog_overwrite_opt != NULL) {
        do_overwrite();
    }

    /*
     * CN Build CN or CN Build DN cannot auto start,for update pgxc_node by start in restore mode.
     * Standby DN incremental build from Primary DN auto start.
     * If connect string is not empty,CN/DN will be started by caller.
     */
    if (conn_str == NULL || (build_mode == STANDBY_FULL_BUILD && conn_str != NULL)) {
        /* cascade standby will use use pgha_opt directly */
        if (pgha_opt == NULL || (strstr(pgha_opt, "cascade_standby") == NULL &&
            strstr(pgha_opt, "hadr_main_standby") == NULL)) {
            /* pg_ctl start -M standby  */
            FREE_AND_RESET(pgha_opt);
            pgha_opt = (char*)pg_malloc(sizeof(standbymode_str));
            tnRet = snprintf_s(pgha_opt, sizeof(standbymode_str), sizeof(standbymode_str) - 1, "%s", standbymode_str);
            securec_check_ss_c(tnRet, "\0", "\0");
        }
        start_time = time(NULL);
        if (needstartafterbuild == true) {
            if (chdir(cwd) != 0) {
                pg_fatal(_("the current work directory: %s could not be changed"), gs_strerror(errno));
                exit(1);
            }
            do_start();
        }
        set_build_pid(0);
    }

    return true;
}

/*
 * used to CN build DN,get connect string from command line.find xlog position and copy
 * xlog from CN start with this position.
 */
static bool do_incremental_build_xlog()
{
    /* save progress info */
    GaussState state;
    errno_t tnRet = 0;

    set_build_pid(getpid());

    tnRet = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check_c(tnRet, "\0", "\0");

    state.mode = STANDBY_MODE;
    /* can only get connection from one CN */
    state.conn_num = 1;
    state.state = BUILDING_STATE;
    state.sync_stat = false;
    state.build_info.build_mode = INC_BUILD;
    UpdateDBStateFile(gaussdb_state_file, &state);
    pg_log(PG_WARNING,
        _("set gaussdb state file when full build:"
          "db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(FULL_BUILD).\n"));

    /* check ssl mode opened */
    read_ssl_confval();

    bool buildSuccess = false;
    /* only copy xlog from CN to DN */
    buildSuccess = backup_incremental_xlog(pg_data);
    /* set pid to zero for cm can start DN */
    set_build_pid(0);
    pg_log(PG_WARNING, _("build %s(%s).\n"), buildSuccess ? "success" : "failed", pg_data);
    return buildSuccess;
}

static void kill_proton_force(void)
{
/* the max real path length in linux is 4096, adapt this for realpath func */
#define MAX_REALPATH_LEN 4096

    char* retVal = NULL;

    struct timeval timeOut;
    char Lrealpath[MAX_REALPATH_LEN + 1] = {0};
    char cmd[MAX_REALPATH_LEN + 1] = {
        "ps c -eo pid,euid,cmd | grep gaussdb | grep -v grep | awk '{if($2 == curuid && $1!=\"-n\") print "
        "\"/proc/\"$1\"/cwd\"}' curuid=`id -u`| xargs ls -l | awk '{if ($NF==\""};
    char cmdexten[] = {"\")  print $(NF-2)}' | awk -F/ '{print $3 }' | xargs kill -9 >/dev/null 2>&1 "};
    errno_t tnRet = EOK;

    pg_log(PG_WARNING, _("stop failed, killing gaussdb by force ...\n"));

    retVal = realpath(pg_data, Lrealpath);
    if (retVal == NULL) {
        pg_log(PG_WARNING, _(" get realpath failed!\n"));
        return;
    }

    tnRet = strncat_s(cmd, MAX_REALPATH_LEN + 1, Lrealpath, strlen(Lrealpath));
    securec_check_c(tnRet, "\0", "\0");
    tnRet = strncat_s(cmd, MAX_REALPATH_LEN + 1, cmdexten, strlen(cmdexten));
    securec_check_c(tnRet, "\0", "\0");

    timeOut.tv_sec = 10;
    timeOut.tv_usec = 0;

    pg_log(PG_WARNING, _("command [%s] path: [%s] \n"), cmd, Lrealpath);
    if (ExecuteCmd(cmd, timeOut)) {
        pg_log(PG_WARNING, _("execute command failed \n"));
        return;
    }

    pg_log(PG_WARNING, _("server stopped\n"));
    return;
}

static void SigAlarmHandler(int arg)
{
    ;
}

int ExecuteCmd(const char* command, struct timeval timeout)
{
#ifndef WIN32
    pid_t pid = 0;
    pid_t child = 0;
    struct sigaction ign, intact, quitact;
    sigset_t newsigblock, oldsigblock;
    struct itimerval write_timeout;
    errno_t tnRet = 0;

    tnRet = memset_s(&ign, sizeof(struct sigaction), 0, sizeof(struct sigaction));
    securec_check_c(tnRet, "\0", "\0");

    if (command == NULL) {
        return 1;
    }
    /*
     * Ignore SIGINT and SIGQUIT, block SIGCHLD. Remember to save existing
     * signal dispositions.
     */
    ign.sa_handler = SIG_IGN;
    (void)sigemptyset(&ign.sa_mask);
    ign.sa_flags = 0;
    (void)sigaction(SIGINT, &ign, &intact);
    (void)sigaction(SIGQUIT, &ign, &quitact);
    (void)sigemptyset(&newsigblock);
    (void)sigaddset(&newsigblock, SIGCHLD);
    (void)sigprocmask(SIG_BLOCK, &newsigblock, &oldsigblock);
    switch (pid = fork()) {
        case -1: /* error */
            break;
        case 0: /* child */

            /*
             * Restore original signal dispositions and exec the command.
             */
            (void)sigaction(SIGINT, &intact, NULL);
            (void)sigaction(SIGQUIT, &quitact, NULL);
            (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
            execl("/bin/sh", "sh", "-c", command, (char*)0);
            _exit(127);
        default:
            /* wait the child process end ,if timeout then kill the child process force */
            write_timeout.it_value.tv_sec = timeout.tv_sec;
            write_timeout.it_value.tv_usec = timeout.tv_usec;
            write_timeout.it_interval.tv_sec = 0;
            write_timeout.it_interval.tv_usec = 0;
            child = pid;
            (void)setitimer(ITIMER_REAL, &write_timeout, NULL);
            signal(SIGALRM, SigAlarmHandler);
            if (pid != waitpid(pid, NULL, 0)) {
                /* kill child process */
                (void)kill(child, SIGKILL);
                pid = -1;
                /* avoid the zombie process */
                (void)wait(NULL);
            }
            signal(SIGALRM, SIG_IGN);
            break;
    }
    (void)sigaction(SIGINT, &intact, NULL);
    (void)sigaction(SIGQUIT, &quitact, NULL);
    (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
    return (pid == -1 ? -1 : 0);
#endif
    return -1;
}

static pgpid_t get_build_pid(void)
{
    FILE* pidf = NULL;
    long pid = -1;
    pidf = fopen(build_pid_file, "r");
    if (pidf == NULL) {
        /* pid file is not exist */
        if (errno == ENOENT) {
            pid = 0;
        }

        return (pgpid_t)pid;
    }
    if (fscanf_s(pidf, "%ld", &pid) != 1 || pid < 0) {
        pg_log(PG_WARNING,
            _(" fscanf_s build_pid_file \"%s\" failed, could not set the build process pid: %s\n"),
            build_pid_file,
            strerror(errno));
    }
    fclose(pidf);
    pidf = NULL;
    return (pgpid_t)pid;
}

static void set_build_pid(pgpid_t pid)
{
    FILE* pidf = NULL;
    pidf = fopen(build_pid_file, "w");
    if (pidf == NULL) {
        pg_log(PG_WARNING,
            _(" open build pid file \"%s\" failed, could not set the build process pid: %s\n"),
            build_pid_file,
            strerror(errno));
        return;
    }
    pg_log(PG_WARNING, _("fopen build pid file \"%s\" success\n"), build_pid_file);
    if (fprintf(pidf, "%ld", pid) < 0) {
        fclose(pidf);
        pidf = NULL;
        pg_log(PG_WARNING,
            _("fprintf build pid file \"%s\" failed, could not set the build process pid: %s\n"),
            build_pid_file,
            strerror(errno));
        return;
    }
    pg_log(PG_WARNING, _("fprintf build pid file \"%s\" success\n"), build_pid_file);
    if (fsync(fileno(pidf)) != 0) {
        fclose(pidf);
        pidf = NULL;
        pg_log(PG_WARNING,
            _("fsync build pid file \"%s\" failed, could not set the build process pid: %s\n"),
            build_pid_file,
            strerror(errno));
        return;
    }
    pg_log(PG_WARNING, _("fsync build pid file \"%s\" success\n"), build_pid_file);
    fclose(pidf);
    pidf = NULL;
}

int set_hotpatch_cmd_file(void)
{
    FILE* cmdfd = NULL;

    canonicalize_path(g_hotpatch_tmp_cmd_file);
    cmdfd = fopen(g_hotpatch_tmp_cmd_file, "w");
    if (cmdfd == NULL) {
        return HP_ERROR_FILE_OPEN_ERROR;
    }

    if (fwrite(&process_id, sizeof(pid_t), 1, cmdfd) != 1) {
        fclose(cmdfd);
        cmdfd = NULL;
        return HP_ERROR_FILE_WRITE_ERROR;
    }

    if (fwrite(g_hotpatch_name, sizeof(g_hotpatch_name), 1, cmdfd) != 1) {
        fclose(cmdfd);
        cmdfd = NULL;
        return HP_ERROR_FILE_WRITE_ERROR;
    }

    if (fwrite(g_hotpatch_action, sizeof(g_hotpatch_action), 1, cmdfd) != 1) {
        fclose(cmdfd);
        cmdfd = NULL;
        return HP_ERROR_FILE_WRITE_ERROR;
    }

    if (fsync(fileno(cmdfd)) != 0) {
        fclose(cmdfd);
        cmdfd = NULL;
        return HP_ERROR_FILE_SYNC_ERROR;
    }
    fclose(cmdfd);
    cmdfd = NULL;

    if (rename(g_hotpatch_tmp_cmd_file, g_hotpatch_cmd_file) != 0) {
        return HP_ERROR_FILE_RENAME_ERROR;
    }

    return 0;
}

int remove_last_result_file(void)
{
    int ret = 0;
    if (pg_ctl_lock(g_hotpatch_lockfile, &g_hotpatch_lock_fd) == 0) {
        ret = unlink(g_hotpatch_ret_file);
        if ((ret != 0) && (errno != ENOENT)) {
            if (pg_ctl_unlock(g_hotpatch_lock_fd) != 0) {
                return -1;
            }
            return -1;
        }

        if (pg_ctl_unlock(g_hotpatch_lock_fd) != 0) {
            return -1;
        }

        return 0;
    }

    return -1;
}

FILE* hotpatch_wait_return_file(void)
{
    int i;
    int ret = 0;
    FILE* retfd = NULL;
    pid_t tmp_pid = 0;
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000000;

    canonicalize_path(g_hotpatch_ret_file);

    for (i = 0; i < g_hotpatch_wait_counter; i++) {
        retfd = fopen(g_hotpatch_ret_file, "r");
        if (retfd != NULL) {
            ret = fread(&tmp_pid, sizeof(pid_t), 1, retfd);
            if (ret != 1) {
                fclose(retfd);
                retfd = NULL;
                continue;
            }
            if (tmp_pid != process_id) {  // the return file is not mine, continue wait for mine
                fclose(retfd);
                retfd = NULL;
                (void)nanosleep(&ts, NULL);
                continue;
            }
            break;
        }
        (void)nanosleep(&ts, NULL);
    }

    return retfd;
}

void hotpatch_wait_and_get_replyinfo_from_node(bool is_list)
{
    int ret;
    FILE* retfd = NULL;
    char return_string[MAX_LENGTH_RETURN_STRING] = {};

    retfd = hotpatch_wait_return_file();
    if (retfd == NULL) {
        pg_log(PG_WARNING, _("[PATCH-ERROR]: Hotpatch timeout!\n"));
        return;
    }

    ret = fread(return_string, sizeof(return_string), 1, retfd);
    if (ret != 1) {
        // file length less than string max length, ret is not 1, this condition is OK. but if error happens, not OK.
        if (ferror(retfd) != 0) {
            fclose(retfd);
            retfd = NULL;
            pg_log(PG_WARNING, _("[PATCH-ERROR] Read hotpatch result failed!\n"));
            return;
        }
    }

    return_string[MAX_LENGTH_RETURN_STRING - 1] = '\0';
    if (is_list) {
        hotpatch_process_list(
            return_string, sizeof(return_string), pg_data, strlen(pg_data), canonicalize_path, write_stderr);
    } else {
        pg_log(PG_WARNING, _("%s\n"), return_string);
    }

    fclose(retfd);
    retfd = NULL;
    return;
}

void do_hotpatch(void)
{
    pgpid_t pid;
    bool is_list = false;
    errno_t ret = 0;

    pid = get_pgpid();
    if (pid == 0) {
        pg_log(PG_WARNING, _("[PATCH-ERROR] Can not get instance pid\n"));
        return;
    }

    if (hotpatch_check(g_hotpatch_name, g_hotpatch_action, &is_list) != 0) {
        pg_log(PG_WARNING, _("[PATCH-ERROR] Action [%s] is not supported\n"), g_hotpatch_action);
        return;
    }

    if (remove_last_result_file() != 0) {
        pg_log(PG_WARNING, _("[PATCH-ERROR] Can not remove result file. maybe other process patching now\n"));
        return;
    }

    if (is_list) {
        ret = snprintf_s(g_hotpatch_action, sizeof(g_hotpatch_action), sizeof(g_hotpatch_action) - 1, "check");
        securec_check_ss_c(ret, "\0", "\0");
    }

    ret = set_hotpatch_cmd_file();
    if (ret != 0) {
        pg_log(PG_WARNING, _("[PATCH-ERROR] Set hotpatch cmd failed on %d\n"), ret);
        unlink(g_hotpatch_tmp_cmd_file);
        return;
    }

    sig = SIGUSR1;
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _("[PATCH-ERROR] send hotpatch signal failed\n"));
        unlink(g_hotpatch_cmd_file);
        return;
    }

    hotpatch_wait_and_get_replyinfo_from_node(is_list);

    return;
}

void set_hotpatch_name(const char* arg)
{
    int ret;
    int opt_len = strlen(arg);
    ;

    if (opt_len >= g_max_length_path) {
        pg_log(PG_WARNING, _("[PATCH-ERROR]: invalid hotpatch name \"%s\"\n"), arg);
        exit(1);
    }

    ret = snprintf_s(g_hotpatch_name, g_max_length_path, g_max_length_path - 1, "%s", arg);
    securec_check_ss_c(ret, "\0", "\0");

    return;
}

static void Help(int argc, const char** argv)
{
    /* support --help and --version even if invoked as root */
    if (argc > 1) {
        if ((strcmp(argv[1], "-h") == 0) || (strcmp(argv[1], "--help") == 0) || (strcmp(argv[1], "-?") == 0)) {
            do_help();
            exit(0);
        } else if ((strcmp(argv[1], "-V") == 0) || (strcmp(argv[1], "--version")) == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            puts("gs_ctl " DEF_GS_VERSION);
#else
            puts("gs_ctl (openGauss) " PG_VERSION);
#endif
            exit(0);
        }
    }
}

#ifndef ENABLE_MULTIPLE_NODES
/* Write one object with memsize into a file named filename as binary format */
static bool WriteFileInfo(const char *filename, const void *content, size_t memsize)
{
    FILE* sofile = nullptr;
    if ((sofile = fopen(filename, "wb")) == nullptr) {
        pg_log(PG_WARNING, _("fail to open file \"%s\": %s\n"), filename, strerror(errno));
        return false;
    }
    if (fwrite(content, memsize, 1, sofile) != 1) {
        fclose(sofile);
        pg_log(PG_WARNING, _("fail to write file \"%s\": %s.\n"), filename, strerror(errno));
        return false;
    }
    if (fclose(sofile)) {
        pg_log(PG_WARNING, _("fail to close file \"%s\": %s.\n"), filename, strerror(errno));
        return false;
    }
    return true;
}

static bool SendSigUsr1ToPM(void)
{
    pgpid_t pid;
    pid = get_pgpid();
    if (pid == 0) { /* no pid file */
        pg_log(PG_WARNING, _(" PID file \"%s\" does not exist\n"), pid_file);
        pg_log(PG_WARNING, _("Is server running?\n"));
        return false;
    } else if (pid < 0) { /* standalone backend, not postmaster */
        pid = -pid;
        pg_log(PG_WARNING,
            _(" cannot run command; "
              "single-user server is running (PID: %ld)\n"),
            pid);
        return false;
    }
    sig = SIGUSR1;
    if (kill((pid_t)pid, sig) != 0) {
        pg_log(PG_WARNING, _("could not send signal (PID: %ld): %s\n"), pid, strerror(errno));
        return false;
    }
    return true;
}

static bool RemoveFileIfExist(const char *filename)
{
    struct stat buffer;
    if (stat(filename, &buffer)) {
        return true;
    }
    if (unlink(filename)) {
        pg_log(PG_WARNING, _("could not remove file \"%s\": %s\n"), filename, strerror(errno));
        return false;
    }
    return true;
}

/*
 * Check status in filename during timeout and param isTimeout keeps timeout result.
 * Status: -1 demotes failed, 0 demotes success and 1 denotes timeout.
 */
static bool IsRightStatus(const char *filename, bool *isTimeout) {
    int wait_count = 0;
    FILE *sofile = nullptr;
    *isTimeout = false;
    while (wait_count++ < wait_seconds) {
        if ((sofile = fopen(filename, "rb")) != nullptr) {
            int status = -1;
            pg_usleep(1000000); /* sleep 1 sec in case no value written */
            int ret = fread(&status, sizeof(status), 1, sofile);
            if (ret != 1) {
                fclose(sofile);
                pg_log(PG_WARNING, _("Read %s failed!\n"), filename);
                return false;
            }
            fclose(sofile);
            sofile = nullptr;
            if (status == -1) {
                pg_log(PG_WARNING, _("Failed status was read!\n"));
                return false;
            }
            if (status == 1) {
                *isTimeout = true;
                pg_log(PG_WARNING, _("Timeout status was read!\n"));
                return false;
            }
            if (status == 0) {
                return true;
            }
            return false;
        }
        pg_log(PG_PRINT, ".");
        pg_usleep(1000000); /* 1 sec */
    }
    *isTimeout = true;
    return false;
}

void do_add_member(void)
{
    if (!g_dcfEnabled) {
        pg_log(PG_WARNING, _("Can't run this command in non DCF mode!\n"));
        exit(1);
    }

    ServerMode run_mode = get_runmode();
    if (run_mode != PRIMARY_MODE) {
        pg_log(PG_WARNING, _("Can't add a member in a non-primary node\n"));
        exit(1);
    }

    if (new_node_id == 0 || new_node_port == 0 || strlen(new_node_ip) == 0) {
        pg_log(PG_WARNING, _("Were invalid node id, ip and port provided?\n"));
        exit(1);
    }
    NewNodeInfo nodeInfo;
    nodeInfo.stream_id = 1;
    nodeInfo.node_id = new_node_id;
    errno_t ret = strncpy_s(nodeInfo.ip, IP_LEN, new_node_ip, strlen(new_node_ip));
    securec_check_c(ret, "\0", "\0");
    nodeInfo.port = new_node_port;
    nodeInfo.role = 4;
    nodeInfo.wait_timeout_ms = 1000;
    pg_log(PG_WARNING, 
           _("Start adding a new member with node id %u ip %s port %u role %u timeout %u.\n"), 
           nodeInfo.node_id, nodeInfo.ip, nodeInfo.port, nodeInfo.role, nodeInfo.wait_timeout_ms);
    if (!WriteFileInfo(add_member_file, &nodeInfo, sizeof(nodeInfo))) {
        RemoveFileIfExist(add_member_file);
        exit(1);
    }

    if (!SendSigUsr1ToPM()) {
        RemoveFileIfExist(add_member_file);
        pg_log(PG_WARNING, _("Adding member failed!"));
        exit(1);
    } else {
        pg_log(PG_WARNING, _("Please check adding member result by command 'gs_ctl query'"));
        return;
    }
}

void do_remove_member(void)
{
    if (!g_dcfEnabled) {
        pg_log(PG_WARNING, _("Can't run this command in non DCF mode!\n"));
        exit(1);
    }
    ServerMode run_mode = get_runmode();
    if (run_mode != PRIMARY_MODE) {
        pg_log(PG_WARNING, _("Can't remove a member in a non-primary node\n"));
        exit(1);
    }

    if (new_node_id == 0) {
        pg_log(PG_WARNING, _("Was invalid node ID provided?\n"));
        exit(1);
    }

    pg_log(PG_WARNING, _("Start removing a member with node id %u.\n"), new_node_id);
    if (!WriteFileInfo(remove_member_file, &new_node_id, sizeof(new_node_id))) {
        RemoveFileIfExist(remove_member_file);
        exit(1);
    }

    if (!SendSigUsr1ToPM()) {
        RemoveFileIfExist(remove_member_file);
        pg_log(PG_WARNING, _("Removing member failed!"));
        exit(1);
    } else {
        pg_log(PG_WARNING, _("Please check removing member result by command 'gs_ctl query'"));
        return;
    }
}

void do_change_role(void)
{
    if (!g_dcfEnabled) {
        pg_log(PG_WARNING, _("Can't run this command in non DCF mode!\n"));
        exit(1);
    }
    int timeoutRet = 2;
    int ret = 0;
    char context[MAXPGPATH] = {0};
    bool isTimeout = false;
    bool isSuccess = false;
    int roleStatus = -1;
    ServerMode run_mode = get_runmode();
    if (run_mode == PRIMARY_MODE && strcmp(new_role, "passive") != 0) {
        pg_log(PG_WARNING, _("Can't change primary role.\n"));
        exit(1);
    }
    /* The unit of timeout is second while DCF uses ms as timeout.
     * So the maximal timeout should be limited in case of overflow.
     */
    int maxTimeout = 2147483;
    if (wait_seconds > maxTimeout) {
        pg_log(PG_WARNING,
            _("The timeout is out of range and please set it lower than %d.\n"), maxTimeout);
        exit(1);
    }
    pg_log(PG_WARNING, _("Start changing local DCF node role.\n"));

    if (strcmp(new_role, "fo") == 0) {
        roleStatus = 0;
    } else if (strcmp(new_role, "pa") == 0) {
        roleStatus = 1;
    }
    ret = snprintf_s(context, MAXPGPATH, MAXPGPATH - 1, "%d_%d_%d", roleStatus, group, priority);
    securec_check_ss_c(ret, "\0", "\0");
    /* Write role into change_role_file */
    if (!WriteFileInfo(change_role_file, context, strlen(context))) {
        RemoveFileIfExist(change_role_file);
        exit(1);
    }

    /* Write timeout into timeout file */
    if (!WriteFileInfo(timeout_file, &wait_seconds, sizeof(wait_seconds))) {
        RemoveFileIfExist(timeout_file);
        exit(1);
    }
    /*
     * Remove the changerole status file before sending signal
     * in case the last one hasn't been deleted and read a wrong value.
     */
    if (!RemoveFileIfExist(g_changeroleStatusFile)) {
        /* Clear env */
        RemoveFileIfExist(change_role_file);
        RemoveFileIfExist(timeout_file);
        exit(1);
    }
    if (!SendSigUsr1ToPM()) {
        /* Clear env */
        RemoveFileIfExist(change_role_file);
        RemoveFileIfExist(timeout_file);
        exit(1);
    }
    isSuccess = IsRightStatus(g_changeroleStatusFile, &isTimeout);
    RemoveFileIfExist(g_changeroleStatusFile);
    if (isSuccess) {
        pg_log(PG_WARNING, _("Change role success"));
        return;
    } else {
        if (isTimeout) {
            pg_log(PG_WARNING, _("Change role timeout after %d seconds!\n"), wait_seconds);
            exit(timeoutRet);
        }
        pg_log(PG_WARNING, _("Change role failed!\n"));
    }
    exit(1);
}

void do_set_run_mode(void)
{
    pg_log(PG_WARNING, _("Start set run mode.\n"));
    bool isTimeout = false;
    bool isSuccess = false;
    RunModeParam  param;

    if (!g_dcfEnabled) {
        pg_log(PG_WARNING, _("Can't run this command in non-DCF mode!\n"));
        exit(1);
    }
    /* WM_NORMAL = 0, WM_MINORITY = 1. */
    if (xmode != 0 && xmode != 1) {
        pg_log(PG_WARNING, _("Was invalid xmode provided?\n"));
        exit(1);
    }
    /* 
     * Vote num should be greater than 0 when start minority.
     * The replication number of data in minority force start mode 
     * is equal to vote num.
     * This gs_ctl command should be called on every alive DN node 
     * to achieve minority start mode, there xmode is set to 1.
     * When the majority recover, this gs_ctl command should be called 
     * on every alive DN node to switch to normal mode, there xmode is 
     * set to 0.
     */
    if (xmode == 1 && vote_num == 0) {
        pg_log(PG_WARNING, _("Was invalid vote number provided?\n"));
        exit(1);
    }

    pg_log(PG_WARNING, _("DCF going to set run mode with xmode %u vote num %u!\n"), xmode, vote_num);
    param.voteNum = vote_num;
    param.xMode = xmode;
    /* Write set run mode signal file */
    if (!WriteFileInfo(start_minority_file, &param, sizeof(param))) {
        RemoveFileIfExist(start_minority_file);
        exit(1);
    }
    /* Remove the status file in case reading status from the last one */
    if (!RemoveFileIfExist(setrunmode_status_file)) {
        exit(1);
    }
    if (!SendSigUsr1ToPM()) {
        RemoveFileIfExist(start_minority_file);
        exit(1);
    }

    isSuccess = IsRightStatus(setrunmode_status_file, &isTimeout);
    RemoveFileIfExist(setrunmode_status_file);
    if (isSuccess) {
        pg_log(PG_WARNING, _("Set run mode success!\n"));
        return;
    } else {
        if (isTimeout) {
            pg_log(PG_WARNING, _("Setting run-mode timeout after %d seconds!\n"), wait_seconds);
            exit(1);
        }
        pg_log(PG_WARNING, _("Set run mode failed!\n"));
    }
    exit(1);
}
#endif

void check_ipv4_format(const char* ipv4)
{
    size_t len = strlen(ipv4);
    if (len > 15) {
        pg_log(PG_WARNING, _("Invalid ipv4 format: %s!\n"), ipv4);
        exit(1);
    }
    int domain1 = 0;
    int domain2 = 0;
    int domain3 = 0;
    int domain4 = 0;
    int ret = sscanf_s(ipv4, "%d.%d.%d.%d", &domain1, &domain2, &domain3, &domain4);
    if (ret == 4 &&
        domain1 >= 0 && domain1 <= 255 &&
        domain2 >= 0 && domain2<= 255 &&
        domain3 >= 0 && domain3 <= 255 &&
        domain4 >= 0 && domain4 <= 255) {
        return;
    }
    pg_log(PG_WARNING, _("Invalid ipv4 format: %s!\n"), ipv4);
    exit(1);
}

void check_num_input(char* input)
{
    char* tmp = input;
    while(*tmp != '\0') {
        if (!isdigit(*tmp)) {
            pg_log(PG_WARNING, _("Invalid number: %s!\n"), input);
            exit(1);
        }
        tmp++;
    }
}

void SetConfigFilePath()
{
    int ret;
    if (pg_data != NULL) {
        ret = snprintf_s(postopts_file, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.opts", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(backup_file, MAXPGPATH, MAXPGPATH - 1, "%s/backup_label", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(recovery_file, MAXPGPATH, MAXPGPATH - 1, "%s/recovery.conf", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(recovery_done_file, MAXPGPATH, MAXPGPATH - 1, "%s/recovery.done", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(failover_file, MAXPGPATH, MAXPGPATH - 1, "%s/failover", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(switchover_file, MAXPGPATH, MAXPGPATH - 1, "%s/switchover", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(primary_file, MAXPGPATH, MAXPGPATH - 1, "%s/primary", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(standby_file, MAXPGPATH, MAXPGPATH - 1, "%s/standby", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(cascade_standby_file, MAXPGPATH, MAXPGPATH - 1, "%s/cascade_standby", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(pg_ctl_lockfile, MAXPGPATH, MAXPGPATH - 1, "%s/pg_ctl.lock", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(pg_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(build_pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/gs_build.pid", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(gaussdb_state_file, MAXPGPATH, MAXPGPATH - 1, "%s/gaussdb.state", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(postport_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s/postport.lock", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(g_hotpatch_cmd_file, MAXPGPATH, MAXPGPATH - 1, "%s/hotpatch.cmd", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(g_hotpatch_tmp_cmd_file, MAXPGPATH, MAXPGPATH - 1, "%s/hotpatch.cmd.tmp", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(g_hotpatch_ret_file, MAXPGPATH, MAXPGPATH - 1, "%s/hotpatch.ret", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(g_hotpatch_lockfile, MAXPGPATH, MAXPGPATH - 1, "%s/hotpatch.lock", pg_data);
        securec_check_ss_c(ret, "\0", "\0");
        g_dcfEnabled = GetPaxosValue(pg_conf_file);
        if (g_dcfEnabled) {
            ret = snprintf_s(add_member_file, MAXPGPATH, MAXPGPATH - 1, "%s/addmember", pg_data);
            securec_check_ss_c(ret, "\0", "\0");
            ret = snprintf_s(remove_member_file, MAXPGPATH, MAXPGPATH - 1, "%s/removemember", pg_data);
            securec_check_ss_c(ret, "\0", "\0");
            ret = snprintf_s(timeout_file, MAXPGPATH, MAXPGPATH - 1, "%s/timeout", pg_data);
            securec_check_ss_c(ret, "\0", "\0");
            ret = snprintf_s(switchover_status_file, MAXPGPATH, MAXPGPATH - 1, "%s/switchoverstatus", pg_data);
            securec_check_ss_c(ret, "\0", "\0");

            ret = snprintf_s(change_role_file, MAXPGPATH, MAXPGPATH - 1, "%s/changerole", pg_data);
            securec_check_ss_c(ret, "\0", "\0");

            ret = snprintf_s(g_changeroleStatusFile, MAXPGPATH, MAXPGPATH - 1, "%s/changerolestatus", pg_data);
            securec_check_ss_c(ret, "\0", "\0");

            ret = snprintf_s(start_minority_file, MAXPGPATH, MAXPGPATH - 1, "%s/startminority", pg_data);
            securec_check_ss_c(ret, "\0", "\0");

            ret = snprintf_s(setrunmode_status_file, MAXPGPATH, MAXPGPATH - 1, "%s/setrunmodestatus", pg_data);
            securec_check_ss_c(ret, "\0", "\0");
        }
    }
}

int main(int argc, char** argv)
{
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
        {"version", no_argument, NULL, 'V'},
        {"log", required_argument, NULL, 'l'},
        {"mode", required_argument, NULL, 'm'},
        {"pgdata", required_argument, NULL, 'D'},
        {"silent", no_argument, NULL, 's'},
        {"timeout", required_argument, NULL, 't'},
        {"core-files", no_argument, NULL, 'c'},
        {"smode", required_argument, NULL, 'M'},
        {"recvtimeout", required_argument, NULL, 'r'},
        {"connect-string", required_argument, NULL, 'C'},
        {"remove-backup", no_argument, NULL, 1},
        {"action", required_argument, NULL, 'a'},
        {"operation", required_argument, NULL, 'O'},
        {"nodeid", required_argument, NULL, 'u'},
        {"ip", required_argument, NULL, 'i'},
        {"port", required_argument, NULL, 'e'},
        {"dbport", required_argument, NULL, 'E'},
        {"role", required_argument, NULL, 'R'},
        {"votenum", required_argument, NULL, 'v'},
        {"xmode", required_argument, NULL, 'x'},
        {"force", no_argument, NULL, 'f'},
        {"obsmode", no_argument, NULL, 2},
        {"no-fsync", no_argument, NULL, 3},
        {"priority", required_argument, NULL, 4},
        {"keycn", required_argument, NULL, 'k'},
        {"slotname", required_argument, NULL, 'K'},
        {"taskid", required_argument, NULL, 'I'},
        {NULL, 0, NULL, 0}};

    int option_index;
    int c;
    pgpid_t killproc = 0;
    int ret;
    uint32 term = 1;
    errno_t tnRet = 0;
    const char* progname_tmp = NULL;
    process_id = getpid();
    uint32 queryxlogid = -1;
    uint32 queryxlogseg = -1;

#if defined(WIN32) || defined(__CYGWIN__)
    setvbuf(stderr, NULL, _IONBF, 0);
#endif

    check_input_for_security(argv[0]);
    /* Check and free program name */
    progname_tmp = get_progname(argv[0]);
    if (strcmp(progname, progname_tmp) != 0) {
        free(const_cast<char*>(progname_tmp));
        do_help();
        exit(0);
    }
    free(const_cast<char*>(progname_tmp));
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_ctl"));
    start_time = time(NULL);

    /*
     * save argv[0] so do_start() can look for the postmaster if necessary. we
     * don't look for postmaster here because in many cases we won't need it.
     */
    argv0 = argv[0];

    umask(S_IRWXG | S_IRWXO);

    Help(argc, (const char**)argv);
    /*
     * Disallow running as root, to forestall any possible security holes.
     */
#ifndef WIN32
    if (geteuid() == 0) {
        pg_log(PG_WARNING,
            _(" cannot be run as root\n"
              "Please log in (using, e.g., \"su\") as the "
              "(unprivileged) user that will\n"
              "own the server process.\n"));
        goto Error;
    }
#endif

    /*
     * 'Action' can be before or after args so loop over both. Some
     * getopt_long() implementations will reorder argv[] to place all flags
     * first (GNU?), but we don't rely on it. Our /port version doesn't do
     * that.
     */
    optind = 1;

    /* process command-line options */
    while (optind < argc) {
#ifdef ENABLE_MULTIPLE_NODES
        while ((c = getopt_long(argc, argv, "a:b:cD:fl:m:M:N:n:o:p:P:r:sS:t:U:wWZ:C:T:dqL:k:K:I:E:Q:X:", long_options,
            &option_index)) != -1)
#else
        // The node defaults to a datanode
        FREE_AND_RESET(pgxcCommand);
        pgxcCommand = xstrdup("--single_node");
#ifdef ENABLE_PRIVATEGAUSS
#ifndef ENABLE_LITE_MODE
        while ((c = getopt_long(argc, argv, "a:b:cD:e:fi:G:l:m:M:N:n:o:O:p:P:r:R:v:x:sS:t:u:U:wWZ:C:dqL:T:Q:", long_options,
            &option_index)) != -1)
#else
        while ((c = getopt_long(argc, argv, "b:cD:e:fi:G:l:m:M:N:o:O:p:P:r:R:v:x:sS:t:u:U:wWZ:C:dqL:T:Q:", long_options,
            &option_index)) != -1)
#endif
#else
        while ((c = getopt_long(argc, argv, "b:cD:e:fi:G:l:m:M:N:o:O:p:P:r:R:v:x:sS:t:u:U:wWZ:C:dqL:T:Q:", long_options,
            &option_index)) != -1)
#endif
#endif
        {
            switch (c) {
                case 'b': {
                    if (strcmp(optarg, "full") == 0) {
                        build_mode = FULL_BUILD;
                    } else if (strcmp(optarg, "incremental") == 0) {
                        build_mode = INC_BUILD;
                    } else if (strcmp(optarg, "cross_cluster_full") == 0) {
                        build_mode = CROSS_CLUSTER_FULL_BUILD;
                    } else if (strcmp(optarg, "cross_cluster_incremental") == 0) {
                        build_mode = CROSS_CLUSTER_INC_BUILD;
                    } else if (strcmp(optarg, "standby_full") == 0) {
                        build_mode = STANDBY_FULL_BUILD;
                    } else if (strcmp(optarg, "cross_cluster_standby_full") == 0) {
                        build_mode = CROSS_CLUSTER_STANDBY_FULL_BUILD;
                    } else if (strcmp(optarg, "copy_secure_files") == 0) {
                        build_mode = COPY_SECURE_FILES_BUILD;
                    } else if (strcmp(optarg, "copy_upgrade_file") == 0) {
                        build_mode = COPY_SECURE_FILES_BUILD;
                        need_copy_upgrade_file = true;
                    }
                    break;
                }
                case 'D': {
                    char* pgdata_D = NULL;
                    check_input_for_security(optarg);
                    if (strlen(optarg) > MAX_PATH_LEN) {
                        pg_log(PG_WARNING, _("max path length is exceeded\n"));
                        goto Error;
                    }
                    pgdata_D = xstrdup(optarg);
                    canonicalize_path(pgdata_D);
                    setenv("PGDATA", pgdata_D, 1);

                    /*
                     * We could pass PGDATA just in an environment
                     * variable but we do -D too for clearer postmaster
                     * 'ps' display
                     */
                    FREE_AND_RESET(pgdata_opt);
                    pgdata_opt = (char*)pg_malloc(strlen(pgdata_D) + 7);
                    ret = snprintf_s(pgdata_opt, strlen(pgdata_D) + 7, strlen(pgdata_D) + 6, "-D \"%s\" ", pgdata_D);
                    securec_check_ss_c(ret, pgdata_opt, "\0");
                    free(pgdata_D);
                    pgdata_D = NULL;
                    break;
                }
                case 'e': {
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    char* tmp = NULL;
                    uint64 num = strtoul(optarg, &tmp, 10);
                    if (*tmp != '\0' || strlen(optarg) == 0 || num == 0 || num > PG_UINT32_MAX) {
                        pg_log(PG_WARNING, _("unexpected number specified\n"));
                        goto Error;
                    }
                    new_node_port = (uint32)num;
                    break;
                }
                case 'E': {
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    FREE_AND_RESET(dbport);
                    dbport = inc_dbport(optarg);
                    break;
                }
                case 'f': {
                    pg_log(PG_WARNING, _("Performing a Forced Switchover\n"));
                    switch_mode = ExtremelyFast;
                    break;
                }
                case 'i':
                    check_input_for_security(optarg);
                    tnRet = strncpy_s(new_node_ip, IP_LEN, optarg, strlen(optarg));
                    securec_check_c(tnRet, "\0", "\0");
                    check_ipv4_format(new_node_ip);
                    break;
                case 'l':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(log_file);
                    log_file = xstrdup(optarg);
                    break;
                case 'm':
                    check_input_for_security(optarg);
                    set_mode(optarg);
                    break;
                case 'N':
                    if (register_servicename != NULL && strcmp(register_servicename, "gaussdb") != 0) {
                        FREE_AND_RESET(register_servicename);
                    }
                    check_input_for_security(optarg);
                    register_servicename = xstrdup(optarg);
                    break;
                case 'n':
                    check_input_for_security(optarg);
                    set_hotpatch_name(optarg);
                    break;
                case 'o':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(post_opts);
                    post_opts = xstrdup(optarg);
                    break;
                case 'O':
                    check_input_for_security(optarg);
                    set_member_operation(optarg);
                    break;
                case 'p':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(exec_path);
                    exec_path = xstrdup(optarg);
                    break;
                case 'P':
                    FREE_AND_RESET(register_password);
                    register_password = xstrdup(optarg);
                    ret = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                    securec_check_c(ret, optarg, "\0");
                    break;
                case 'Q':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(xlog_overwrite_opt);
                    xlog_overwrite_opt = xstrdup(optarg);
                    break;
                case 'X':
                    check_input_for_security(optarg);
                    {
                        int ret = 0;
                        int opt_len = strlen(optarg);
                        if (opt_len >= MAX_BARRIER_ID_LENGTH) {
                            pg_log(PG_WARNING, _("invalid stop barrier command \n"));
                            goto Error;
                        }
                        ret = memset_s(barrier_id, MAX_STOP_BARRIER_LEN, 0, MAX_STOP_BARRIER_LEN);
                        securec_check(ret, "", "");
                        ret = snprintf_s(barrier_id, MAX_STOP_BARRIER_LEN, MAX_STOP_BARRIER_LEN - 1, "-X %s", optarg);
                        securec_check_ss_c(ret, "\0", "\0");
                        stop_barrier = barrier_id;
                    }
                    break;
                case 'Z':
                    FREE_AND_RESET(pgxcCommand);
#ifndef ENABLE_MULTIPLE_NODES
                    if (strcmp(optarg, "coordinator") == 0 || strcmp(optarg, "datanode") == 0) {
                        pg_log(PG_WARNING,
                            _(" --coordinator and --datanode option are not supported on single node mode\n"));
                        goto Error;
                    }
#endif
                    if (strcmp(optarg, "coordinator") == 0)
                        pgxcCommand = xstrdup("--coordinator");
                    else if (strcmp(optarg, "datanode") == 0)
                        pgxcCommand = xstrdup("--datanode");
                    else if (strcmp(optarg, "restoremode") == 0)
                        pgxcCommand = xstrdup("--restoremode");
                    else if (strcmp(optarg, "single_node") == 0)
                        pgxcCommand = xstrdup("--single_node");
                    break;
                case 's':
                    silent_mode = true;
                    break;
                case 'S':
#if defined(WIN32) || defined(__CYGWIN__)
                    set_starttype(optarg);
#else
                    pg_log(PG_WARNING, _(" -S option not supported on this platform\n"));
                    goto Error;
#endif
                    break;
                case 't':
                    check_input_for_security(optarg);
                    if (atoi(optarg) < 0 || atoi(optarg) > PG_INT32_MAX) {
                        pg_log(PG_WARNING, _("unexpected wait seconds specified\n"));
                        goto Error;
                    }
                    wait_seconds = atoi(optarg);
                    break;

                case 'T': {
                    check_input_for_security(optarg);
                    char* tmp = NULL;
                    term = (uint32)strtoul(optarg, &tmp, 10);
                    if (*tmp != '\0' || strlen(optarg) == 0) {
                        pg_log(PG_WARNING, _("unexpected term specified\n"));
                        goto Error;
                    }
                    break;
                }
                case 'u': {
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    char* tmp = NULL;
                    uint64 num = strtoul(optarg, &tmp, 10);
                    if (*tmp != '\0' || strlen(optarg) == 0 || num == 0 || num > PG_UINT32_MAX) {
                        pg_log(PG_WARNING, _("unexpected number specified\n"));
                        goto Error;
                    }
                    new_node_id = (uint32)num;
                    break;
                }
                case 'v': {
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    char* tmp = NULL;
                    uint64 num = strtoul(optarg, &tmp, 10);
                    if (*tmp != '\0' || strlen(optarg) == 0 || num == 0 || num > PG_UINT32_MAX) {
                        pg_log(PG_WARNING, _("unexpected vote number specified\n"));
                        goto Error;
                    }
                    vote_num = (uint32)num;
                    break;
                }
                case 'x': {
                    check_input_for_security(optarg);
                    if (strcmp(optarg, "normal") == 0) {
                        xmode = 0;
                    } else if (strcmp(optarg, "minority") == 0) {
                        xmode = 1;
                    } else {
                        pg_log(PG_WARNING, _("invalid xmode \"%s\"\n"), optarg);
                        goto Error;
                    }
                    break;
                }
                case 'U':
                    FREE_AND_RESET(register_username);
                    check_env_name_c(optarg);
                    if (strchr(optarg, '\\') != NULL) {
                        register_username = xstrdup(optarg);
                    } else { /* Prepend .\ for local accounts */
                        if (strlen(optarg) + g_length_suffix > NAMEDATALEN) {
                            pg_log(PG_WARNING, _("wrong name length: %zu"), strlen(optarg));
                            goto Error;
                        }
                        register_username = (char*)malloc(strlen(optarg) + g_length_suffix);
                        if (register_username == NULL) {
                            pg_log(PG_WARNING, _("out of memory\n"));
                            goto Error;
                        }
                        ret = strcpy_s(register_username, strlen(optarg) + g_length_suffix, ".\\");
                        securec_check_c(ret, register_username, "\0");
                        ret = strcat_s(register_username, strlen(optarg) + g_length_suffix, optarg);
                        securec_check_c(ret, register_username, "\0");
                    }
                    break;
                case 'w':
                    do_wait = true;
                    wait_set = true;
                    dbgetpassword = -1;
                    break;
                case 'W':
                    do_wait = false;
                    wait_set = true;
                    dbgetpassword = 1;
                    break;
                case 'c':
                    allow_core_files = true;
                    break;
                case 'M': {
                    check_input_for_security(optarg);
                    FREE_AND_RESET(pgha_str);
                    pgha_str = xstrdup(optarg);
                    FREE_AND_RESET(pgha_opt);
                    pgha_opt = (char*)pg_malloc(strlen(pgha_str) + 4);
                    ret = snprintf_s(pgha_opt, strlen(pgha_str) + 4, strlen(pgha_str) + 3, "-M %s", pgha_str);
                    securec_check_ss_c(ret, pgha_opt, "\0");
                } break;
                case 'r':
                    standby_recv_timeout = atoi(optarg);
                    if (standby_recv_timeout < 0) {
                        pg_log(PG_WARNING, _(" invalid recv timeout\n"));
                        goto Error;
                    }
                    break;
                case 'R':
                    check_input_for_security(optarg);
                    if (pg_strcasecmp(optarg, "follower") == 0) {
                        new_role = "fo";
                    } else if (pg_strcasecmp(optarg, "passive") == 0) {
                        new_role = "pa";
                    } else {
                        pg_log(PG_WARNING, _("invalid role \"%s\"\n"), optarg);
                        goto Error;
                    }
                    break;
                case 'C':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(conn_str);
                    conn_str = xstrdup(optarg);
                    break;
                case 'L':
                    check_input_for_security(optarg);
                    if (sscanf_s(optarg, "%08X/%08X", &queryxlogid, &queryxlogseg) != 2) {
                        pg_log(PG_WARNING, _("invalid lsn \"%s\"\n"), optarg);
                        goto Error;
                    }
                    /* Start to find the max lsn from a valid xlogfile */
                    querylsn = queryxlogseg + ((XLogRecPtr)queryxlogid * XLogSegmentsPerXLogId * XLogSegSize);
                    if (XLogRecPtrIsInvalid(querylsn)) {
                        pg_log(PG_WARNING, _("invalid lsn \"%s\"\n"), optarg);
                        goto Error;
                    }
                    islsnquery = true;
                    break;
                case 'd':
                    openDebugLog();
                    break;
                case 'q':
                    needstartafterbuild = false;
                    break;
                case 'a':
                    check_input_for_security(optarg);
                    {
                        int ret = 0;
                        int opt_len = strlen(optarg);
                        ;
                        if (opt_len >= g_max_length_act) {
                            pg_log(PG_WARNING, _("invalid hotpatch command \n"));
                            goto Error;
                        }
                        ret = snprintf_s(g_hotpatch_action, g_max_length_act, g_max_length_act - 1, "%s", optarg);
                        securec_check_ss_c(ret, "\0", "\0");
                    }
                    break;
                case 'k':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(key_cn);
                    key_cn = xstrdup(optarg);
                    break;
                case 'K':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(slotname);
                    slotname = xstrdup(optarg);
                    break;
                case 'I':
                    check_input_for_security(optarg);
                    FREE_AND_RESET(taskid);
                    taskid = xstrdup(optarg);
                    break;
                case 'G': {
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    group = atoi(optarg);
                    if (group < 0 || group > INT_MAX) {
                        pg_log(PG_WARNING, _("unexpected vote number specified\n"));
                        goto Error;
                    }
                    break;
                }
                case 1:
                    clear_backup_dir = true;
                    break;
                case 2:
                    g_is_obsmode = true;
                    break;
                case 3:
                    no_need_fsync = true;
                    break;
                case 4:{
                    check_input_for_security(optarg);
                    check_num_input(optarg);
                    priority = atoi(optarg);
                    if (priority < 0 || priority > INT_MAX) {
                        pg_log(PG_WARNING, _("unexpected vote number specified\n"));
                        goto Error;
                    }
                    break;
                }
                default:
                    /* getopt_long already issued a suitable error message */
                    do_advice();
                    goto Error;
            }
        }

        /* Process an action */
        if (optind < argc) {
            if (ctl_command != NO_COMMAND) {
                pg_log(PG_WARNING, _("too many command-line arguments (first is)\n"));
                do_advice();
                goto Error;
            }

            if (strcmp(argv[optind], "init") == 0 || strcmp(argv[optind], "gs_initdb") == 0)
                ctl_command = INIT_COMMAND;
            else if (strcmp(argv[optind], "start") == 0)
                ctl_command = START_COMMAND;
            else if (strcmp(argv[optind], "restart") == 0)
                ctl_command = RESTART_COMMAND;
            else if (strcmp(argv[optind], "stop") == 0)
                ctl_command = STOP_COMMAND;
            else if (strcmp(argv[optind], "reload") == 0)
                ctl_command = RELOAD_COMMAND;
            else if (strcmp(argv[optind], "status") == 0)
                ctl_command = STATUS_COMMAND;
            else if (strcmp(argv[optind], "notify") == 0)
                ctl_command = NOTIFY_COMMAND;
            else if (strcmp(argv[optind], "query") == 0)
                ctl_command = QUERY_COMMAND;
            else if (strcmp(argv[optind], "querybuild") == 0)
                ctl_command = BUILD_QUERY_COMMAND;
            else if (strcmp(argv[optind], "failover") == 0)
                ctl_command = FAILOVER_COMMAND;
            else if (strcmp(argv[optind], "switchover") == 0)
                ctl_command = SWITCHOVER_COMMAND;
            else if (strcmp(argv[optind], "member") == 0 && member_operation == ADD_OPERATION)
                ctl_command = ADD_MEMBER_COMMAND;
            else if (strcmp(argv[optind], "member") == 0 && member_operation == REMOVE_OPERATION)
                ctl_command = REMOVE_MEMBER_COMMAND;
            else if (strcmp(argv[optind], "member") == 0 && member_operation == CHANGE_OPERATION)
                ctl_command = CHANGE_ROLE_COMMAND;
            else if (strcmp(argv[optind], "changerole") == 0)
                ctl_command = CHANGE_ROLE_COMMAND;
            else if (strcmp(argv[optind], "setrunmode") == 0)
                ctl_command = MINORITY_START_COMMAND;
            else if (strcmp(argv[optind], "kill") == 0) {
                if (argc - optind < 3) {
                    pg_log(PG_WARNING, _(" missing arguments for kill mode\n"));
                    do_advice();
                    goto Error;
                }
                ctl_command = KILL_COMMAND;
                set_sig(argv[++optind]);
                killproc = atol(argv[++optind]);
            }
#if defined(WIN32) || defined(__CYGWIN__)
            else if (strcmp(argv[optind], "register") == 0)
                ctl_command = REGISTER_COMMAND;
            else if (strcmp(argv[optind], "unregister") == 0)
                ctl_command = UNREGISTER_COMMAND;
            else if (strcmp(argv[optind], "runservice") == 0)
                ctl_command = RUN_AS_SERVICE_COMMAND;
#endif
            else if (strcmp(argv[optind], "build") == 0)
                ctl_command = BUILD_COMMAND;
            else if (strcmp(argv[optind], "restore") == 0)
                ctl_command = RESTORE_COMMAND;
            else if (strcmp(argv[optind], "hotpatch") == 0)
                ctl_command = HOTPATCH_COMMAND;
            else if (strcmp(argv[optind], "finishredo") == 0)
                ctl_command = FINISH_REDO_COMMAND;
            else if (strcmp(argv[optind], "copy") == 0)
                ctl_command = COPY_COMMAND;
            else {
                pg_log(PG_WARNING, _(" unrecognized operation mode \"%s\"\n"), argv[optind]);
                do_advice();
                goto Error;
            }
            optind++;
        }
    }

    if (ctl_command == NO_COMMAND) {
        pg_log(PG_WARNING, _(" no operation specified\n"));
        do_advice();
        goto Error;
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* stop command does not need to have Coordinator or Datanode options */
    if ((ctl_command == START_COMMAND || ctl_command == RESTART_COMMAND || ctl_command == BUILD_COMMAND) &&
        (pgxcCommand == NULL)) {
        pg_log(PG_WARNING, _(" Coordinator or Datanode option not specified (-Z)\n"));
        do_advice();
        goto Error;
    }
#endif

    /* Note we put any -D switch into the env var above */
    pg_config = getenv("PGDATA");
    check_input_for_security(pg_config);
    if (pg_config != NULL) {
        pg_config = xstrdup(pg_config);
        char lcdata[MAXPGPATH] = {0};

        if (!is_absolute_path(pg_config)) {
            if (getcwd(lcdata, sizeof(lcdata)) != NULL) {
                int len = strlen(lcdata);
                if (len + g_length_stop_char + strlen(lcdata) < MAXPGPATH) {
                    lcdata[len] = '/';
                    lcdata[len + 1] = '\0';
                    tnRet = strncat_s(lcdata, MAXPGPATH, pg_config, strlen(pg_config));
                    securec_check_c(tnRet, "\0", "\0");
                }
            } else {
                tnRet = strncpy_s(lcdata, MAXPGPATH, pg_config, strlen(pg_config));
                securec_check_c(tnRet, "\0", "\0");
            }
        } else {
            tnRet = strncpy_s(lcdata, MAXPGPATH, pg_config, strlen(pg_config));
            securec_check_c(tnRet, "\0", "\0");
        }

        canonicalize_path(lcdata);
        pg_data = xstrdup(lcdata);
    }

    /* -D might point at config-only directory; if so find the real PGDATA */
    adjust_data_dir();

    /* Complain if -D needed and not provided */
    if (pg_config == NULL && ctl_command != KILL_COMMAND && ctl_command != UNREGISTER_COMMAND) {
        pg_log(PG_WARNING, _(" no database directory specified and environment variable PGDATA unset\n"));
        do_advice();
        goto Error;
    }

    if (!wait_set) {
        switch (ctl_command) {
            case RESTART_COMMAND:
            case START_COMMAND:
            case STOP_COMMAND:
            case SWITCHOVER_COMMAND:
            case FAILOVER_COMMAND:
            case BUILD_COMMAND:
            case NOTIFY_COMMAND:
                do_wait = true;
                break;
            default:
                break;
        }
    }

    if (ctl_command == RELOAD_COMMAND) {
        sig = SIGHUP;
        do_wait = false;
    }

    SetConfigFilePath();

    pg_host = getenv("PGHOST");
    check_input_for_security(pg_host);

    // log output redirect
    init_log(PROG_NAME);

    switch (ctl_command) {
        case INIT_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl init,datadir is %s \n"), pg_data);
            do_init();
            break;
        case STATUS_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl status,datadir is %s \n"), pg_data);
            do_status();
            break;
        case START_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl started,datadir is %s \n"), pg_data);
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_start();
                pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running,start failed !\n"));
            }
            break;
        case RESTART_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl restarted ,datadir is %s \n"), pg_data);
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_restart();
                pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running,restart failed !\n"));
            }
            break;
        case STOP_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl stopped ,datadir is %s \n"), pg_data);
            do_stop(false);
            break;
        case RELOAD_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl reload ,datadir is %s \n"), pg_data);
            do_reload();
            break;
        case FINISH_REDO_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl finish redo ,datadir is %s \n"), pg_data);
            do_finish_redo();
            break;
        case FAILOVER_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl failover ,datadir is %s \n"), pg_data);
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_failover(term);
                pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running,failover failed !\n"));
                goto Error;
            }
            break;
        case SWITCHOVER_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl switchover ,datadir is %s \n"), pg_data);
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_switchover(term);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running,switchover failed !\n"));
                goto Error;
            }
            break;
        case NOTIFY_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl notify ,datadir is %s \n"), pg_data);
            do_notify(term);
            break;
        case QUERY_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl query ,datadir is %s \n"), pg_data);
            do_query();
            break;
        case BUILD_QUERY_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl build query ,datadir is %s \n"), pg_data);
            do_build_query();
            break;
        case KILL_COMMAND:
            pg_log(PG_PROGRESS, _("gs_ctl kill ,datadir is %s \n"), pg_data);
            do_kill(killproc);
            break;
#if defined(WIN32) || defined(__CYGWIN__)
        case REGISTER_COMMAND:
            pgwin32_doRegister();
            break;
        case UNREGISTER_COMMAND:
            pgwin32_doUnregister();
            break;
        case RUN_AS_SERVICE_COMMAND:
            pgwin32_doRunAsService();
            break;
#endif
        case BUILD_COMMAND:
            if (build_mode == COPY_SECURE_FILES_BUILD && (conn_str == NULL || register_username == NULL ||
                register_password == NULL)) {
                pg_log(PG_PROGRESS, _("When copy secure files from remote, need remote host and authentication!\n"));
                goto Error;
            }
            if (conn_str != NULL) {
                if (build_mode == FULL_BUILD || build_mode == CROSS_CLUSTER_FULL_BUILD) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl full build ,datadir is %s,conn_str is \'%s\'\n"),
                        pg_data,
                        conn_str);
                } else if (build_mode == STANDBY_FULL_BUILD || build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl standby full build ,datadir is %s,conn_str is \'%s\'\n"),
                        pg_data,
                        conn_str);
                } else if (build_mode == COPY_SECURE_FILES_BUILD) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl copy secure files from remote build ,datadir is %s,conn_str is \'%s\'\n"),
                        pg_data,
                        conn_str);
                } else {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl incremental build ,datadir is %s,conn_str is \'%s\'\n"),
                        pg_data,
                        conn_str);
                }
            } else {
                if (build_mode == FULL_BUILD || build_mode == CROSS_CLUSTER_FULL_BUILD) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl full build ,datadir is %s\n"),
                        pg_data);
                } else if (build_mode == STANDBY_FULL_BUILD || build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl standby full build ,datadir is %s\n"),
                        pg_data);
                } else if (g_is_obsmode) {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl full backup to obs ,datadir is %s\n"),
                        pg_data);
                } else {
                    pg_log(PG_PROGRESS,
                        _("gs_ctl incremental build ,datadir is %s\n"),
                        pg_data);
                }
            }
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                if (g_is_obsmode) {
                    do_full_backup(term);
                } else {
                    do_build(term);
                }
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running,build failed !\n"));
                goto Error;
            }
            break;
        case HOTPATCH_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_hotpatch();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("[PATCH-ERROR] Another gs_ctl command is still running,hotpatch failed !\n"));
            }
            break;
#ifndef ENABLE_MULTIPLE_NODES
        case ADD_MEMBER_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_add_member();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, add member failed.\n"));
                goto Error;
            }
            break;
        case REMOVE_MEMBER_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_remove_member();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, remove member failed.\n"));
                goto Error;
            }
            break;
        case CHANGE_ROLE_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_change_role();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, change role failed.\n"));
                goto Error;
            }
            break;
        case MINORITY_START_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_set_run_mode();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, minority start failed.\n"));
                goto Error;
            }
            break;
#endif
        case RESTORE_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                if (g_is_obsmode) {
                    do_full_restore();
                } else {
                    do_restore();
                }
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, restore failed.\n"));
            }
        case COPY_COMMAND:
            if (-1 != pg_ctl_lock(pg_ctl_lockfile, &lockfile)) {
                do_xlog_copy();
                (void)pg_ctl_unlock(lockfile);
            } else {
                pg_log(PG_PROGRESS, _("Another gs_ctl command is still running, restore failed.\n"));
            }
        default:
            break;
    }

    free_ctl();
    exit(0);

Error:
    free_ctl();
    exit(1);
}

static void free_ctl()
{
    errno_t tnRet = 0;

    FREE_AND_RESET(log_file);
    /* Clear password related memory to avoid leaks when core. */
    if (register_password != NULL) {
        tnRet = memset_s(register_password, strlen(register_password), 0, strlen(register_password));
        securec_check_c(tnRet, "\0", "\0");
    }
    if (register_servicename != NULL && strcmp(register_servicename, "gaussdb") != 0) {
        FREE_AND_RESET(register_servicename);
    }
    if (pgdata_opt != NULL && strcmp(pgdata_opt, "") != 0) {
        FREE_AND_RESET(pgdata_opt);
    }
    FREE_AND_RESET(register_password);
    FREE_AND_RESET(pgha_str);
    FREE_AND_RESET(pgha_opt);
}
