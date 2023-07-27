/*
 * ------------------------------------------------------------------------
 *
 * pgxc_clean utility
 *
 *	Recovers outstanding 2PC when after crashed nodes or entire cluster
 *  is recovered.
 *
 *  Depending upon how nodes/XC cluster fail, there could be outstanding
 *  2PC transactions which are partly prepared and partly commited/borted.
 *  Such transactions must be commited/aborted to remove them from the
 *  snapshot.
 *
 *  This utility checks if there's such outstanding transactions and
 *  cleans them up.
 *
 * Command syntax
 *
 * pgxc_clean [option ... ] [database] [user]
 *
 * Options are:
 *
 *  -a, --all				cleanup all the database avilable
 *  -d, --dbname=DBNAME		database name to clean up.   Multiple -d option
 *                          can be specified.
 *  -h, --host=HOSTNAME		Coordinator hostname to connect to.
 *  -N, --no-clean			only test.  no cleanup actually.
 *  -p, --port=PORT			Coordinator port number.
 *  -q, --quiet				do not print messages except for error, default.
 *  -s, --status			prints out 2PC status.
 *  -U, --username=USERNAME	database user name
 *  -v, --verbose			same as -s, plus prints result of each cleanup.
 *  -V, --version			prints out the version,
 *  -w, --no-password		never prompt for the password.
 *  -W, --password			prompt for the password,
 *  -e, --exclusive			only clean temp table and remove clean's timeout
 *  -?, --help				prints help message
 *
 * ------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <pwd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include "c.h"
#include "libpq/libpq-fe.h"
#include "pg_config.h"
#include "getopt_long.h"
#include "pgxc_clean.h"
#include "txninfo.h"
#include "port.h"

#include "bin/elog.h"
#include <sys/stat.h>
#include "securec.h"
#include "securec_check.h"

#define PROG_NAME "gs_clean"
#define NAMEDATALEN 64
#define DEFAULT_CONNECT_TIMEOUT "5"
#define FORMATTED_TS_LEN 128
#define FirstNormalTransactionId ((TransactionId)3)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)
char m_formatted_log_time[FORMATTED_TS_LEN];
/* Who I am */
const char* progname;
char* my_nodename;
int my_nodeidx = -1; /* Index in pgxc_clean_node_info */
char* gtm_mode = NULL;
char* gtm_option = NULL;
int  gtm_option_num; /* GTM 0 GTMLITE 1 GTMFREE 2 */

/* Databases to clean */
bool clean_all_databases = false; /* "--all" overrides specific database specification */

/* Databases to clean */
bool commit_all_prepared = false;   /* if global status is prepared ,wether commit all*/
bool rollback_all_prepared = false; /* if global status is prepared ,wether abort all*/

database_names* head_database_names = NULL;
database_names* last_database_names = NULL;
char* rollback_cn_name = NULL;
/* Coordinator to connect to */
char* coordinator_host = NULL;
int coordinator_port = -1;
char* target_node = NULL;

char* connect_timeout = DEFAULT_CONNECT_TIMEOUT;

typedef enum passwd_opt { TRI_DEFAULT, TRI_YES, TRI_NO } passwd_opt;

/* Miscellaneous */
char* username = NULL;
bool version_opt = false;
passwd_opt try_password_opt = TRI_DEFAULT;
bool status_opt = false;
bool no_clean_opt = false;
bool verbose_opt = false;

/* Global variables */
node_info* pgxc_clean_node_info;
int pgxc_clean_node_count;
int pgxc_clean_datanode_count;

database_info* head_database_info;
database_info* last_database_info;
/* @Temp Table. Store temp schema name selected from CN or DN. */
tempschema_info* temp_schema_info = NULL;

/* @Temp Table. Store current active backend pid to decide which temp schema should be dropped. */
activebackend_info* active_backend_info = NULL;

/* PLAN_TABLE: store session_id selected from plan_table_data of one db. */
sessionid_info* session_id_info = NULL;

/* clean temp table only */
static bool clean_temp_schema_only = false;
static bool do_commit_abort_failed = false;

/* clean plan_table_data only */
static bool clean_plan_table_only = false;
static bool no_clean_temp_schema_tbspc = false;

static char* password = NULL;
static char password_prompt[256];

typedef enum {
    LOG,
    DEBUG
} LogLevel;

LogLevel g_log_level = LOG;

#ifdef Assert
#undef Assert
#endif
#ifdef USE_ASSERT_CHECKING
#define Assert(cond) assert(cond)
#else
#define Assert(cond) ((void)0)
#endif

/* Funcs */
static char* transfor_gtm_optoin(const char* option, const char* gtm_free_mode);
static void add_to_database_list(char* dbname);
static void parse_pgxc_clean_options(int argc, char* argv[]);
static void usage(void);
static void showVersion(void);
static PGconn* loginDatabase(char* host, int port, char* user, char* password, char* dbname, const char* progname,
    char* encoding, const char* password_prompt);
static void getMyNodename(PGconn* conn);
static void getMyGtmMode(PGconn* conn);
static void recover2PCForDatabase(database_info* db_info, CleanWorkerInfo* wkinfo);
static void recover2PC(PGconn* conn, txn_info* txn, int num);
static void getDatabaseList(PGconn* conn);
static void getNodeList(PGconn* conn);
static void getPreparedTxnList(PGconn* conn);
static void getTxnInfoOnOtherNodesAll(CleanWorkerInfo* wkinfo);
static void do_commit(PGconn* conn, txn_info* txn, int num);
static void do_abort(PGconn* conn, txn_info* txn, int num);
static void do_commit_abort(PGconn* conn, txn_info* txn, bool is_commit, int num);
static bool setMaintenanceMode(PGconn* conn, bool on);
static bool setEnableParallelDDL(PGconn* conn, bool on);
static char* pg_strdup(const char* string);
static char* formatLogTime(void);
static void getActiveBackendListOnCN(PGconn* conn, int datanode_no = -1);
static void getActiveBackendList(PGconn* conn, const char* query);
static void dropTempSchemas(PGconn* conn, bool missing_ok);
static void dropTempSchema(PGconn* conn, char* nspname, bool missing_ok);

static void getTempSchemaListOnCN(PGconn* conn);
static void getTempSchemaOnOneDN(PGconn* conn, int no);
static int getTempSchemaListOnDN(PGconn* conn);
static void checkAllocMem(void* p);
static void cleanTempSchemaList();
static void cleanBackendList();
static void dropTempNamespace();
static int64 convertCsnStr(const char* csn);
static char* fetchCsnInfo(PGconn* conn, GlobalTransactionId gxid, int node_id, bool* meet_error);

/* For plan_table cleanning */
static void deletePlanTableDataForInactiveSession();
static void getSessionIdListInPlantable(PGconn* conn);
static void deletePlanTableDataS(PGconn* conn, bool missing_ok);
static void addSessionIdInfo(char* sessid);
static void deletePlanTableData(PGconn* conn, char* sessid, bool missing_ok);
static void cleanPlanTableSessidList();

/* For parallel clean */
const int DEFAULT_GS_CLEAN_WORK_NUM = 3;
const int MIN_GS_CLEAN_WORK_NUM = 1;
const int MAX_GS_CLEAN_WORK_NUM = 10;
int g_gs_clean_worker_num = DEFAULT_GS_CLEAN_WORK_NUM;
const int GS_CLEAN_CHECK_SECONDS = 15; /* check per seconds */

volatile int g_receive_chld_exit_sig = false;

volatile GSCleanGlobalThreads g_ThreadsData;
volatile GSCleanGlobalThreads* g_Threads = &g_ThreadsData;
pthread_mutex_t g_init_worker_lock;
pthread_mutex_t g_thread_lock;
pthread_t g_main_threadid;

static void* gs_clean_main(void* arg);
static void gs_clean_start(void);
static void gs_clean_end(void);
void gs_clean_work(void);
static bool check_gs_clean_done(bool need_lock);
static void cleanup_gs_clean_worker(void* arg);

static void cleanup_gs_clean_worker(void* arg)
{
    bool send_signal = false;
    if (pthread_mutex_lock(&g_thread_lock) != 0) {
        write_stderr("fail to lock g_thread_lock.\n");
        exit(1);
    }
    CleanWorkerInfo* wkinfo = (CleanWorkerInfo*)arg;
    /* set worker status and clean connection */
    wkinfo->worker_status = WRKR_TERMINATED;
    PQfinish(wkinfo->conn);
    wkinfo->conn = NULL;
    send_signal = check_gs_clean_done(false);
    if (pthread_mutex_unlock(&g_thread_lock) != 0) {
        write_stderr("fail to release g_thread_lock.\n");
        exit(1);
    }
    if (send_signal) {
        if (pthread_kill(g_main_threadid, SIGCHLD) != 0) {
            write_stderr("fail to kill thread %ld.\n", g_main_threadid);
        }
    }
}

static bool GetTxnInfo(CleanWorkerInfo* wkinfo)
{
    int num = wkinfo->work_job;
    char* database_name = (char*)(clean_all_databases ? "postgres" : head_database_names->database_name);

    wkinfo->conn = loginDatabase(
        coordinator_host, coordinator_port, username, password, database_name, progname, "auto", password_prompt);
    if (wkinfo->conn == NULL) {
        write_stderr("job%d: %s Could not connect to the database %s.\n", num, formatLogTime(), database_name);
        return false;
    }
    if (!setMaintenanceMode(wkinfo->conn, true)) {
        /* Cannot set */
        write_stderr(
            "job%d: %s fail to set xc_maintenance_mode to the database %s.\n", num, formatLogTime(), database_name);
        PQfinish(wkinfo->conn);
        wkinfo->conn = NULL;
        return false;
    }
    if (verbose_opt)
        write_stderr("job%d: %s %s: connected to the database \"%s\"\n", num, formatLogTime(), progname, database_name);

    /*
     * Check status of each prepared transaction. To do this, look into
     * nodes where the transaction is not recorded as "prepared".
     * Possible status are unknown (prepare has not been issued), committed or
     * aborted, or running.
     */
    getTxnInfoOnOtherNodesAll(wkinfo);
    if (verbose_opt) {
        /* Print all the prepared transaction list */
        database_info* cur_db = NULL;

        write_stderr("job%d: %s %s: 2PC transaction list.\n", num, formatLogTime(), progname);
        for (cur_db = head_database_info; cur_db != NULL; cur_db = cur_db->next) {
            txn_info* txn = NULL;

            write_stderr("job%d: %s Database: \"%s\":\n", num, formatLogTime(), cur_db->database_name);

            for (txn = cur_db->all_head_txn_info[num]; txn != NULL; txn = txn->next) {
                write_stderr("job%d: %s	 localxid: %lu, gid: \"%s\", owner: %s\n",
                    num,
                    formatLogTime(),
                    txn->localxid,
                    txn->gid,
                    txn->owner);
                for (int ii = 0; ii < pgxc_clean_node_count; ii++) {
                    write_stderr("job%d: %s		 node: %s, status: %s\n",
                        num,
                        formatLogTime(),
                        pgxc_clean_node_info[ii].node_name,
                        str_txn_stat(txn->txn_stat[ii]));
                }
            }
        }
    }

    /*
     * Then disconnect from the database.
     * I need to login to specified databases which 2PC is issued for.	Again, we assume
     * that all the prepare is issued against the same database in each node, which
     * current Coordinator does and there seems to be no way to violate this assumption.
     */
    if (verbose_opt) {
        write_stderr("job%d: %s %s: disconnecting\n", num, formatLogTime(), progname);
    }
    PQfinish(wkinfo->conn);
    wkinfo->conn = NULL;

    /*
     * If --no-clean option is specified, we exit here.
     */
    if (no_clean_opt) {
        write_stderr("job%d: %s --no-clean opt is specified. Exiting.\n", num, formatLogTime());
        return false;
    } else {
        return true;
    }
}

static void* gs_clean_main(void* arg)
{
    CleanWorkerInfo* wkinfo = (CleanWorkerInfo*)arg;
    bool success_get_txn = false;

    if (pthread_detach(pthread_self()) != 0) {
        write_stderr("pthread detach failed.\n");
        exit(1);
    }

    pthread_cleanup_push(cleanup_gs_clean_worker, wkinfo);

    if (pthread_mutex_lock(&g_init_worker_lock) != 0) {
        write_stderr("fail to lock init_worker_lock.\n");
        exit(1);
    }
    wkinfo->worker_status = WRKR_WORKING;
    if (pthread_mutex_unlock(&g_init_worker_lock) != 0) {
        write_stderr("fail to release init_worker_lock.\n");
        exit(1);
    }

    /* do actural work here */
    success_get_txn = GetTxnInfo(wkinfo);
    Assert(wkinfo->work_job >= 0);
    Assert(wkinfo->work_job < g_gs_clean_worker_num);

    /* if get Txn info success , we do clean job */
    if (success_get_txn) {
        /*
         * Recover 2PC for specified databases
         */
        if (clean_all_databases) {
            database_info* cur_database_info = NULL;

            for (cur_database_info = head_database_info; cur_database_info != NULL;
                 cur_database_info = cur_database_info->next) {
                recover2PCForDatabase(cur_database_info, wkinfo);
            }
        } else {
            database_info* cur_database_info = NULL;
            database_names* cur_database_name = NULL;

            for (cur_database_name = head_database_names; cur_database_name != NULL;
                 cur_database_name = cur_database_name->next) {
                cur_database_info = find_database_info(cur_database_name->database_name);
                if (cur_database_info != NULL) {
                    recover2PCForDatabase(cur_database_info, wkinfo);
                }
            }
        }
    }
    pthread_cleanup_pop(1);
    return ((void*)0);
}

static void gs_clean_start(void)
{
    int i = 0;
    int err = 0;
    errno_t rc = EOK;
    /* block sub threads until all start works done. */
    if (pthread_mutex_lock(&g_init_worker_lock) != 0) {
        write_stderr("fail to lock init_worker_lock.\n");
        exit(1);
    }
    /* position 0 has been arranged to master. */
    for (i = 1; i <= g_gs_clean_worker_num; i++) {
        CleanWorkerInfo* wkinfo = NULL;

        wkinfo = &(g_Threads->worker_info[i]);
        rc = memset_s(wkinfo, sizeof(CleanWorkerInfo), 0, sizeof(CleanWorkerInfo));
        securec_check_c(rc, "\0", "\0");
        /* construct wkinfo */
        wkinfo->is_main_thread = false;
        wkinfo->worker_status = WRKR_IDLE;
        /* ignore main thread */
        wkinfo->work_job = i - 1;
        err = pthread_create(&wkinfo->thr_id, NULL, gs_clean_main, wkinfo);
        if (err != 0) {
            (void)pthread_mutex_unlock(&g_init_worker_lock);
            write_stderr("%s: failed to create a new worker thread: %d\n", formatLogTime(), err);
            exit(1);
        }
        g_Threads->num_workers++;
    }

    Assert(g_Threads->num_workers - 1 == g_gs_clean_worker_num);
    write_stderr("%s The active workers is : %d\n", formatLogTime(), g_Threads->num_workers - 1);
    if (pthread_mutex_unlock(&g_init_worker_lock) != 0) {
        write_stderr("fail to release init_worker_lock.\n");
        exit(1);
    }
}

static void gs_clean_end(void)
{
    /* Only left main thread */
    g_Threads->num_workers = 1;
}

void gs_clean_work(void)
{
    gs_clean_start();
    for (;;) {
        if (g_receive_chld_exit_sig) {
            write_stderr("%s recive child exit signal, all child workers exit.\n", formatLogTime());
        }
        if (check_gs_clean_done(true)) {
            write_stderr("%s all 2pc clean workers have finished jobs.\n", formatLogTime());
            break;
        }

        sleep(GS_CLEAN_CHECK_SECONDS);
    }
    gs_clean_end();
}

/* hold lock and check if all jobs have done */
static bool check_gs_clean_done(bool need_lock)
{
    int i = 0;
    int count = 0;
    CleanWorkerInfo* wkinfo = NULL;

    if (need_lock && (pthread_mutex_lock(&g_thread_lock) != 0)) {
        write_stderr("fail to lock g_thread_lock.\n");
        exit(1);
    }
    for (i = 0; i < g_Threads->num_workers; i++) {
        wkinfo = &(g_Threads->worker_info[i]);
        if (wkinfo->is_main_thread == false) {
            if (wkinfo->worker_status != WRKR_TERMINATED)
                count++;
        }
    }
    if (need_lock && (pthread_mutex_unlock(&g_thread_lock) != 0)) {
        write_stderr("fail to release g_thread_lock.\n");
        exit(1);
    }

    if (count == 0) {
        return true;
    } else {
        return false;
    }
}

static void initGSCleanWorker(void)
{
    int rc;
    CleanWorkerInfo* maininfo = NULL;
    g_Threads->worker_info = (CleanWorkerInfo*)calloc((g_gs_clean_worker_num + 1), sizeof(CleanWorkerInfo));
    if (g_Threads->worker_info == NULL) {
        write_stderr("out of memory, malloc failed.");
        exit(1);
    }
    g_Threads->num_workers = 1;
    /* set main thread worker info */
    maininfo = &(g_Threads->worker_info[0]);
    rc = memset_s(maininfo, sizeof(CleanWorkerInfo), 0, sizeof(CleanWorkerInfo));
    securec_check_c(rc, "\0", "\0");
    maininfo->worker_status = WRKR_WORKING;
    maininfo->is_main_thread = true;
    g_main_threadid = pthread_self();
    if (pthread_mutex_init(&g_init_worker_lock, NULL) != 0) {
        write_stderr("fail to init g_init_worker_lock.\n");
        exit(1);
    }
    if (pthread_mutex_init(&g_thread_lock, NULL) != 0) {
        write_stderr("fail to init g_thread_lock.\n");
        exit(1);
    }
}

static void GSCleanChildWorkerExit(int sig)
{
    g_receive_chld_exit_sig = true;
    return;
}

/*
 * Connection to the Coordinator
 */
PGconn* coord_conn;

static inline int strtolSafe(const char* nptr, int base)
{
    char* tmp = NULL;
    int res = (int)strtol(nptr, &tmp, base);
    if (unlikely(*tmp != '\0')) {
        write_stderr("%s %s: strtol failed, input str:%s\n", formatLogTime(), progname, nptr);
        exit(1);
    }
    return res;
}

static inline int64 strtollSafe(const char* nptr, int base)
{
    char* tmp = NULL;
    int64 res = strtoll(nptr, &tmp, base);
    if (unlikely(*tmp != '\0')) {
        write_stderr("%s %s: strtoll failed, input str:%s\n", formatLogTime(), progname, nptr);
        exit(1);
    }
    return res;
}

static inline char* strrchrSafe(char* str, int c)
{
    char* res = strrchr(str, c);
    if (unlikely(res == NULL)) {
        write_stderr("%s %s: strrchr failed, can't find '%c' in '%s'\n",
            formatLogTime(), progname, (char)c, str);
        exit(1);
    }
    return res;
}


/*
 *
 * Main
 *
 */
int main(int argc, char* argv[])
{
    errno_t errorno = EOK;
    int rc;
    bool twoPCExists = true;
    if (signal(SIGCHLD, GSCleanChildWorkerExit) == SIG_ERR) {
        write_stderr("Register signal 'SIFCHLD' failed.\n");
        exit(1);
    }
    /* Should setup pglocale when it is supported by XC core */

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            showVersion();
            exit(0);
        }
    }
    parse_pgxc_clean_options(argc, argv);

    /*
     * Check missing arguments
     */
    if (clean_all_databases == false && head_database_names == NULL) {
        write_stderr("%s %s: you must specify -a or -d option.\n", formatLogTime(), progname);
        exit(1);
    }

    /* log output redirect */
    init_log(PROG_NAME);

    if (coordinator_host == NULL) {
        char* pghost = NULL;
        if ((pghost = getenv("PGHOST")) == NULL || strlen(pghost) > MAXPGPATH) {
            coordinator_host = "";
        } else {
            check_env_value_c(pghost);
            coordinator_host = pg_strdup(pghost);
        }
    }

    if (coordinator_port == -1) {
        /* Default Coordinator port */
        char* pgport = NULL;
        if ((pgport = getenv("PGPORT")) == NULL) {
            coordinator_port = DEF_PGPORT; /* pg_config.h */
        } else {
            check_env_value_c(pgport);
            coordinator_port = strtolSafe(pgport, 10);
        }
    }
    if (username == NULL) {
        errorno = strcpy_s(password_prompt, sizeof(password_prompt), "Password: ");
        securec_check_c(errorno, "\0", "\0");
    } else {
        rc = snprintf_s(
            password_prompt, sizeof(password_prompt), sizeof(password_prompt) - 1, "Password for user %s: ", username);
        securec_check_ss_c(rc, "\0", "\0");
    }
    if (try_password_opt == TRI_YES && NULL == password)
        password = simple_prompt(password_prompt, 100, false);

    /* Tweak options --> should be improved in the next releases */
    if (status_opt) {
        verbose_opt = true;
    }

    if (verbose_opt) {
        /* Print environments */
        write_stderr("%s %s (%s): Cleanup outstanding 2PCs.\n", formatLogTime(), progname, PG_VERSION);
        /* Target databaess */
        write_stderr("%s Target databases:", formatLogTime());
        if (clean_all_databases) {
            write_stderr("(ALL)\n");
        } else {
            database_names* cur_name = NULL;

            for (cur_name = head_database_names; cur_name != NULL; cur_name = cur_name->next)
                write_stderr("%s %s", cur_name->database_name, formatLogTime());
            write_stderr("\n");
        }
        /* Username to use */
        write_stderr("%s Username: %s\n", formatLogTime(), username != NULL ? username : "default");
        /* Status opt */
        write_stderr("%s Status opt: %s\n", formatLogTime(), status_opt ? "on" : "off");
        /* No-dlean opt */
        write_stderr("%s no-clean: %s\n", formatLogTime(), no_clean_opt ? "on" : "off");
        /* rollback_cn_name */
        write_stderr("%s rollback-cnname: %s\n", formatLogTime(), rollback_cn_name != NULL ? rollback_cn_name : "off");
    }

    /* init */
    initGSCleanWorker();

    /* Connect to XC server */
    if (verbose_opt) {
        write_stderr("%s %s: connecting to database \"%s\", host: \"%s\", port: %d\n",
            formatLogTime(),
            progname,
            clean_all_databases ? "postgres" : head_database_names->database_name,
            coordinator_host,
            coordinator_port);
    }
    coord_conn = loginDatabase(coordinator_host,
        coordinator_port,
        username,
        password,
        (char*)(clean_all_databases ? "postgres" : head_database_names->database_name),
        progname,
        "auto",
        password_prompt);
    if (NULL == coord_conn) {
        write_stderr("%s %s: connect failed\n", formatLogTime(), progname);
        exit(1);
    }
    if (verbose_opt) {
        write_stderr("%s %s: connected successfully\n", formatLogTime(), progname);
    }

    /* Set maintenance mode for skipping xact sync wait */
    if (!setMaintenanceMode(coord_conn, true)) {
        /* Cannot recover */
        write_stderr("%s can not set maintenance mode .\n", formatLogTime());
        PQfinish(coord_conn);
        exit(1);
    }

    /*
     * Get my nodename (connected Coordinator) and gtm_mode
     */
    getMyNodename(coord_conn);
    getMyGtmMode(coord_conn);
    if (verbose_opt) {
        write_stderr("%s %s: Connected to the node \"%s\", current gtm_option: %s, option_num: %d.\n",
            formatLogTime(),
            progname,
            my_nodename,
            transfor_gtm_optoin(gtm_option, gtm_mode),
            gtm_option_num);
    }

    /*
     * Get available databases
     *
     * pgxc_clean assumes that all the database are available from the connecting Coordinator.
     * Some (expert) DBA can create a database local to subset of the node by EXECUTE DIRECT.
     * In this case, DBA may have to clean outstanding 2PC transactions manually or clean
     * 2PC transactions by connecting pgxc_clean to different Coordinators.
     *
     * If such node-subset database is found to be used widely, pgxc_clean may need
     * an extension to deal with this case.
     */
    if (clean_all_databases)
        getDatabaseList(coord_conn);
    if (verbose_opt) {
        database_info* cur_database = NULL;

        write_stderr("%s %s: Databases visible from the node \"%s\": ", formatLogTime(), progname, my_nodename);

        if (head_database_info != NULL) {
            for (cur_database = head_database_info; cur_database != NULL; cur_database = cur_database->next) {
                write_stderr("%s  \"%s\"", formatLogTime(), cur_database->database_name);
            }
            write_stderr("\n");
        }
    }

    /*
     * Get list of Nodes
     *
     * As in the case of database, we clean transactions in visible nodes from the
     * connecting Coordinator. DBA can also setup different node configuration
     * at different Coordinators. In this case, DBA should be careful to choose
     * appropriate Coordinator to clean up transactions.
     */
    getNodeList(coord_conn);
    if (verbose_opt) {
        int ii;

        write_stderr("%s %s: Node list visible from the node \"%s\"\n", formatLogTime(), progname, my_nodename);

        for (ii = 0; ii < pgxc_clean_node_count; ii++) {
            write_stderr("%s Name: %s, host: %s, port: %d/%d, type: %s\n",
                formatLogTime(),
                pgxc_clean_node_info[ii].node_name,
                pgxc_clean_node_info[ii].host,
                pgxc_clean_node_info[ii].port,
                pgxc_clean_node_info[ii].port + 1,
                pgxc_clean_node_info[ii].type == NODE_TYPE_COORD ? "coordinator" : "datanode");
        }
    }

    /*
     * Only clean temp schema if clean_temp_schema_only == true,
     * and then exit
     */
    if (clean_temp_schema_only) {
        PQfinish(coord_conn);
        coord_conn = NULL;
        if (!no_clean_opt) {
            dropTempNamespace();
        }
        exit(0);
    }

    /*
     * Only clean plan_table if clean_plan_table_only == true,
     * and then exit
     */
    if (clean_plan_table_only) {
        PQfinish(coord_conn);
        coord_conn = NULL;
        if (!no_clean_opt) {
            deletePlanTableDataForInactiveSession();
        }
        exit(0);
    }

    /*
     * Get list of prepared statement
     */
    getPreparedTxnList(coord_conn);

    /*
     * Check if there're any 2PC candidate to recover
     */
    if (!check2PCExists()) {
        write_stderr("%s %s: There's no prepared 2PC in this cluster.\n", formatLogTime(), progname);
        twoPCExists = false;
    }

    if (twoPCExists) {
        /*
         * Then disconnect from the database.
         * I need to login to specified databases which 2PC is issued for.  Again, we assume
         * that all the prepare is issued against the same database in each node, which
         * current Coordinator does and there seems to be no way to violate this assumption.
         */
        if (verbose_opt) {
            write_stderr("%s %s: disconnecting\n", formatLogTime(), progname);
        }
        PQfinish(coord_conn);
        coord_conn = NULL;

        /* parallel gs clean work */
        gs_clean_work();

        /*
         * If --no-clean option is specified, we exit here.
         */
        if (no_clean_opt) {
            write_stderr("%s --no-clean opt is specified. Exiting.\n", formatLogTime());
            exit(0);
        }
    }

    PQfinish(coord_conn);
    coord_conn = NULL;

    if (!no_clean_opt && !no_clean_temp_schema_tbspc) {
        /* Drop non-active temp schema of each database. */
        dropTempNamespace();

        /* Delete non-active session data in 'plan_table_data' for each database. */
        deletePlanTableDataForInactiveSession();
    }

    /* clear sensitive info when exit */
    if (password != NULL) {
        rc = memset_s(password, strlen(password), 0, strlen(password));
        securec_check_c(rc, "\0", "\0");
        free(password);
        password = NULL;
    }

    if (do_commit_abort_failed)
        exit(1);
    else
        exit(0);
}

/*
 * @Description: check if 'plan_table_data' exist.
 * @in: void
 * @return: if 'plan_table_data' does not exist, return true. Then return false.
 */
static bool tableIsNotExist(PGconn* conn)
{
    PGresult* res = NULL;
    errno_t rc = EOK;
#define stmt_len 60

    /* make up SQL delete stmt */
    char* stmt_select_pt_from_pgclass = (char*)malloc(stmt_len + 1);
    checkAllocMem(stmt_select_pt_from_pgclass);

    rc = snprintf_s(stmt_select_pt_from_pgclass,
        stmt_len + 1,
        stmt_len,
        "SELECT OID FROM pg_class where relname='plan_table_data';");
    securec_check_ss_c(rc, "\0", "\0");

    res = PQexec(conn, stmt_select_pt_from_pgclass);

    /* 'plan_table_data' does not exist, return true. */
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        free(stmt_select_pt_from_pgclass);
        PQclear(res);
        return true;
    }
    /* 'plan_table_data' already exist, return false. */
    free(stmt_select_pt_from_pgclass);
    PQclear(res);
    return false;
}

/*
 * @Description: Main entrance for delete inactive backend data in plan_table_data of current CN for each database.
 * @in: void
 * @return: void
 */
static void deletePlanTableDataForInactiveSession()
{
    if (head_database_info == NULL)
        return;

    database_info* cur_database = NULL;

    if (verbose_opt)
        write_stderr("%s start deleting from plan_table_data\n", formatLogTime());

    for (cur_database = head_database_info; cur_database != NULL; cur_database = cur_database->next) {
        coord_conn = loginDatabase(coordinator_host,
            coordinator_port,
            username,
            password,
            cur_database->database_name,
            progname,
            "auto",
            password_prompt);

        if (coord_conn == NULL) {
            write_stderr("%s could not connect to the database %s.\n", formatLogTime(), cur_database->database_name);
            return;
        }

        if (tableIsNotExist(coord_conn))
            return;

        if (g_log_level == DEBUG)
            write_stderr("%s %s: ready to delete from plan_table_data for database \"%s\"\n",
                formatLogTime(),
                progname,
                cur_database->database_name);

        if (!setMaintenanceMode(coord_conn, false)) {
            /* Cannot recover */
            write_stderr("%s can not set maintenance mode.\n", formatLogTime());
            PQfinish(coord_conn);
            coord_conn = NULL;
            return;
        }

        if (g_log_level == DEBUG)
            write_stderr(
                "%s %s: connected to the database \"%s\"\n", formatLogTime(), progname, cur_database->database_name);

        (void)getSessionIdListInPlantable(coord_conn);
        /* we only can get the backend pid of the current cn.*/
        (void)getActiveBackendListOnCN(coord_conn);
        (void)deletePlanTableDataS(coord_conn, false);
        (void)cleanBackendList();

        if (g_log_level == DEBUG)
            write_stderr("%s %s: delete from plan_table_data for database \"%s\" finished\n",
                formatLogTime(),
                progname,
                cur_database->database_name);
        PQfinish(coord_conn);
        coord_conn = NULL;
    }

    if (verbose_opt)
        write_stderr("%s finish deleting from plan_table_data\n", formatLogTime());
}

static bool checkDangerCharacter(const char* input)
{
    const char* dangerCharacterList[] = {";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", NULL};

    for (int i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr(input, dangerCharacterList[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/*
 * @Description: get the session_id_list from plan_table_data of current database.
 * @in conn: connection to the database.
 * @return: void
 */
static void getSessionIdListInPlantable(PGconn* conn)
{
    int sessid_count;
    int i;
    PGresult* res = NULL;
    errno_t rc = EOK;
    char* session_id = NULL;

#define STMT_LEN 48
    char* stmt_get_sessid_list = (char*)malloc(STMT_LEN + 1);
    checkAllocMem(stmt_get_sessid_list);
    (void)memset_s(stmt_get_sessid_list, STMT_LEN + 1, '\0', STMT_LEN + 1);

    rc = snprintf_s(stmt_get_sessid_list, STMT_LEN + 1, STMT_LEN, "SELECT DISTINCT(session_id) FROM plan_table_data");
    securec_check_ss_c(rc, "\0", "\0");

    if (g_log_level == DEBUG)
        write_stderr("%s start getting sessiond_id from PLAN_TABLE_DATA on %s\n", formatLogTime(), my_nodename);

    res = PQexec(conn, stmt_get_sessid_list);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s could not obtain sessiond_id from PLAN_TABLE_DATA.\n", formatLogTime());
        PQclear(res);
        free(stmt_get_sessid_list);
        exit(1);
    }
    sessid_count = PQntuples(res);

    /* get sission id list */
    for (i = 0; i < sessid_count; i++) {
        session_id = PQgetvalue(res, i, 0);
        if (checkDangerCharacter(session_id)) {
            write_stderr("%s session id contains invalid token:%s\n", formatLogTime(), session_id);
            PQclear(res);
            free(stmt_get_sessid_list);
            exit(1);
        }
        addSessionIdInfo(session_id);
    }

    PQclear(res);
    free(stmt_get_sessid_list);
}

/*
 * @Description: add session id info into session id list.
 * @in sessid: session id is made up of thread start time and pid.
 * @return: void
 */
static void addSessionIdInfo(char* sessid)
{
    sessionid_info* add_sessid = NULL;

    add_sessid = (sessionid_info*)malloc(sizeof(sessionid_info));
    checkAllocMem(add_sessid);

    add_sessid->next = NULL;
    add_sessid->session_id = strdup(sessid);
    checkAllocMem(add_sessid->session_id);
    if (g_log_level == DEBUG)
        write_stderr("%s add session id: %s\n", formatLogTime(), sessid);

    if (session_id_info == NULL)
        session_id_info = add_sessid;
    else {
        add_sessid->next = session_id_info->next;
        session_id_info->next = add_sessid;
    }
}

/*
 * @Description: delete plan table data for all thread if need.
 * @in conn: connection to the database.
 * @in missing_ok: missing is not ok.
 * @return: void
 */
static void deletePlanTableDataS(PGconn* conn, bool missing_ok)
{
    sessionid_info* sessid_cell = session_id_info;
    activebackend_info* bkid_cell = NULL;

    if (session_id_info == NULL || active_backend_info == NULL)
        return;

    while (sessid_cell != NULL) {
        int64 plan_table_data_inserted_thread = strtollSafe(strrchrSafe(sessid_cell->session_id, '.') + 1, 10);
        bkid_cell = active_backend_info;
        while (bkid_cell != NULL) {
            if (plan_table_data_inserted_thread == bkid_cell->sessionID)
                break;

            bkid_cell = bkid_cell->next;
        }

        /* Now, we have find the inactive session id in plan_table_data */
        if (bkid_cell == NULL) {
            /*
             * We don`t need to make sure the thread may be active on other cn.
             * because the data store in each CN is not shared among others.
             * We should only delete data on this node, now we just done like we want.
             */
            deletePlanTableData(conn, sessid_cell->session_id, missing_ok);
        }

        sessid_cell = sessid_cell->next;
    }

    /* free session_id list. */
    cleanPlanTableSessidList();
}

/*
 * @Description: excute the sql to delete plan table data.
 * @in conn: connection to the database.
 * @in sessid: session_id that should be delete.
 * @in missing_ok: missing is not ok.
 * @return: void
 */
static void deletePlanTableData(PGconn* conn, char* sessid, bool missing_ok)
{
    PGresult* res = NULL;
    errno_t rc = EOK;

/* 32 is session_id length. */
#define delete_stmt_len (45 + 32)

    /* make up SQL delete stmt */
    char* stmt_delete_plan_table_data = (char*)malloc(delete_stmt_len + 1);
    checkAllocMem(stmt_delete_plan_table_data);

    rc = snprintf_s(stmt_delete_plan_table_data,
        delete_stmt_len + 1,
        delete_stmt_len,
        "DELETE FROM plan_table_data where session_id=%s;",
        sessid);
    securec_check_ss_c(rc, "\0", "\0");

    if (g_log_level == DEBUG)
        write_stderr("%s %s\n", formatLogTime(), stmt_delete_plan_table_data);

    res = PQexec(conn, stmt_delete_plan_table_data);

    /* If execution is not successful, give an log and go on.*/
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK) {
        write_stderr("%s could not delete data from plan_table_data: %s\n", formatLogTime(), PQresultErrorMessage(res));
    }

    free(stmt_delete_plan_table_data);
    PQclear(res);
}

/*
 * @Description: free the session_id list.
 * @return: void
 */
static void cleanPlanTableSessidList()
{
    sessionid_info* sessid_cell = session_id_info;
    sessionid_info* delete_cell = NULL;

    while (sessid_cell != NULL) {
        delete_cell = sessid_cell;
        sessid_cell = sessid_cell->next;
        if (delete_cell->session_id != NULL) {
            free(delete_cell->session_id);
        }
        free(delete_cell);
    }

    session_id_info = NULL;
}

/*
 * @Description: Drop inactive temp schemas of each database.
 * @in: void
 * @return: void
 */
static void dropTempNamespace()
{
    database_info* cur_database = NULL;
    int datanode_no = -1;

    if (head_database_info != NULL) {
        if (verbose_opt)
            write_stderr("%s start droping temp namespaces\n", formatLogTime());

        for (cur_database = head_database_info; cur_database != NULL; cur_database = cur_database->next) {
            if (g_log_level == DEBUG)
                write_stderr("%s %s: drop temp namespace for database \"%s\"\n",
                    formatLogTime(),
                    progname,
                    cur_database->database_name);

            coord_conn = loginDatabase(coordinator_host,
                coordinator_port,
                username,
                password,
                cur_database->database_name,
                progname,
                "auto",
                password_prompt);

            if (coord_conn == NULL) {
                write_stderr(
                    "%s Could not connect to the database %s.\n", formatLogTime(), cur_database->database_name);
                return;
            }

            if (!setMaintenanceMode(coord_conn, true)) {
                /* Cannot recover */
                write_stderr("%s can not set maintenance mode.\n", formatLogTime());
                PQfinish(coord_conn);
                coord_conn = NULL;
                return;
            }

            if (!setEnableParallelDDL(coord_conn, false)) {
                /* Cannot recover */
                write_stderr("%s can not set enable_parallel_ddl.\n", formatLogTime());
                PQfinish(coord_conn);
                coord_conn = NULL;
                return;
            }

            if (g_log_level == DEBUG)
                write_stderr("%s %s: connected to the database \"%s\"\n",
                    formatLogTime(),
                    progname,
                    cur_database->database_name);

            (void)getTempSchemaListOnCN(coord_conn);
            (void)getActiveBackendListOnCN(coord_conn);
            (void)dropTempSchemas(coord_conn, true);
            (void)cleanBackendList();

            datanode_no = getTempSchemaListOnDN(coord_conn);
            if (datanode_no != -1) {
                (void)getActiveBackendListOnCN(coord_conn, datanode_no);
                (void)dropTempSchemas(coord_conn, true);
                (void)cleanBackendList();
            }

            if (g_log_level == DEBUG)
                write_stderr("%s %s: drop temp namespace for database \"%s\" finished\n",
                    formatLogTime(),
                    progname,
                    cur_database->database_name);
            PQfinish(coord_conn);
            coord_conn = NULL;
        }
    }

    if (verbose_opt)
        write_stderr("%s finish droping temp namespaces\n", formatLogTime());
}

static void dropTempSchemas(PGconn* conn, bool missing_ok)
{
    tempschema_info* cell1 = temp_schema_info;
    activebackend_info* cell2 = NULL;
    char node_name[NAMEDATALEN];
    char temp_buffer[NAMEDATALEN] = {0};
    char temp_buffer2[NAMEDATALEN] = {0};
    bool onOtherCNNode = false;
    errno_t rc;

    if (temp_schema_info == NULL || active_backend_info == NULL)
        return;

    while (cell1 != NULL) {
        /*
         * Get threadID, tempID, timelineID from end to start. Temp schema name is pg_temp_%s_%u_%u_%lu.
         * These items are CN'name, timelineID, tempID, threadID.
         */
        const char* last_pos = strrchrSafe(cell1->tempschema_name, '_');
        int64 temp_table_created_session = strtollSafe(last_pos + 1, 10);
        rc = strncpy_s(temp_buffer, sizeof(temp_buffer), cell1->tempschema_name, last_pos - cell1->tempschema_name);
        securec_check_c(rc, "\0", "\0");
        int temp_table_lwtid = 0;
        temp_table_lwtid = strtolSafe(strrchrSafe(temp_buffer, '_') + 1, 10);
        const char* second_last_pos = strrchrSafe(temp_buffer, '_');
        uint32 tempID = strtolSafe(second_last_pos + 1, 10);
        rc = strncpy_s(temp_buffer2, sizeof(temp_buffer2), temp_buffer, second_last_pos - temp_buffer);
        securec_check_c(rc, "\0", "\0");
        uint32 timeLineID = strtolSafe(strrchrSafe(temp_buffer2, '_') + 1, 10);

        cell2 = active_backend_info;
        /*
         * Tid is obtained from pthread_self, which is reuse often.
         * When the session of a temp table is actually out-of-date, and a new session reuse the tid,
         * we cannot drop the temp table although it is out-of-data.
         * Thus, We use tid, timeLineID and tempID together to judge which session a temp table belongs to, instead of
         * tid only.
         */
        while (cell2 != NULL) {
            if (temp_table_created_session == cell2->sessionID && tempID == cell2->tempID &&
                timeLineID == cell2->timeLineID)
                break;

            cell2 = cell2->next;
        }

        if (cell2 == NULL) {
            /*
             * now it is not the active backend, furthermore, we need to
             * check whether the schema on the other node:
             * if not
             * 	(there are two scenarios:
             * 	1. the schema built on the current CN or DN node
             * 	2. the schema built on a dropped node
             * ),
             * drop it;
             *
             * otherwise, keep it untouched.
             */
            onOtherCNNode = false;
            rc = memset_s(node_name, sizeof(node_name), 0, sizeof(node_name));
            securec_check_c(rc, "", "");

            /*
             * tempschema format:  pg_temp_nodename_xx_xx_PID
             *
             * to get node name, we set char '_' just behind nodename as '\0',
             * recall that there are three '_' behind the nodename, and copy
             * node name start from &tempschema_name[8].
             *
             * Note: nodename may contain several chars '_' itself, but there
             * are specific numbers(i.e. 3) of '_' behind the nodename.
             */
            char* p = NULL;
            int j = 0;
            rc = strcpy_s(node_name, sizeof(node_name), &cell1->tempschema_name[8]);
            securec_check_c(rc, "\0", "\0");
            while (j < 3) {
                p = strrchr(node_name, '_');
                if (p == NULL) {
                    write_stderr("%s Check temp schema: %s, Expected format: pg_temp_nodename_xx_xx_PID.\n",
                        formatLogTime(),
                        cell1->tempschema_name);
                    exit(1);
                }
                *p = '\0';
                j++;
            }
            Assert(*node_name != '\0');

            if (strlen(my_nodename) != strlen(node_name)) {
                write_stderr("%s Node name %s in temp schema: %s could be truncated in InitTempTableNamespace.\n",
                             formatLogTime(),
                             node_name,
                             cell1->tempschema_name);
            }

            /* if it is current CN node, just drop it (recall that it is non-active backend one) */
            if (strcmp(my_nodename, node_name) == 0)
                dropTempSchema(conn, cell1->tempschema_name, missing_ok);
            else {
                /*
                 * otherwise, if it is other CN node(do not include dropped node), untouch it
                 * since these temp schemas will be dropped by their CN elsewhere
                 */
                for (int i = 0; i < pgxc_clean_node_count; i++) {
                    if (pgxc_clean_node_info[i].type == NODE_TYPE_COORD &&
                        strcmp(pgxc_clean_node_info[i].node_name, node_name) == 0) {
                        /*
                         * find the schema on other CN node, if find, we do not drop the schema.
                         * here, the case 'node_name == my_nodename' can't not happen
                         */
                        onOtherCNNode = true;
                        break;
                    }
                }

                /* only drop schemas related to this node (include the obsolete node) */
                if (!onOtherCNNode)
                    dropTempSchema(conn, cell1->tempschema_name, missing_ok);
            }
        }

        cell1 = cell1->next;
    }

    cleanTempSchemaList();
}
static void cleanTempSchemaList()
{
    tempschema_info* cell1 = temp_schema_info;
    tempschema_info* saved_cell = NULL;

    while (cell1 != NULL) {
        saved_cell = cell1;
        cell1 = cell1->next;
        free(saved_cell->tempschema_name);
        free(saved_cell);
    }

    temp_schema_info = NULL;
}

static void cleanBackendList()
{
    activebackend_info* cell1 = active_backend_info;
    activebackend_info* saved_cell = NULL;

    while (cell1 != NULL) {
        saved_cell = cell1;
        cell1 = cell1->next;
        free(saved_cell);
    }

    active_backend_info = NULL;
}

static void dropTempSchema(PGconn* conn, char* nspname, bool missing_ok)
{
#define dropSchemaStmtLen (2 * NAMEDATALEN + 64)
    PGresult* res = NULL;
    int rc;

    char* toastnspname = (char*)malloc(NAMEDATALEN);
    checkAllocMem(toastnspname);

    /* SQL Statement */
    char* STMT_DROP_TEMP_SCHEMA = (char*)malloc(dropSchemaStmtLen);
    checkAllocMem(STMT_DROP_TEMP_SCHEMA);

    rc = snprintf_s(toastnspname, NAMEDATALEN, strlen(nspname) + 7, "pg_toast_temp_%s", nspname + 8);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(STMT_DROP_TEMP_SCHEMA,
        dropSchemaStmtLen,
        2 * strlen(nspname) + 30,
        "DROP SCHEMA %s, %s CASCADE;",
        nspname,
        toastnspname);
    securec_check_ss_c(rc, "\0", "\0");

    if (missing_ok)
        rc = snprintf_s(STMT_DROP_TEMP_SCHEMA,
            dropSchemaStmtLen,
            2 * strlen(nspname) + 40,
            "DROP SCHEMA IF EXISTS %s, %s CASCADE;",
            nspname,
            toastnspname);
    else
        rc = snprintf_s(STMT_DROP_TEMP_SCHEMA,
            dropSchemaStmtLen,
            2 * strlen(nspname) + 30,
            "DROP SCHEMA %s, %s CASCADE;",
            nspname,
            toastnspname);
    securec_check_ss_c(rc, "\0", "\0");

    if (g_log_level == DEBUG)
        write_stderr("%s %s\n", formatLogTime(), STMT_DROP_TEMP_SCHEMA);

    res = PQexec(conn, STMT_DROP_TEMP_SCHEMA);

    /* If exec is not success, give an log and go on. */
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK) {
        write_stderr("%s Could not drop temp schema %s, %s: %s\n",
            formatLogTime(),
            nspname,
            toastnspname,
            PQresultErrorMessage(res));
    }

    free(STMT_DROP_TEMP_SCHEMA);
    free(toastnspname);
    PQclear(res);
}

static char* pg_strdup(const char* string)
{
    char* tmp = NULL;

    if (string == NULL) {
        write_stderr("%s pg_strdup: cannot duplicate null pointer (internal error)\n", formatLogTime());
        exit(1);
    }
    tmp = strdup(string);
    if (tmp == NULL) {
        write_stderr("%s pg_strdup: out of memory\n", formatLogTime());
        exit(1);
    }
    return tmp;
}

static void getMyNodename(PGconn* conn)
{
    static const char* stmt = "SELECT pgxc_node_str()";
    PGresult* res = NULL;

    res = PQexec(conn, stmt);

    /* Error handling here */
    if (res != NULL && (PQresultStatus(res) == PGRES_TUPLES_OK))
        my_nodename = pg_strdup(PQgetvalue(res, 0, 0));
    else
        my_nodename = pg_strdup("unknown");

    PQclear(res);
}

static void getMyGtmMode(PGconn* conn)
{
    static const char* stmt = "SHOW ENABLE_GTM_FREE";
    static const char* stmt_gtm_option = "SHOW GTM_OPTION";
    PGresult* res = NULL;

    res = PQexec(conn, stmt);

    /* Error handling here */
    if (res != NULL && (PQresultStatus(res) == PGRES_TUPLES_OK)) {
        gtm_mode = pg_strdup(PQgetvalue(res, 0, 0));
    } else {
        gtm_mode = pg_strdup("unknown");
    }
    PQclear(res);

    res = PQexec(conn, stmt_gtm_option);
    /* Error handling here */
    if (res != NULL && (PQresultStatus(res) == PGRES_TUPLES_OK)) {
        gtm_option = pg_strdup(PQgetvalue(res, 0, 0));
        gtm_option_num = atoi(gtm_option);
    } else {
        gtm_option = pg_strdup("unknown");
        gtm_option_num = -1;
    }
    PQclear(res);
}

static void recover2PCForDatabase(database_info* db_info, CleanWorkerInfo* wkinfo)
{
    txn_info* cur_txn = NULL;
    int num = wkinfo->work_job;
    if (verbose_opt)
        write_stderr("job%d: %s %s: recovering 2PC for database \"%s\"\n",
            num,
            formatLogTime(),
            progname,
            db_info->database_name);
    wkinfo->conn = loginDatabase(coordinator_host,
        coordinator_port,
        username,
        password,
        db_info->database_name,
        progname,
        "auto",
        password_prompt);
    if (wkinfo->conn == NULL) {
        write_stderr("job%d: %s Could not connect to the database %s.\n", num, formatLogTime(), db_info->database_name);
        return;
    }
    if (!setMaintenanceMode(wkinfo->conn, true)) {
        /* Cannot recover */
        write_stderr("job%d: %s Skipping database %s.\n", num, formatLogTime(), db_info->database_name);
        PQfinish(wkinfo->conn);
        wkinfo->conn = NULL;
        return;
    }
    if (verbose_opt)
        write_stderr(
            "job%d: %s %s: connected to the database \"%s\"\n", num, formatLogTime(), progname, db_info->database_name);
    for (cur_txn = db_info->all_head_txn_info[num]; cur_txn != NULL; cur_txn = cur_txn->next) {
        recover2PC(wkinfo->conn, cur_txn, num);
    }
    PQfinish(wkinfo->conn);
    wkinfo->conn = NULL;
}

static void recover2PC(PGconn* conn, txn_info* txn, int num)
{
    TXN_STATUS txn_stat;

    txn_stat = check_txn_global_status(txn, commit_all_prepared, rollback_all_prepared);
    if (verbose_opt) {
        write_stderr("job%d: %s     Recovering TXN: localxid: %lu, gid: \"%s\", owner: \"%s\", global status: %s\n",
            num,
            formatLogTime(),
            txn->localxid,
            txn->gid,
            txn->owner,
            str_txn_stat(txn_stat));
    }
    switch (txn_stat) {
        case TXN_STATUS_FAILED:
            if (verbose_opt)
                write_stderr("job%d: %s         Recovery not needed.\n", num, formatLogTime());
            return;
        case TXN_STATUS_PREPARED:
            if (commit_all_prepared)
                do_commit(conn, txn, num);
            else if (rollback_all_prepared)
                do_abort(conn, txn, num);
            else if (verbose_opt)
                write_stderr(
                    "job%d: %s         Recovery not needed, transaction status: all prepared.\n", num, formatLogTime());
            return;
        case TXN_STATUS_COMMITTED:
            do_commit(conn, txn, num);
            return;
        case TXN_STATUS_ABORTED:
            do_abort(conn, txn, num);
            return;
        case TXN_STATUS_UNCONNECT:
            if (verbose_opt)
                write_stderr("job%d: %s         some node connect bad, do nothing for the prepared xid %lu.\n",
                    num,
                    formatLogTime(),
                    txn->localxid);
            return;
        case TXN_STATUS_RUNNING:
            if (verbose_opt)
                write_stderr(
                    "job%d: %s         the prepared transaction is running, do nothing for the prepared xid %lu.\n",
                    num,
                    formatLogTime(),
                    txn->localxid);
            return;
        default:
            write_stderr("job%d: %s         Unknown TXN status, pgxc_clean error.\n", num, formatLogTime());
            exit(1);
    }
    return;
}

static void do_commit(PGconn* conn, txn_info* txn, int num)
{
    do_commit_abort(conn, txn, true, num);
}

static void do_abort(PGconn* conn, txn_info* txn, int num)
{
    do_commit_abort(conn, txn, false, num);
}

#define COMMITSEQNO_COMMIT_INPROGRESS (UINT64CONST(1) << 62)
#define COMMITSEQNO_SUBTRANS_BIT (UINT64CONST(1) << 63)
#define MAX_COMMITSEQNO UINT64CONST((UINT64CONST(1) << 60) - 1)
#define COMMITSEQNO_FROZEN UINT64CONST(0x2)
#define COMMITSEQNO_IS_COMMITTING(csn) (((csn)&COMMITSEQNO_COMMIT_INPROGRESS) == COMMITSEQNO_COMMIT_INPROGRESS)
#define COMMITSEQNO_IS_SUBTRANS(csn) (((csn)&COMMITSEQNO_SUBTRANS_BIT) == COMMITSEQNO_SUBTRANS_BIT)
#define COMMITSEQNO_IS_COMMITTED(csn) \
    (((csn)&MAX_COMMITSEQNO) >= COMMITSEQNO_FROZEN && !COMMITSEQNO_IS_SUBTRANS(csn) && !COMMITSEQNO_IS_COMMITTING(csn))

const char* const EXEC_STMT_FMT_CSN =
    "SELECT * FROM GLOBAL_CLEAN_PREPARED_XACTS('%s PREPARED ''%s'' WITH ''%s'';','%s');";
const char* const GLOBAL_STMT_FMT_CSN = "%s PREPARED '%s' WITH '%s';";
const char* const EXEC_STMT_FMT = "SELECT * FROM GLOBAL_CLEAN_PREPARED_XACTS('%s PREPARED ''%s'';','%s');";
const char* const GLOBAL_STMT_FMT = "%s PREPARED '%s';";

const char* const NEW_GID_EXEC_STMT_FMT_CSN =
    "SELECT * FROM GLOBAL_CLEAN_PREPARED_XACTS('N%s PREPARED ''%%s'' WITH ''%s'';','%s');";
const char* const NEW_GID_EXEC_STMT_FMT = "SELECT * FROM GLOBAL_CLEAN_PREPARED_XACTS('N%s PREPARED ''%%s'';','%s');";

const int MAXSTMTLEN = 4096;
const int MAXNODELISTLEN = (4096 - 256);

static void do_commit_abort(PGconn* conn, txn_info* txn, bool is_commit, int num)
{
    int ii;
    char stmt[MAXSTMTLEN];
    char node_list[MAXNODELISTLEN];
    PGresult* res = NULL;
    ExecStatusType res_status;
    int rc;
    int node_id = 0;
    char* csn = NULL;
    uint64 csnll = 0;
    bool meet_error = false;
    bool enable_gtm_free = (strcmp(gtm_mode, "on") == 0);

    /*
     * Get the CSN value when commit(and not gtm_free, because the node will use the
     * local csn for gtm free), send it to the other nodes.
     */
    if (is_commit && !enable_gtm_free && gtm_option_num != 2) {
        bool found = false;
        if (txn->cn_xid != 0) {
            for (ii = 0; ii < pgxc_clean_node_count; ii++) {
                /* always reference matched coordiantor */
                if (strcmp(pgxc_clean_node_info[ii].node_name, txn->cn_nodename) == 0) {
                    node_id = ii;
                    found = true;
                    break;
                }
            }
        }

        /* First get the CSN value from primary CN */
        if (found) {
            csn = fetchCsnInfo(conn, txn->cn_xid, node_id, &meet_error);
            csnll = convertCsnStr(csn);
            Assert(csnll != 1);
        }

        /*
         * If the primary CN is missing or meet error(failed to get a valid csn
         * from CN), we need to search the other nodes.
         */
        if (!COMMITSEQNO_IS_COMMITTED(csnll)) {
            for (ii = 0; ii < pgxc_clean_node_count; ii++) {
                if (strcmp(pgxc_clean_node_info[ii].node_name, txn->cn_nodename) != 0) {
                    char* dnCsn = NULL;
                    uint64 dnCsnll = 0;
                    if (gtm_option_num == 0) {  /* gtm mode, direct fetch csn from other node */
                        dnCsn = fetchCsnInfo(conn, txn->cn_xid, ii, &meet_error);
                        dnCsnll = convertCsnStr(dnCsn);
                    } else if (gtm_option_num == 1) {  /* gtm lite mode, fetch csn from next node' xid */
                        int next_node_idx = txn->txn_gid_info[ii].next_node_idx;
                        GlobalTransactionId next_node_xid = txn->txn_gid_info[ii].next_node_xid;
                        if (next_node_xid == 0)
                            continue;
                        dnCsn = fetchCsnInfo(conn, next_node_xid, next_node_idx, &meet_error);
                        dnCsnll = convertCsnStr(dnCsn);
                    }

                    /*
                     * If we find a valid csn(not frozen) from other nodes,
                     * then break and use it.
                     */
                    if (COMMITSEQNO_IS_COMMITTED(dnCsnll)) {
                        /* free old csn memory */
                        if (csn != NULL) {
                            free(csn);
                        }
                        csn = dnCsn;
                        csnll = dnCsnll;
                        break;
                    } else if (dnCsn != NULL) {
                        free(dnCsn);
                    }
                    /* every one has the same next_node_idx, just fetch once */
                    if (txn->new_ddl_version) {
                        break;
                    }
                }
            }
        }
    }

    /*
     * For commit gtm transaction, go on with gs_clean when the csn is comitted,
     * or we just ignore this gs_clean for the current transaction.
     */
    if ((is_commit && !enable_gtm_free && gtm_option_num != 2) && !COMMITSEQNO_IS_COMMITTED((uint64)csnll)) {
        if (csn != NULL) {
            free(csn);
        }
        do_commit_abort_failed = true;
        return;
    }

    if (verbose_opt)
        write_stderr("job%d: %s     %s...\n", num, formatLogTime(), is_commit ? "committing" : "aborting");

    /* node_name */
    int use_lenth = 0;
    bool send_query = false;
    bool need_send = false;
    for (ii = 0; ii < pgxc_clean_node_count; ii++) {
        if (txn->txn_stat[ii] == TXN_STATUS_PREPARED && ii != my_nodeidx) {
            int left_space = MAXNODELISTLEN - 1 - use_lenth;
            int node_name_len = strlen(pgxc_clean_node_info[ii].node_name);
            int gid_len = (txn->new_version && txn->txn_gid_info[ii].my_gid != NULL) ?
                          strlen(txn->txn_gid_info[ii].my_gid) + 1 : 0;
            int src_lenth = node_name_len + gid_len; /* append space of ',' */
            need_send = true;
            Assert(left_space >= 0);
            if (left_space < src_lenth + 1) {
                /* have no enough space */
                send_query = true;
                ii--;
            } else if (use_lenth == 0) {
                rc = strcpy_s(node_list + use_lenth, left_space, pgxc_clean_node_info[ii].node_name);
                securec_check_c(rc, "\0", "\0");
                if (txn->new_version) {
                    rc = strcpy_s(node_list + use_lenth + node_name_len, left_space, ",");
                    securec_check_c(rc, "\0", "\0");
                    rc = strcpy_s(node_list + use_lenth + node_name_len + 1, left_space,
                                  txn->txn_gid_info[ii].my_gid);
                    securec_check_c(rc, "\0", "\0");
                }
                use_lenth += src_lenth;
            } else {
                rc = strcpy_s(node_list + use_lenth, left_space, ",");
                securec_check_c(rc, "\0", "\0");
                rc = strcpy_s(node_list + use_lenth + 1, left_space, pgxc_clean_node_info[ii].node_name);
                securec_check_c(rc, "\0", "\0");
                if (txn->new_version) {
                    rc = strcpy_s(node_list + use_lenth + 1 + node_name_len, left_space, ",");
                    securec_check_c(rc, "\0", "\0");
                    rc = strcpy_s(node_list + use_lenth + 1 + node_name_len + 1, left_space,
                                 txn->txn_gid_info[ii].my_gid);
                    securec_check_c(rc, "\0", "\0");
                }
                use_lenth += src_lenth + 1;
            }
        }

        if (send_query || (ii == pgxc_clean_node_count - 1 && need_send)) {
            if (csn != NULL) {
                if (txn->new_version) {
                    rc = sprintf_s(stmt, sizeof(stmt), NEW_GID_EXEC_STMT_FMT_CSN,
                                  is_commit ? "COMMIT" : "ROLLBACK", csn, node_list);
                } else {
                    rc = sprintf_s(stmt, sizeof(stmt), EXEC_STMT_FMT_CSN,
                                  is_commit ? "COMMIT" : "ROLLBACK", txn->gid, csn, node_list);
                }
            } else {
                if (txn->new_version) {
                    rc = sprintf_s(stmt, sizeof(stmt), NEW_GID_EXEC_STMT_FMT,
                                  is_commit ? "COMMIT" : "ROLLBACK", node_list);
                } else {
                    rc = sprintf_s(stmt, sizeof(stmt), EXEC_STMT_FMT,
                                  is_commit ? "COMMIT" : "ROLLBACK", txn->gid, node_list);
                }
            }
            securec_check_ss_c(rc, "\0", "\0");
            res = PQexec(conn, stmt);
            res_status = PQresultStatus(res);
            if (verbose_opt) {
                if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
                    write_stderr("job%d: %s succeeded (%s)\n", num, formatLogTime(), node_list);
                else
                    write_stderr("job%d: %s send on (%s), failed message (%s)\n",
                        num,
                        formatLogTime(),
                        node_list,
                        PQresultErrorMessage(res));
            } else {
                if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK) {
                    write_stderr("job%d: %s send on (%s), failed message (%s)\n",
                        num,
                        formatLogTime(),
                        node_list,
                        PQresultErrorMessage(res));
                }
            }
            PQclear(res);
            send_query = false;
            rc = memset_s(node_list, MAXNODELISTLEN, 0, MAXNODELISTLEN);
            securec_check_c(rc, "\0", "\0");
            use_lenth = 0;
            need_send = false;
        }
    }

    if (txn->txn_stat[my_nodeidx] == TXN_STATUS_PREPARED) {
        /* Issue global statment */
        char *finish_prepared_gid = (txn->new_version ? txn->txn_gid_info[my_nodeidx].my_gid : txn->gid);
        if (csn != NULL) {
            rc = sprintf_s(stmt, sizeof(stmt), GLOBAL_STMT_FMT_CSN,
                          is_commit ? "COMMIT" : "ROLLBACK", finish_prepared_gid, csn);
        } else {
            rc = sprintf_s(stmt, sizeof(stmt), GLOBAL_STMT_FMT,
                          is_commit ? "COMMIT" : "ROLLBACK", finish_prepared_gid);
        }
        securec_check_ss_c(rc, "\0", "\0");

        res = PQexec(conn, stmt);
        res_status = PQresultStatus(res);
        if (verbose_opt) {
            if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK)
                write_stderr("job%d: %s succeeded (%s)\n", num, formatLogTime(), my_nodename);
            else {
                do_commit_abort_failed = true;
                write_stderr(
                    "job%d: %s failed (%s: %s)\n", num, formatLogTime(), my_nodename, PQresultErrorMessage(res));
            }
        }

        else if (res_status != PGRES_COMMAND_OK && res_status != PGRES_TUPLES_OK) {
            do_commit_abort_failed = true;
            write_stderr(
                "job%d: %s Failed to recover TXN, localxid: %lu, gid: \"%s\", owner: \"%s\", node: \"%s\" (%s)\n",
                num,
                formatLogTime(),
                txn->localxid,
                txn->gid,
                txn->owner,
                my_nodename,
                PQresultErrorMessage(res));
        }
        PQclear(res);
    }

    if (csn != NULL)
        free(csn);
}

static int64 convertCsnStr(const char* csn)
{
    int64 ret = 0;
    if (csn != NULL)
        ret = atoll(csn);
    return ret;
}

static char* fetchCsnInfo(PGconn* conn, GlobalTransactionId gxid, int node_id, bool* meet_error)
{
    Assert(gxid != 0);
    PGresult* res = NULL;
    char* res_s = NULL;
    int rc;
    char* csn = NULL;
    char stmt[1024];

    char* node_name = pgxc_clean_node_info[node_id].node_name;
    static const char* STMT_FORM = "EXECUTE DIRECT ON (%s) 'SELECT "
                                   "pgxc_get_csn(''%lu''::xid);'";

    rc = sprintf_s(stmt, sizeof(stmt), STMT_FORM, node_name, gxid);

    securec_check_ss_c(rc, "\0", "\0");
    res = PQexec(conn, stmt);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not obtain transaction csn for node "
                     "%s, gxid %lu\n",
            formatLogTime(),
            node_name,
            gxid);
        PQclear(res);
        *meet_error = true;
        return csn;
    }
    if (PQgetisnull(res, 0, 0)) {
        PQclear(res);
        return csn;
    }
    res_s = PQgetvalue(res, 0, 0);
    if (res_s != NULL) {
        csn = pg_strdup(res_s);
    }
    PQclear(res);

    return csn;
}

static PGconn* loginDatabase(char* host, int port, char* user, char* password, char* dbname, const char* progname,
    char* encoding, const char* password_prompt)
{
    bool new_pass = false;
    PGconn* coord_conn = NULL;
    char port_s[32];
#define PARAMS_ARRAY_SIZE 10
    const char* keywords[PARAMS_ARRAY_SIZE];
    const char* values[PARAMS_ARRAY_SIZE];
    int rc;
    bool need_clear_pass = false;

    rc = sprintf_s(port_s, sizeof(port_s), "%d", port);
    securec_check_ss_c(rc, "\0", "\0");

    keywords[0] = "host";
    values[0] = host;
    keywords[1] = "port";
    values[1] = port_s;
    keywords[2] = "user";
    values[2] = user;
    keywords[3] = "password";
    keywords[4] = "dbname";
    values[4] = dbname;
    keywords[5] = "fallback_application_name";
    values[5] = progname;
    keywords[6] = "client_encoding";
    values[6] = encoding;
    keywords[7] = "connect_timeout";
    values[7] = connect_timeout;
    keywords[8] = "options";
    if (clean_temp_schema_only == false) {
        /* this mode: set timeout = 180 seconds */
        values[8] = "-c xc_maintenance_mode=on -c statement_timeout=180000 -c max_query_retry_times=0";
    } else {
        /* this mode: remove timeout */
        values[8] = "-c xc_maintenance_mode=on";
    }
    keywords[9] = NULL;
    values[9] = NULL;

    /* Loop until we have a password if requested by backend */
    do {
        values[3] = password;

        new_pass = false;
        coord_conn = PQconnectdbParams(keywords, values, true);

        if (PQstatus(coord_conn) == CONNECTION_BAD && PQconnectionNeedsPassword(coord_conn) && password == NULL &&
            try_password_opt != TRI_NO) {
            PQfinish(coord_conn);
            password = simple_prompt(password_prompt, 100, false);
            new_pass = true;
            need_clear_pass = true;
        }
    } while (new_pass);

    /* if the password is get from simple_prompt in the above instead of global password, it should be free avoid
     * memmory leak */
    if (need_clear_pass && password != NULL) {
        rc = memset_s(password, strlen(password), 0, strlen(password));
        securec_check_c(rc, "\0", "\0");
        free(password);
        password = NULL;
        values[3] = NULL;
    }

    if ((coord_conn != NULL) && PQstatus(coord_conn) == CONNECTION_BAD) {
        PQfinish(coord_conn);
        return NULL;
    }

    return (coord_conn);
}

static TXN_STATUS getTxnStatus(PGconn* conn, GlobalTransactionId gxid, int node_idx, bool primary_cn)
{
    char stmt[1024];
    char* res_s = NULL;
    int rc;
    PGresult* res1 = NULL;
    PGresult* res2 = NULL;
    char* node_name = pgxc_clean_node_info[node_idx].node_name;
    unsigned int count = 0;

    if (primary_cn) {
        /* Check if the prepared transaction is running on this node */
        static const char* STMT_RUNNING = "EXECUTE DIRECT ON (%s) 'select "
                                          "count(1) from pg_running_xacts where prepare_xid::text = %lu;' ";
        rc = sprintf_s(stmt, sizeof(stmt), STMT_RUNNING, node_name, gxid);
        securec_check_ss_c(rc, "\0", "\0");

        res1 = PQexec(conn, stmt);
        if (res1 == NULL || (PQresultStatus(res1) != PGRES_TUPLES_OK)) {
            write_stderr("%s Could not get the status of prepared transaction %lu for node %s.(%s)\n",
                formatLogTime(),
                gxid,
                node_name,
                res1 != NULL ? PQresultErrorMessage(res1) : "");
            PQclear(res1);
            return TXN_STATUS_UNCONNECT;
        }
        if (PQntuples(res1) > 0) {
            count = atoi(PQgetvalue(res1, 0, 0));
        } else {
            count = 1; /* treat it as running if doubt */
        }

        PQclear(res1);
        /*
         * Prepared transaction that is running, need not clear it, coordinator will finish it then,
         * otherwise, we treat it as remained/pending, we should clear it.
         */
        if (count > 0)
            return TXN_STATUS_RUNNING;
    }

    /* Not running, but remained prepared transaction, check if committed */
    static const char* STMT_FORM = "EXECUTE DIRECT ON (%s) 'SELECT "
                                   "pgxc_is_committed(''%lu''::xid);'";
    rc = sprintf_s(stmt, sizeof(stmt), STMT_FORM, node_name, gxid);
    securec_check_ss_c(rc, "\0", "\0");

    res2 = PQexec(conn, stmt);
    if (res2 == NULL || PQresultStatus(res2) != PGRES_TUPLES_OK) {
        write_stderr(
            "%s Could not obtain transaction status for node %s, gxid %lu\n", formatLogTime(), node_name, gxid);
        PQclear(res2);
        return TXN_STATUS_UNCONNECT;
    }
    if (PQgetisnull(res2, 0, 0)) {
        PQclear(res2);
        return TXN_STATUS_UNKNOWN;
    }
    res_s = PQgetvalue(res2, 0, 0);
    if (strcmp(res_s, "t") == 0) {
        PQclear(res2);
        return TXN_STATUS_COMMITTED;
    } else if (strcmp(res_s, "f") == 0) {
        PQclear(res2);
        return TXN_STATUS_ABORTED;
    } else {
        /* we should not fall down here in normal case */
        write_stderr("%s Obtain unknown transaction status: %s for node %s, gxid %lu\n",
            formatLogTime(),
            res_s,
            node_name,
            gxid);
        PQclear(res2);
        /* something wrong happend, we don't use gs_clean to clear it */
        return TXN_STATUS_RUNNING;
    }
}

/* Get txn status from primary cn node, called by getTxnInfoOnOtherNodes() */
static bool getTxnStatOnPrimaryCN(PGconn* conn, txn_info* txn, int cnid, bool enable_gtm_free)
{
    TXN_STATUS tmp;
    tmp = getTxnStatus(conn, txn->cn_xid, cnid, true);

    if (txn->txn_stat[cnid] == TXN_STATUS_INITIAL)
        txn->txn_stat[cnid] = tmp;

    if (txn->txn_stat[cnid] == TXN_STATUS_PREPARED) {
        /* if more clear status */
        if (tmp != TXN_STATUS_UNKNOWN) {
            txn->txn_stat[cnid] = tmp;
        } else {
            txn->is_exec_cn_prepared = true;
        }
    }

    if (tmp == TXN_STATUS_UNCONNECT || tmp == TXN_STATUS_UNKNOWN) {
        return false;
    }
    return true;
}

/* Get txn status from other nodes, called by getTxnInfoOnOtherNodes() */
static void getTxnStatOnOtherNodes(PGconn* conn, txn_info* txn, int cnid, bool enable_gtm_free)
{
    TXN_STATUS tmp;
    int ii;
    /* gtm branch */
    if (!enable_gtm_free && (gtm_option_num == 0)) {
        for (ii = 0; ii < pgxc_clean_node_count; ii++) {
            tmp = getTxnStatus(conn, txn->localxid, ii, (ii == cnid));
            if (tmp == TXN_STATUS_COMMITTED || tmp == TXN_STATUS_ABORTED || tmp == TXN_STATUS_RUNNING) {
                txn->txn_stat[ii] = tmp;
                break;
            }
            if (tmp == TXN_STATUS_UNCONNECT) {
                txn->txn_stat[ii] = tmp;
            }
        }
    } else if (txn->new_version) {
        /* new_gid_version, gtm lite or gtm free */
        for (ii = 0; ii < pgxc_clean_node_count; ii++) {
            /* every one has next member's xid, try to get txn from next member */
            int next_node_idx = txn->txn_gid_info[ii].next_node_idx;
            GlobalTransactionId next_node_xid = txn->txn_gid_info[ii].next_node_xid;
            if (next_node_xid == 0) {
                continue;
            }
            tmp = getTxnStatus(conn, next_node_xid, next_node_idx, (next_node_idx == cnid));
            if (tmp == TXN_STATUS_COMMITTED || tmp == TXN_STATUS_ABORTED || tmp == TXN_STATUS_RUNNING) {
                txn->txn_stat[next_node_idx] = tmp;
                break;
            }
            if (tmp == TXN_STATUS_UNCONNECT) {
                txn->txn_stat[next_node_idx] = tmp;
            }
            /* every one has the same next_node_idx, just fetch once */
            if (txn->new_ddl_version) {
                break;
            }
        }
    }
}

static void getTxnInfoOnOtherNodes(PGconn* conn, txn_info* txn)
{
    int ii;
    int cnid = -1;
    bool found = false;
    bool enable_gtm_free = (strcmp(gtm_mode, "on") == 0);
    bool is_cn_deleted = true; /* cn deleted, cn unconnected have different meaning */

    if (txn->cn_xid != 0) {
        for (ii = 0; ii < pgxc_clean_node_count; ii++) {
            /* always reference matched coordiantor */
            if (strcmp(pgxc_clean_node_info[ii].node_name, txn->cn_nodename) == 0) {
                cnid = ii;
                found = true;
                is_cn_deleted = false;
                break;
            }
        }

        /* get txn status from primary cn node*/
        if (found) {
            found = getTxnStatOnPrimaryCN(conn, txn, cnid, enable_gtm_free);
        }
    }

    /* try get txn status from other nodes*/
    /*
     * if enable_gtm_free is true, we cannot do anything to xact, so set status to  unconnect,
     * otherwise try get txn status from other nodes.
     * only do below logic if exec cn deleted, if cn is unconneted, we do nothing.
     */
    if (is_cn_deleted) {
        getTxnStatOnOtherNodes(conn, txn, cnid, enable_gtm_free);
        write_stderr(
            "%s Could not get the status of prepared transaction %s from node %s,"
            " current gtm_option: %s, option_num: %d.\n",
            formatLogTime(),
            txn->gid,
            txn->cn_nodename,
            transfor_gtm_optoin(gtm_option, gtm_mode),
            gtm_option_num);
    }
}

static void getTxnInfoOnOtherNodesForDatabase(CleanWorkerInfo* wkinfo, database_info* database)
{
    txn_info* cur_txn = NULL;
    PGconn* conn = wkinfo->conn;
    int work_index = wkinfo->work_job;
    for (cur_txn = database->all_head_txn_info[work_index]; cur_txn != NULL; cur_txn = cur_txn->next) {
        getTxnInfoOnOtherNodes(conn, cur_txn);
    }
}

static void getTxnInfoOnOtherNodesAll(CleanWorkerInfo* wkinfo)
{
    database_info* cur_database = NULL;
    for (cur_database = head_database_info; cur_database != NULL; cur_database = cur_database->next) {
        getTxnInfoOnOtherNodesForDatabase(wkinfo, cur_database);
    }
}

static void form_get_prepared_list_sql(char* stmt, int len, bool fetch_all, int idx)
{
    int rc;
    /* SQL Statement */
    static const char* SQL_TEMP1_FETCHALL = "SELECT TRANSACTION, GID, OWNER, DATABASE, NODE_NAME "
                                            "FROM GET_GLOBAL_PREPARED_XACTS;";
    static const char* SQL_TEMP2_FETCHALL = "SELECT TRANSACTION, GID, OWNER, DATABASE, NODE_NAME "
                                            "FROM GET_GLOBAL_PREPARED_XACTS WHERE GID LIKE 'T%%_%s';";
    static const char* SQL_TEMP1 = "EXECUTE DIRECT ON (%s) 'SELECT TRANSACTION, GID, OWNER,"
                                   "DATABASE FROM PG_PREPARED_XACTS;'";
    static const char* SQL_TEMP2 = "EXECUTE DIRECT ON (%s) 'SELECT TRANSACTION, GID, OWNER,"
                                   "DATABASE FROM PG_PREPARED_XACTS WHERE GID LIKE ''T%%_%s'';'";
    if (fetch_all == true) {
        if (rollback_cn_name != NULL) {
            rc = sprintf_s(stmt, len, SQL_TEMP2_FETCHALL, rollback_cn_name);
        } else {
            rc = sprintf_s(stmt, len, SQL_TEMP1_FETCHALL);
        }
    } else {
        if (rollback_cn_name != NULL) {
            rc = sprintf_s(stmt, len, SQL_TEMP2, pgxc_clean_node_info[idx].node_name, rollback_cn_name);
        } else {
            rc = sprintf_s(stmt, len, SQL_TEMP1, pgxc_clean_node_info[idx].node_name);
        }
    }
    securec_check_ss_c(rc, "\0", "\0");
}
/*
 * first we try to fetch all infomations once time using PGXC_GET_PREPARED_XACTS,
 * If fails, we fetch result one by one.
 */
static bool getPreparedTxnListOfNode(PGconn* conn, bool fetch_all, int idx = 0)
{
#define MAX_STMT_LEN 1024
    char stmt[MAX_STMT_LEN];
    int prep_txn_count;
    int ii;
    PGresult* res = NULL;
    ExecStatusType pq_status;

    /* form query of getting prepared txn list */
    form_get_prepared_list_sql(stmt, MAX_STMT_LEN, fetch_all, idx);
    res = PQexec(conn, stmt);
    /* ignore this node */
    if (res == NULL || (pq_status = PQresultStatus(res)) != PGRES_TUPLES_OK) {
        if (fetch_all == true) {
            write_stderr("%s Could not obtain prepared transaction list once time.(%s)\n",
                formatLogTime(),
                res != NULL ? PQresultErrorMessage(res) : "");
        } else {
            write_stderr("%s Could not obtain prepared transaction list for node %s.(%s)\n",
                formatLogTime(),
                pgxc_clean_node_info[idx].node_name,
                res != NULL ? PQresultErrorMessage(res) : "");
        }
        PQclear(res);
        return false;
    }
    prep_txn_count = PQntuples(res);
    for (ii = 0; ii < prep_txn_count; ii++) {
        GlobalTransactionId localxid;
        char* gid = NULL;
        char* owner = NULL;
        char* database_name = NULL;
        char* node_name = NULL;

        localxid = atoxid(PQgetvalue(res, ii, 0));
        gid = pg_strdup(PQgetvalue(res, ii, 1));
        owner = pg_strdup(PQgetvalue(res, ii, 2));
        database_name = pg_strdup(PQgetvalue(res, ii, 3));
        node_name = fetch_all ? (pg_strdup(PQgetvalue(res, ii, 4))) : pgxc_clean_node_info[idx].node_name;

        add_txn_info(database_name, node_name, localxid, gid, owner, TXN_STATUS_PREPARED);
        /* free resource, these variables have been checked for not NULL */
        free(gid);
        free(owner);
        free(database_name);
        if (fetch_all)
            free(node_name);
    }
    PQclear(res);
    return true;
}

static void getPreparedTxnList(PGconn* conn)
{
    if (getPreparedTxnListOfNode(conn, true)) {
        write_stderr("%s get prepared transaction on all nodes\n", formatLogTime());
    } else {
        /* something wrong in some node, fetch information in nodes one by one */
        for (int ii = 0; ii < pgxc_clean_node_count; ii++) {
            if (getPreparedTxnListOfNode(conn, false, ii))
                write_stderr(
                    "%s get prepared transaction on node(%s)\n", formatLogTime(), pgxc_clean_node_info[ii].node_name);
        }
    }
}

static void getDatabaseList(PGconn* conn)
{
    int database_count;
    int ii;
    PGresult* res = NULL;
    char* dbname = NULL;

    /* SQL Statement */
    static const char* STMT_GET_DATABASE_LIST = "SELECT DATNAME FROM PG_DATABASE;";

    /*
     * Get database list
     */
    res = PQexec(conn, STMT_GET_DATABASE_LIST);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not obtain database list.\n", formatLogTime());
        PQclear(res);
        exit(1);
    }
    database_count = PQntuples(res);
    for (ii = 0; ii < database_count; ii++) {
        dbname = PQgetvalue(res, ii, 0);
        if (strcmp(dbname, "template0") == 0 || strcmp(dbname, "template1") == 0) {
            /* Skip template0 and template1 database */
            continue;
        }
        add_database_info(dbname);
    }
    PQclear(res);
}

static void add_temp_schema_info(char* temp_schema_name)
{
    tempschema_info* rv = NULL;

    rv = (tempschema_info*)malloc(sizeof(tempschema_info));
    checkAllocMem(rv);

    rv->next = NULL;
    rv->tempschema_name = strdup(temp_schema_name);
    checkAllocMem(rv->tempschema_name);
    if (g_log_level == DEBUG)
        write_stderr("%s add temp schema: %s\n", formatLogTime(), temp_schema_name);

    if (temp_schema_info == NULL)
        temp_schema_info = rv;
    else {
        rv->next = temp_schema_info->next;
        temp_schema_info->next = rv;
    }
}

static void add_active_backend_info(int64 sessionID, uint32 tempID, uint32 timeLineID)
{
    activebackend_info* rv = NULL;

    rv = (activebackend_info*)malloc(sizeof(activebackend_info));
    checkAllocMem(rv);

    rv->next = NULL;
    rv->sessionID = sessionID;
    rv->tempID = tempID;
    rv->timeLineID = timeLineID;

    if (g_log_level == DEBUG)
        write_stderr("%s add backendID: %lld\n", formatLogTime(), (long long int)sessionID);

    if (active_backend_info == NULL)
        active_backend_info = rv;
    else {
        rv->next = active_backend_info->next;
        active_backend_info->next = rv;
    }
}

/*
 * @Description: get temp schema list of a random DN.
 * @param[IN] conn:  node connection handle
 * @return: int - the random datanode number, -1 means failure.
 */
static int getTempSchemaListOnDN(PGconn* conn)
{
    if (pgxc_clean_datanode_count == 0) {
        /* No active datanode, just return */
        return -1;
    }

    const char* STMT_GET_RANDOM = "SELECT RANDOM();";
    PGresult* res = NULL;
    double random_value;
    int random_datanode_no;

    for (;;) {
        res = PQexec(conn, STMT_GET_RANDOM);
        if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
            write_stderr("%s Could not execute query.\n", formatLogTime());
            PQclear(res);
            exit(1);
        }

        random_value = atof(PQgetvalue(res, 0, 0));
        random_datanode_no = (int)(pgxc_clean_node_count * random_value);

        PQclear(res);

        if (pgxc_clean_node_info[random_datanode_no].type == NODE_TYPE_DATANODE) {
            if (g_log_level == DEBUG)
                write_stderr("%s starting get temp schema on %s\n",
                    formatLogTime(),
                    pgxc_clean_node_info[random_datanode_no].node_name);

            getTempSchemaOnOneDN(conn, random_datanode_no);
            break;
        }
    }

    return random_datanode_no;
}

/*
 * @Description: get temp schemas' name on a random DN. This is to avoid node
 *				replacement/xc_maintenance_mode causing CN/DN temp schema inconsistency. When
 *				inconsistency, we cann't get the temp schema name on CN, so we
 *				must get it from DN directly.
 *
 * @param[IN] conn:  node connection handle
 * @return: void
 */
static void getTempSchemaOnOneDN(PGconn* conn, int no)
{
    char STMT_GET_DN_TEMP_SCHEMA[NAMEDATALEN * 3 + 128] = {0};
    PGresult* res = NULL;
    int i;
    int temp_schema_count;
    char* nspname = NULL;
    int rc;

    /*
     * Here we handle two scenarios:
     * 1. The coordinator is replaced because of old coodinator broken, so we can only find temp schemas
          on datanode;
      2. The DN is doing build, and the build uses temp table, then the DN is killed, so the temp schmea
          will be exist on this DN only.
     *
     * Note: we get all temp schemas like 'pg_temp_%' (include temp schemas:
     *         a. created on all alive CN;
     *         b. created on current DN
     *         c. created on dropped CN
     * ).
     */
    rc = snprintf_s(STMT_GET_DN_TEMP_SCHEMA,
        sizeof(STMT_GET_DN_TEMP_SCHEMA),
        sizeof(STMT_GET_DN_TEMP_SCHEMA) - 1,
        "EXECUTE DIRECT on (%s) 'SELECT * FROM PG_NAMESPACE WHERE NSPNAME LIKE ''pg_temp_%%''';",
        pgxc_clean_node_info[no].node_name);
    securec_check_ss_c(rc, "\0", "\0");

    res = PQexec(conn, STMT_GET_DN_TEMP_SCHEMA);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not execute query.\n", formatLogTime());
        PQclear(res);
        exit(1);
    }

    temp_schema_count = PQntuples(res);

    for (i = 0; i < temp_schema_count; i++) {
        nspname = PQgetvalue(res, i, 0);
        add_temp_schema_info(nspname);
    }

    PQclear(res);
}

static void getTempSchemaListOnCN(PGconn* conn)
{
    int temp_schema_count;
    int i;
    PGresult* res = NULL;
    char* nspname = NULL;
    int rc;

    char STMT_GET_TEMP_SCHEMA_LIST[NAMEDATALEN + 128] = {0};

    rc = snprintf_s(STMT_GET_TEMP_SCHEMA_LIST,
        sizeof(STMT_GET_TEMP_SCHEMA_LIST),
        sizeof(STMT_GET_TEMP_SCHEMA_LIST) - 1,
        "SELECT NSPNAME FROM PG_NAMESPACE WHERE NSPNAME LIKE 'pg_temp_%s_%%'",
        my_nodename);
    securec_check_ss_c(rc, "\0", "\0");

    if (g_log_level == DEBUG)
        write_stderr("%s start getting temp schema on %s\n", formatLogTime(), my_nodename);

    /*
     * Get database list
     */
    res = PQexec(conn, STMT_GET_TEMP_SCHEMA_LIST);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not obtain temp schema list.\n", formatLogTime());
        PQclear(res);
        exit(1);
    }
    temp_schema_count = PQntuples(res);

    for (i = 0; i < temp_schema_count; i++) {
        nspname = PQgetvalue(res, i, 0);
        if (strchr(&nspname[7], '_') == NULL) {
            write_stderr("%s Error when parse schema name.\n", formatLogTime());
        }
        if (strncasecmp(my_nodename, &nspname[7], strchr(&nspname[7], '_') - &nspname[7]) == 0)
            add_temp_schema_info(nspname);
    }

    PQclear(res);
}

/*
 * @Description: get active backend list, to avoid deleting active temp namespace.
 *	There are two scenarios:
 *		1. The temp schema name is gotten from CN, then we should get active threadid
 *			list directly by select pg_stat_activity from CN.
 *		2. The temp schema name is gotten from DN, then the temp schema may be
 *			created by CN, or datanode itself(for example, in maintenance mode), then
 *			we should first do step1, and then get active threadid list by
 *			'executing direct on datanode'.
 *
 * @param[IN] conn:  node connection handle
 * @param[IN] datanode_no:  datanode number. -1 if temp schema is gotten from CN,
 *									actual DN no if temp schema is gotten from DN.
 * @return: void
 */
static void getActiveBackendListOnCN(PGconn* conn, int datanode_no)
{
#define MAX_SQL_LEN (2 * NAMEDATALEN + 128)
    int rc;

    /* SQL Statement */
    char STMT_ACTIVE_BACKEND_LIST[MAX_SQL_LEN] = {0};

    if (datanode_no != -1) {
        rc = sprintf_s(STMT_ACTIVE_BACKEND_LIST,
            MAX_SQL_LEN,
            "EXECUTE DIRECT ON (%s) 'SELECT SESSIONID, TEMPID, TIMELINEID FROM PG_DATABASE D, "
            "PG_STAT_GET_ACTIVITY_FOR_TEMPTABLE() AS S WHERE S.DATID = D.OID AND D.DATNAME = ''%s'''",
            pgxc_clean_node_info[datanode_no].node_name,
            PQdb(conn));
        securec_check_ss_c(rc, "\0", "\0");
        getActiveBackendList(conn, STMT_ACTIVE_BACKEND_LIST);
    }

    rc = sprintf_s(STMT_ACTIVE_BACKEND_LIST,
        MAX_SQL_LEN,
        "SELECT SESSIONID, TEMPID, TIMELINEID FROM PG_DATABASE D, PG_STAT_GET_ACTIVITY_FOR_TEMPTABLE() AS S WHERE "
        "S.DATID = D.OID AND D.DATNAME = '%s'",
        PQdb(conn));
    securec_check_ss_c(rc, "\0", "\0");

    getActiveBackendList(conn, STMT_ACTIVE_BACKEND_LIST);
}
/*
 * @Description: execute the given query to get active backend id list from CN
 * @param[IN] conn:  node connection handle
 * @param[IN] query: get cn-created temp namespace's backend id or dn-created temp namespace's backend id,
                     using different sql query.
 * @return: void
 */
static void getActiveBackendList(PGconn* conn, const char* query)
{
    int active_backend_count;
    int i;
    PGresult* res = NULL;
    int64 sessionID;
    uint32 tempID;
    uint32 timeLineID;

    /*
     * Get database list
     */
    res = PQexec(conn, query);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not obtain active backend thread id list.\n", formatLogTime());
        PQclear(res);
        exit(1);
    }
    active_backend_count = PQntuples(res);

    for (i = 0; i < active_backend_count; i++) {
        char* result = PQgetvalue(res, i, 0);
        sessionID = atoll(result);
        result = PQgetvalue(res, i, 1);
        tempID = atoi(result);
        result = PQgetvalue(res, i, 2);
        timeLineID = atoi(result);
        (void)add_active_backend_info(sessionID, tempID, timeLineID);
    }

    PQclear(res);
}

static void checkAllocMem(void* p)
{
    if (p == NULL) {
        write_stderr("%s No more memory, FILE:%s, LINE:%d.\n", formatLogTime(), __FILE__, __LINE__);
        exit(1);
    }
}

static void getNodeList(PGconn* conn)
{
    int ii;
    PGresult* res = NULL;

    /* SQL Statement */
    static const char* STMT_GET_NODE_INFO = "SELECT NODE_NAME, NODE_TYPE, NODE_PORT, NODE_HOST FROM PGXC_NODE WHERE "
                                            "NODE_TYPE IN ('C','D') AND nodeis_active = true;";

    res = PQexec(conn, STMT_GET_NODE_INFO);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr("%s Could not obtain node list.\n", formatLogTime());
        goto error_exit;
    }
    pgxc_clean_node_count = PQntuples(res);
    pgxc_clean_datanode_count = 0;
    pgxc_clean_node_info = (node_info*)calloc(pgxc_clean_node_count, sizeof(node_info));
    if (pgxc_clean_node_info == NULL) {
        write_stderr("%s No more memory.\n", formatLogTime());
        goto error_exit;
    }

    for (ii = 0; ii < pgxc_clean_node_count; ii++) {
        char* node_name = NULL;
        char* node_type_c = NULL;
        NODE_TYPE node_type;
        int port;
        char* host = NULL;

        node_name = pg_strdup(PQgetvalue(res, ii, 0));
        node_type_c = pg_strdup(PQgetvalue(res, ii, 1));
        switch (node_type_c[0]) {
            case 'C':
                /* pgxc_clean has to connect to the Coordinator */
                node_type = NODE_TYPE_COORD;
                if (strcmp(node_name, my_nodename) == 0) {
                    my_nodeidx = ii;
                }
                break;
            case 'D':
                node_type = NODE_TYPE_DATANODE;
                pgxc_clean_datanode_count++;
                break;
            default:
                write_stderr("%s Invalid catalog data (node_type), node_name: %s, node_type: %s\n",
                    formatLogTime(),
                    node_name,
                    node_type_c);
                /* free memory before exit */
                if (node_name != NULL)
                    free(node_name);
                if (node_type_c != NULL)
                    free(node_type_c);
                goto error_exit;
        }
        port = atoi(PQgetvalue(res, ii, 2));
        host = pg_strdup(PQgetvalue(res, ii, 3));

        bool set_node_info_fail = (set_node_info(node_name, port, host, node_type, ii) == -1);
        /* free memory before check result */
        if (node_name != NULL)
            free(node_name);
        if (node_type_c != NULL)
            free(node_type_c);
        if (host != NULL)
            free(host);
        if (set_node_info_fail) {
            goto error_exit;
        }
    }
    /* Check if local Coordinator has been found */
    if (my_nodeidx == -1) {
        write_stderr("%s Failed to identify the coordinator which %s is connecting to.  ", formatLogTime(), progname);
        write_stderr("%s Connecting to a wrong node.\n", formatLogTime());
        goto error_exit;
    }

    PQclear(res);
    return;

error_exit:
    PQclear(res);
    exit(1);
}

static char* transfor_gtm_optoin(const char* option, const char* gtm_free_mode)
{
    if (strcmp(option, "0") == 0 && strcmp(gtm_free_mode, "off") == 0) {
        return "GTM";
    } else if (strcmp(option, "1") == 0 && strcmp(gtm_free_mode, "off") == 0) {
        return "GTM_LITE";
    } else if (strcmp(option, "2") == 0 || strcmp(gtm_free_mode, "on") == 0) {
        return "GTM_FREE";
    } else {
        return "UnKNown";
    }
}

static void showVersion(void)
{
    puts("gs_clean " DEF_GS_VERSION);
}

static void add_to_database_list(char* dbname)
{
    if (head_database_names == NULL) {
        head_database_names = last_database_names = (database_names*)malloc(sizeof(database_names));
        if (head_database_names == NULL) {
            write_stderr("%s No more memory, FILE:%s, LINE:%d.\n", formatLogTime(), __FILE__, __LINE__);
            exit(1);
        }
    } else {
        last_database_names->next = (database_names*)malloc(sizeof(database_names));
        if (last_database_names->next == NULL) {
            write_stderr("%s No more memory, FILE:%s, LINE:%d.\n", formatLogTime(), __FILE__, __LINE__);
            exit(1);
        }
        last_database_names = last_database_names->next;
    }
    last_database_names->next = NULL;
    last_database_names->database_name = dbname;
}

typedef struct {
    const char* level_name_upper;
    const char* level_name_lower;
    LogLevel log_level;
} LevelMap;

static void set_log_level(const char* level_name)
{
    static LevelMap log_levels[] = {{"LOG", "log", LOG},
                                    {"DEBUG", "debug", DEBUG},
                                    {NULL, NULL, LOG}};

    for (int i = 0; log_levels[i].level_name_upper != NULL; i++) {
        if (level_name != NULL && (strcmp(log_levels[i].level_name_upper, level_name) == 0 ||
            strcmp(log_levels[i].level_name_lower, level_name) == 0)) {
            g_log_level = log_levels[i].log_level;
            return;
        }
    }

    write_stderr("%s Unexpected log level %s, use DEBUG or LOG.\n", formatLogTime(), level_name);
    exit(1);
}

static void parse_pgxc_clean_options(int argc, char* argv[])
{
    static struct option long_options[] = {{"all", no_argument, NULL, 'a'},
        {"commit", no_argument, NULL, 'c'},
        {"dbname", required_argument, NULL, 'd'},
        {"host", required_argument, NULL, 'h'},
        {"loglevel", required_argument, NULL, 'l'},
        {"node", required_argument, NULL, 'n'},
        {"no-clean", no_argument, NULL, 'N'},
        {"port", required_argument, NULL, 'p'},
        {"quiet", no_argument, NULL, 'q'},
        {"rollback", no_argument, NULL, 'r'},
        {"rollback-cnname", required_argument, NULL, 'R'},
        {"timeout", required_argument, NULL, 't'},
        {"username", required_argument, NULL, 'U'},
        {"verbose", no_argument, NULL, 'v'},
        {"version", no_argument, NULL, 'V'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", required_argument, NULL, 'W'},
        {"help", no_argument, NULL, '?'},
        {"status", no_argument, NULL, 's'},
        {"exclusive", no_argument, NULL, 'e'},
        {"cleaning", no_argument, NULL, 'C'},
        {"Exclusive", no_argument, NULL, 'E'},
        {"jobs", required_argument, NULL, 'j'},
        {NULL, 0, NULL, 0}};

    int optindex;
    extern char* optarg;
    extern int optind;
    int c;
    errno_t rc = EOK;

    progname = get_progname(argv[0]); /* Should be more fancy */

    while ((c = getopt_long(argc, argv, "acd:h:l:n:Np:qrR:t:U:vVwW:seCEj:?", long_options, &optindex)) != -1) {
        switch (c) {
            case 'a':
                clean_all_databases = true;
                break;
            case 'c':
                commit_all_prepared = true;
                break;
            case 'd':
                check_env_name_c(optarg);
                add_to_database_list(optarg);
                break;
            case 'h':
                check_env_value_c(optarg);
                coordinator_host = optarg;
                break;
            case 'l':
                check_env_name_c(optarg);
                set_log_level(optarg);
                break;
            case 'n':
                check_env_value_c(optarg);
                target_node = optarg;
                break;
            case 'N':
                no_clean_opt = true;
                break;
            case 'p':
                check_env_value_c(optarg);
                if (optarg != NULL) {
                    coordinator_port = strtolSafe(optarg, 10);
                }
                break;
            case 'q':
                verbose_opt = false;
                break;
            case 'r':
                rollback_all_prepared = true;
                break;
            case 'R':
                check_env_value_c(optarg);
                rollback_cn_name = optarg;
                break;
            case 't':
                check_env_value_c(optarg);
                connect_timeout = optarg;
                break;
            case 'U':
                check_env_name_c(optarg);
                username = optarg;
                break;
            case 'V':
                version_opt = 0;
                break;
            case 'v':
                verbose_opt = true;
                break;
            case 'w':
                try_password_opt = TRI_NO;
                break;
            case 'W':
                try_password_opt = TRI_YES;
                if (optarg != NULL) {
                    password = pg_strdup(optarg);
                    rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                    securec_check_c(rc, "\0", "\0");
                } else {
                    password = NULL;
                }
                break;
            case 's':
                status_opt = true;
                break;
            case 'e':
                clean_temp_schema_only = true;
                break;
            case 'C':
                clean_plan_table_only = true;
                break;
            case 'E':
                clean_plan_table_only = false;
                clean_temp_schema_only = false;
                no_clean_temp_schema_tbspc = true;
                break;
            case 'j':
                check_env_value_c(optarg);
                g_gs_clean_worker_num = atoi(optarg);
                if (g_gs_clean_worker_num < MIN_GS_CLEAN_WORK_NUM || g_gs_clean_worker_num > MAX_GS_CLEAN_WORK_NUM) {
                    write_stderr("ERROR: parallel worker number out of range(%d ~ %d).\n",
                        MIN_GS_CLEAN_WORK_NUM,
                        MAX_GS_CLEAN_WORK_NUM);
                    exit(1);
                }
                break;
            case '?':
                if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0) {
                    usage();
                    exit(0);
                } else {
                    write_stderr("%s Try \"%s --help\" for more information.\n", formatLogTime(), progname);
                    exit(1);
                }
                break;
            default:
                write_stderr("%s Try \"%s --help\" for more information.\n", formatLogTime(), progname);
                exit(1);
                break;
        }
    }

    while (argc - optind >= 1) {
        if (head_database_names == NULL) {
            if (strcmp(argv[optind], "template0") == 0) {
                write_stderr("%s %s: You should not clean template0 database.\n", formatLogTime(), progname);
                exit(1);
            }
            add_to_database_list(argv[optind]);
        }
        if (username == NULL) {
            username = argv[optind];
        } else {
            write_stderr("%s %s: warning: extra command-line argument \"%s\" ignored\n",
                formatLogTime(),
                progname,
                argv[optind]);
        }
        optind++;
    }

    if (!clean_all_databases && head_database_names == NULL) {
        write_stderr("%s Please specify at least one database or -a for all\n", formatLogTime());
        exit(1);
    }
    if (commit_all_prepared && rollback_all_prepared) {
        write_stderr(" Parameter -c and -r cannot be used in one time,\n"
                     " because we cannot both commit and rollback one transaction\n");
        exit(1);
    }
}

static bool setMaintenanceMode(PGconn* conn, bool on)
{
    PGresult* res = NULL;
    ExecStatusType res_status;

    res = on ? PQexec(conn, "SET xc_maintenance_mode = on;") : PQexec(conn, "SET xc_maintenance_mode = off;");
    res_status = PQresultStatus(res);
    if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK) {
        PQclear(res);
        return true;
    } else
        write_stderr("%s Failed to set xc_maintenance_mode. (%s)\n", formatLogTime(), PQresultErrorMessage(res));

    PQclear(res);
    return false;
}

static bool setEnableParallelDDL(PGconn* conn, bool on)
{
    PGresult* res = NULL;
    ExecStatusType res_status;

    res = on ? PQexec(conn, "SET enable_parallel_ddl = on;") : PQexec(conn, "SET enable_parallel_ddl = off;");
    res_status = PQresultStatus(res);
    if (res_status == PGRES_COMMAND_OK || res_status == PGRES_TUPLES_OK) {
        PQclear(res);
        return true;
    } else
        write_stderr("%s Failed to set enable_parallel_ddl. (%s)\n", formatLogTime(), PQresultErrorMessage(res));

    PQclear(res);
    return false;
}

static char* formatLogTime()
{
    time_t now = time(NULL);
    struct tm* nowtm = localtime(&now);
    if (nowtm == NULL) {
        return "invalid local time";
    }
    strftime(m_formatted_log_time, FORMATTED_TS_LEN, "%Y-%m-%d %H:%M:%S ", nowtm);
    m_formatted_log_time[FORMATTED_TS_LEN - 1] = '\0';
    return m_formatted_log_time;
}

static void usage(void)
{
    char* env = NULL;

    printf("gs_clean cleans up outstanding 2PCs after failed node is recovered.\n"
           "Usage:\n"
           "gs_clean [OPTION ...] [DBNAME [USERNAME]]\n\n"
           "Options:\n");

    printf("  -a, --all                cleanup all the databases available.\n");
    printf("  -c, --commit             commit all prepared transaction.\n");
    printf("  -d, --dbname=DBNAME      database name to clean up.\n");
    env = getenv("PGHOST");
    check_env_value_c(env);
    printf("  -h, --host=HOSTNAME      target coordinator host address, (default: \"%s\")\n",
        env != NULL ? env : "local socket");
    printf("  -l, --loglevel=level     set log level, current supports DEBUG/LOG\n");
    printf("  -n, --node=NODENAME      only recover prepared transaction on this node\n");
    printf("  -N, --no-clean           only collect 2PC information.  Do not recover them\n");
    env = getenv("PGPORT");
    check_env_value_c(env);
    printf("  -p, --port=PORT          port number of the coordinator (default: \"%s\")\n",
        env != NULL ? env : DEF_PGPORT_STR);
    printf("  -q, --quiet              quiet mode.  do not print anything but error information.\n");
    printf("  -r, --rollback           rollback all prepared transaction.\n");
    printf("  -R, --rollback-cnname    rollback all prepared transaction when CN is disconnected.\n");
    printf("  -s, --status             prints out 2PC status\n");
    printf("  -t, --timeout=SECS       seconds to wait when attempting connection, 0 disables (default: %s)\n",
        DEFAULT_CONNECT_TIMEOUT);
    printf("  -U, --username=USERNAME  database user name.\n");
    printf("  -v, --verbose            print recovery information.\n");
    printf("  -V, --version            prints out the version.\n");
    printf("  -w, --no-password        never prompt for the password.\n");
    printf("  -W, --password=PASSWORD  prompt for the password.\n");
    printf("  -e, --exclusive          only clean temp table and remove clean's timeout\n");
    printf("  -C, --clean              only clean plan_table data and clean's timeout\n");
    printf("  -E, --Exclusive          only clean 2pc transaction. \n");
    printf("  -j, --jobs	           parallel number of jobs to do the 2PC clean (default: %d, range: %d ~ %d). \n",
        DEFAULT_GS_CLEAN_WORK_NUM,
        MIN_GS_CLEAN_WORK_NUM,
        MAX_GS_CLEAN_WORK_NUM);
    printf("  -?, --help               print this message.\n");
}

