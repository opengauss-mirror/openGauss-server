/*-------------------------------------------------------------------------
 *
 * pg_regress --- regression test driver
 *
 * This is a C implementation of the previous shell script for running
 * the regression tests, and should be mostly compatible with it.
 * Initial author of C translation: Magnus Hagander
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/regress/pg_regress.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_regress.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <arpa/inet.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#endif
#include <assert.h>

#include "getopt_long.h"
#include "pg_config_paths.h"

#define DATANODE 1
#define COORD 2
#define GTM 3
#define SHELL 4

#ifdef BUILD_BY_CMAKE
char* code_base_src = NULL;
char* current_exe_dir = NULL;
char* CMAKE_PGBINDIR = NULL;
char* CMAKE_LIBDIR = NULL;
char* CMAKE_PGSHAREDIR = NULL;

void get_value_from_env(char** out_dir, char* env_name)
{
    char *p;
    if ((p = getenv(env_name))) {
        *out_dir = strdup(p);
    } else {
        fprintf(stderr, _("\nERROR: could not get %s env\n"), env_name);
        exit_nicely(2);
    }
}

void get_value_from_cwd(char** out_dir)
{
    if ((*out_dir = getcwd(NULL, 0)) == NULL)
    {
        fprintf(stderr, _("\n: could not get value from getcwd"));
        exit_nicely(2);
    }
}
#endif

#ifdef PGXC
/*
 * In Postgres-XC, a regression test check is run on 2 Coordinators
 * and 2 Datanodes. Coordinator 1 is considered as being the default
 * used in pg_regress.
 * External connections are made to Coordinator 1.
 * All connections to remote nodes are made from Coordinator 1.
 * Here is the list of nodes identified with a unique ID.
 */
typedef enum { PGXC_COORD_1, PGXC_COORD_2, PGXC_DATANODE_1, PGXC_DATANODE_2, PGXC_GTM } PGXCNodeTypeNum;
#endif

pgxc_node_info myinfo;

/* for resultmap we need a list of pairs of strings */
typedef struct _resultmap {
    char* test;
    char* type;
    char* resultfile;
    struct _resultmap* next;
} _resultmap;

/* for parallel initdb */
typedef struct thread_desc {
    int thn;        /* how may threads to spawn */
    int thi;        /* current thread counter */
    pthread_t* thd; /* pthread_t array */
    char** args;    /* arguments for each thread */
} thread_desc;
/*
 * Values obtained from pg_config_paths.h and Makefile.  The PG installation
 * paths are only used in temp_install mode: we use these strings to find
 * out where "make install" will put stuff under the temp_install directory.
 * In non-temp_install mode, the only thing we need is the location of psql,
 * which we expect to find in psqldir, or in the PATH if psqldir isn't given.
 *
 * XXX Because pg_regress is not installed in bindir, we can't support
 * this for relocatable trees as it is.  --psqldir would need to be
 * specified in those cases.
 */
char* bindir = PGBINDIR;
char* libdir = LIBDIR;
char* datadir = PGSHAREDIR;
char* host_platform = HOST_TUPLE;
char* top_builddir = NULL;

#ifndef WIN32_ONLY_COMPILER
static char* makeprog = MAKEPROG;
#endif

#ifndef WIN32 /* not used in WIN32 case */
static char* shellprog = SHELLPROG;
#endif

/*
 * On Windows we use -w in diff switches to avoid problems with inconsistent
 * newline representation.	The actual result files will generally have
 * Windows-style newlines, but the comparison files might or might not.
 */
#ifndef WIN32
const char* basic_diff_opts = "";
const char* pretty_diff_opts = "-C3";
#else
const char* basic_diff_opts = "-w";
const char* pretty_diff_opts = "-w -C3";
#endif

#define LOCAL_IP_LEN 20

#define REGR_MAX_NUM_OF_LINE_BUFF 12
#define REGR_MAX_PARALLEL_TESTS 200
#define REGR_MAX_SCH_LINE_SIZE (32 * 1024)

/* Maximum length for the time string */
#define MAX_REG_TIME_STR_LEN 25

/* Maximum token length */
#define MAX_REGR_TOKEN_LEN 25
#define MAX_ADD_DATA_LEN 27

double g_dTotalTime;
double g_dGroupTotalTime;
char g_acTestStartTime[MAX_REG_TIME_STR_LEN];
char g_acDataPath[MAXPGPATH + MAX_ADD_DATA_LEN];

#define REGR_CHR_EOS '\0'
#define REGR_MCR_SIZE_NIL 0
#define REGR_MCR_SIZE_1B 1
#define REGR_MCR_SIZE_1KB 1024
#define REGR_MCR_SIZE_1MB 1048576
#define REGR_MCR_SIZE_1GB 1073741824

char** g_ppcBuf = NULL;
unsigned int g_uiCurLineBufIdx = 0;
unsigned int g_uiTotalBuf = 0;

/* Maximum line length in regress.conf file */
#define MAX_LINE_LEN (1024)

/* Maximum length for the name of a configuration item in
 * regress.conf file */
#define MAX_CONF_ITEM_NAME_LEN (128)

/* Current number of configuration items in regress.conf file */
#define MAX_REG_CONF_ITEMS (4)

#define REGR_DELAY_FOR_PERF_COLLECTION (100000L)
#define REGR_DELAY_FOR_PID_FILE_OPEN (1000000L)
#define REGR_NUM_OF_RETRY_FOR_PID_FILE_OPEN (15)

#define REGR_PID_WAIT_FOR_ANY_CHILD (-1)

#define NO_CHILD_PROC_HAS_CHANGED_STATE (0)

#define REGR_SIZE_OF_INDEX_ELEM (sizeof(unsigned int))

/* Array index definition for items in aacRegConfItemName */
#define REGR_TUPONLY_ID 0
#define REGR_COL_SEP_ID 1
#define REGR_PERF_DATA_LOGGING_ID 2
#define REGR_DIAG_COLLECT_ID 3

#define REGR_SUCCESS 0
#define REGR_ERRCODE_MALLOC_FAILURE 1
#define REGR_ERRCODE_FOPEN_FAILURE 2
#define REGR_ERRCODE_ENV_FETCH_FAILED 3
#define REGR_ERRCODE_BUFF_NOT_ENOUGH 4
#define REGR_ERRCODE_RESRC_STAT_FETCH_FAILED 5
#define REGR_ERRCODE_SYNTAX_ERR_IN_SCH 6
#define REGR_EOF_REACHED 7
#define REGR_ERRCODE_MAX_NUM_OF_LINE_BUFF 8

/* The number by which the memory block for replacement patterns gets expanded.
 * It is also indicating the initial number of slots that is getting allocated*/
#define REGR_NUM_OF_REPLC_ITEMS_CHUNK 25

/* This is for the rough estimation of the size of the memory that need to be
 * allocated for the storage of the replacement pattern strings.
 * This shows the average size of the replacement pattern string */
#define REGR_AVG_REPLC_PATT_LEN 64

/* This is for the rough estimation of the size of the memory that need to be
 * allocated for the storage of the replacement pattern strings.
 * This shows the average size of the replacement pattern string */
#define REGR_AVG_REPLC_PATT_VAL_LEN 512

/* INDICATOR for the start of the replacement pattern string */
#define REGR_REPLC_PATT_START_CHAR '@'

/* INDICATOR for the end of the replacement pattern string */
#define REGR_REPLC_PATT_END_CHAR '@'

#define REGR_FREE(pvAddr) \
    do {                  \
        free((pvAddr));   \
        (pvAddr) = NULL;  \
    } while (0)

#define REGR_START_TIMER                                                                                 \
    {                                                                                                    \
        if (g_bEnablePerfDataPrint) {                                                                    \
            time_t timer;                                                                                \
            struct tm* pstTmInfo = NULL;                                                                 \
            (void)gettimeofday(&g_stRegrStartTime, NULL);                                                \
            (void)time(&timer);                                                                          \
            pstTmInfo = localtime(&timer);                                                               \
            if (NULL != pstTmInfo)                                                                       \
                (void)strftime(g_acTestStartTime, MAX_REG_TIME_STR_LEN, "%Y-%m-%d %H:%M:%S", pstTmInfo); \
        }                                                                                                \
    }

#define REGR_STOP_TIMER                                  \
    {                                                    \
        if (g_bEnablePerfDataPrint) {                    \
            (void)gettimeofday(&g_stRegrStopTime, NULL); \
            regrGetElapsedTime();                        \
        }                                                \
    }

#define REGR_PRINT_ELAPSED_TIME                                                 \
    {                                                                           \
        if (g_bEnablePerfDataPrint) {                                           \
            status(_("TIME START: [%s]. TIME TAKEN: [%9.3lf] %%UCPU: [%5.2lf] " \
                     "%%SCPU:  [%5.2lf] %%MEM: [%5.2lf] MEM: [%-9s]\n"),        \
                g_acTestStartTime,                                              \
                g_dTotalTime,                                                   \
                g_stResourceUsageDetails.dUAvgcpuUsage,                         \
                g_stResourceUsageDetails.dSAvgcpuUsage,                         \
                g_stResourceUsageDetails.dAvgMemUsagePct,                       \
                g_stResourceUsageDetails.acMemUsage);                           \
            status_end();                                                       \
        }                                                                       \
    }

typedef struct tagREGR_AVG_RSRCE_USAGE_STRU {
    double dUAvgcpuUsage;
    double dSAvgcpuUsage;
    double dAvgMemUsage;
    double dAvgMemUsagePct;
    long unsigned int ulCount;
    char acMemUsage[MAX_REGR_TOKEN_LEN];
} REGR_AVG_RSRCE_USAGE_STRU;

typedef struct tagREGR_RESRC_STAT_STRU {
    long unsigned int ulUtimeTicks;
    long int lCutimeTicks;
    long unsigned int ulStimeTicks;
    long int lCstimeTicks;
    long unsigned int vsize;
    long unsigned int rss;            /* virtual memory size in bytes */
    long unsigned int ulCpuTotalTime; /* Resident  Set  Size in bytes*/
    double dMemPct;                   /* %MEM */
} REGR_RESRC_STAT_STRU;

/* Stores the start time of a test file execution */
#define REG_MAX_NUM (50)

double g_dOneGroupTotalTime;
double g_dOneRegrTotalTime[REG_MAX_NUM];
char g_acOneRegrTestStartTime[REG_MAX_NUM][MAX_REG_TIME_STR_LEN];
static struct timeval g_stRegrStartTimeTmp[REG_MAX_NUM];

/* Stores the end time of a test file execution */
static struct timeval g_stRegrStopTimeTmp[REG_MAX_NUM];
#define REGR_START_TIMER_TEMP(i)                                                                                   \
    {                                                                                                              \
        if (g_bEnablePerfDataPrint) {                                                                              \
            time_t timer;                                                                                          \
            struct tm* pstTmInfo = NULL;                                                                           \
            (void)gettimeofday(&g_stRegrStartTimeTmp[i], NULL);                                                    \
            (void)time(&timer);                                                                                    \
            pstTmInfo = localtime(&timer);                                                                         \
            if (NULL != pstTmInfo)                                                                                 \
                (void)strftime(g_acOneRegrTestStartTime[i], MAX_REG_TIME_STR_LEN, "%Y-%m-%d %H:%M:%S", pstTmInfo); \
        }                                                                                                          \
    }

#define REGR_STOP_TIMER_TEMP(i)                                \
    {                                                          \
        if (g_bEnablePerfDataPrint) {                          \
            (void)gettimeofday(&g_stRegrStopTimeTmp[i], NULL); \
            regrGetOneRegrElapsedTime(i);                      \
        }                                                      \
    }

#define REGR_PRINT_ELAPSED_TIME_TEMP(i)                                \
    {                                                                  \
        if (g_bEnablePerfDataPrint) {                                  \
            status(_("TIME TAKEN: [%9.3lf]"), g_dOneRegrTotalTime[i]); \
            status_end();                                              \
        }                                                              \
    }

#define REGR_PRINT_ONEGROUP_ELAPSED_TIME                                                                 \
    {                                                                                                    \
        if (g_bEnablePerfDataPrint) {                                                                    \
            struct timeval stRegrEndTime;                                                                \
            (void)gettimeofday(&stRegrEndTime, NULL);                                                    \
            g_dOneGroupTotalTime = (stRegrEndTime.tv_sec - g_stRegrStartTimeTmp[0].tv_sec) +             \
                                   (stRegrEndTime.tv_usec - g_stRegrStartTimeTmp[0].tv_usec) * 0.000001; \
            g_dGroupTotalTime = g_dGroupTotalTime + g_dOneGroupTotalTime;                                \
            g_dOneGroupTotalTime = 0;                                                                    \
        }                                                                                                \
    }

static void regrGetOneRegrElapsedTime(int i)
{
    g_dOneRegrTotalTime[i] = (g_stRegrStopTimeTmp[i].tv_sec - g_stRegrStartTimeTmp[i].tv_sec) +
                             (g_stRegrStopTimeTmp[i].tv_usec - g_stRegrStartTimeTmp[i].tv_usec) * 0.000001;
}

REGR_RESRC_STAT_STRU g_stPrevCpuUsage;
REGR_RESRC_STAT_STRU g_stCurrCpuUsage;
REGR_AVG_RSRCE_USAGE_STRU g_stResourceUsageDetails;

/* Names of the configuration items in regress.conf file */
char aacRegConfItemName[][MAX_CONF_ITEM_NAME_LEN] = {
    "column_name_present", "column_separator", "performance_data_printing", "diagnostic_collect_on_fail"};

/* options settable from command line */
_stringlist* dblist = NULL;
bool debug = false;
char* inputdir = ".";
char* outputdir = ".";
char* psqldir = PGBINDIR;
char* launcher = NULL;
char* gausshomedir = "oldgausshome";
char* loginuser = NULL;
char pgbenchdir[MAXPGPATH];

static const int UPGRADE_GRAY_STAGE = 1;
static const int UPGRADE_GRAY_STAGE_ROLLBACK = 2;
static const int UPGRADE_GRAY_OBSERVE_STAGE = 3;
static const int UPGRADE_GRAY_OBSERVE_STAGE_ROLLBACK = 4;
static const int UPGRADE_FINASH = 5;
static _stringlist* loadlanguage = NULL;
static _stringlist* loadextension = NULL;
static int max_connections = 0;
static char* encoding = NULL;
static _stringlist* schedulelist = NULL;
static int upgrade_cn_num = 1;
static int upgrade_dn_num = 4;
static _stringlist* upgrade_schedulelist = NULL;
static _stringlist* extra_tests = NULL;
static char* pcRegConfFile = NULL;
static char* temp_install = NULL;
static char* temp_config = NULL;
static bool nolocale = false;
static bool use_existing = false;
static char* hostname = NULL;
static int port = -1;
static bool comm_tcp_mode = true;
static char* hdfshostname = "nohostname";
static char* hdfscfgpath = "noconfigpath";
static char* hdfsport = "noport";
static char* hdfsstoreplus = "nohdfsstoreplus";
/*define obs server ip and bucked name*/
static char* obshostname = "nohostname";
static char* obsbucket = "nobucket";
static char* ak = "noaccesskey";
static char* sk = "nosecretaccesskey";
static int dop = 1;

static bool port_specified_by_user = false;
static char* dlpath = PKGLIBDIR;
static char* user = NULL;
static _stringlist* extraroles = NULL;
static _stringlist* extra_install = NULL;
static FILE* start_script;
static FILE* stop_script;
/* define ai engine ip and port */
static char* g_aiehost = "nohostname";
static char* g_aieport = "noport";

static bool g_enable_segment = false;

/* internal variables */
static const char* progname;
static char* logfilename;
static FILE* logfile;
char* difffilename;
static bool clean = true;
static _resultmap* resultmap = NULL;

static bool postmaster_running = false;
static bool standby_defined = false;
static int success_count = 0;
static int fail_count = 0;
static int fail_ignore_count = 0;

static bool securitymode = false;
static bool ignore_exitcode = false;
static bool keep_run = false;

/* TRUE if diagnostic data need to be collected on test failure */
static bool g_bEnableDiagCollection = false;

/* TRUE if performance related data need to be printed,asPer the config value */
static bool g_bEnablePerfDataPrint = false;

static PID_TYPE g_iPostmasterPid = INVALID_PID;

/* Stores the start time of a test file execution */
static struct timeval g_stRegrStartTime;

/* Stores the end time of a test file execution */
static struct timeval g_stRegrStopTime;

/* To store the values of the regress.conf values */
REGR_CONF_ITEMS_STRU g_stRegrConfItems;

/* Module-global structure instance for the storing the details of the
 * replacement pattern strings */
static REGR_REPLACE_PATTERNS_STRU g_stRegrReplcPatt;

/* In default, we do not change initial password */
static bool change_password = false;

/* Only init database, for inplace upgrade test use */
static bool init_database = false;

/* Do inplace upgrade before run regression tests */
static bool inplace_upgrade = false;
static bool parallel_initdb = false;
static char* data_base_dir = "../upgrade";
static char* old_bin_dir = "./tmp_check/bin";
static int grayscale_upgrade = -1;
static int upgrade_from = 0;
static char* upgrade_script_dir = "../upgrade";
static bool super_user_altered = true;
static bool passwd_altered = true;
bool test_single_node = false;
static char* platform = "euleros2.0_sp2_x86_64";

/* client logic jdbc run regression tests */
static bool use_jdbc_client = false;
static bool to_create_jdbc_user = false;
static bool is_skip_environment_cleanup = false;
static char* client_logic_hook = "encryption";
static _stringlist* destination_files = NULL;

static bool directory_exists(const char* dir);
static void make_directory(const char* dir);
static void convertSourcefilesIn(char*, char*, char*, char*);
static void loadRegressConf(char* pcRegConfFile);
static void processTuplesOnlyConfItem(char* pcConfigValue);

static void processColSepConfItem(const char* pcConfigValue);
static int regressStrncasecmp(const char* s1, const char* s2, int maxLenToCmp);
static void processPerfDataLoggingConf(const char* pcConfigValue);
static bool regrValidateBoolConfigVal(const char* pcConfigValue, int iConfId);
static void regrGetElapsedTime(void);
static int regrGetResrcUsage(const pid_t pid, REGR_RESRC_STAT_STRU* result);
static void regrCalcCpuUsagePct(const REGR_RESRC_STAT_STRU* pstCurrUsage, const REGR_RESRC_STAT_STRU* pstPrevUsage,
    double* pdUcpuUsagePct, double* pdScpuUsagePct);
static void regrConvertSizeInBytesToReadableForm(unsigned long int ulValue, char* pcBuf, unsigned int uiBufSize);
static void kill_node(int i, int type);
static void cleanup_environment();

static void header(const char* fmt, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static void status(const char* fmt, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static void psql_command(const char* database, const char* query, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

#ifdef WIN32
typedef BOOL(WINAPI* __CreateRestrictedToken)(
    HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from some versions of MingW headers */
#ifndef DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE 0x1
#endif
#endif

#ifdef PGXC
static void psql_command_node(const char* database, int i, int type, const char* query, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 4, 5)));
#endif

static char* get_node_info_name(int i, int type, bool is_node_name);
static void initdb_node_info_parallel(bool standby);

static void setBinAndLibPath(bool);

/*
 * allow core files if possible.
 */
void restartPostmaster(bool isOld);

void checkProcInsert();

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
static void unlimit_core_size(void)
{
    struct rlimit lim;

    (void)getrlimit(RLIMIT_CORE, &lim);
    if (lim.rlim_max == 0) {
        fprintf(stderr, _("%s: could not set core size: disallowed by hard limit\n"), progname);
        return;
    } else if (lim.rlim_max == RLIM_INFINITY || lim.rlim_cur < lim.rlim_max) {
        lim.rlim_cur = lim.rlim_max;
        (void)setrlimit(RLIMIT_CORE, &lim);
    }
}
#endif

/*
 * Converts the size (represented in bytes), to human readable form
 * (in KB, MB or in GB) and the result is placed in the buffer.
 */
static void regrConvertSizeInBytesToReadableForm(unsigned long int ulValue, char* pcBuf, unsigned int uiBufSize)
{
    /* Representing the passed value as such because it is less than 1 KB */
    if (ulValue < REGR_MCR_SIZE_1KB)
        (void)snprintf(pcBuf, uiBufSize, "%lu B", ulValue);

    else if ((ulValue >= REGR_MCR_SIZE_1KB) && (ulValue < REGR_MCR_SIZE_1MB)) {
        double dSize = (ulValue / (double)REGR_MCR_SIZE_1KB);

        /* Representing the value in KB */
        (void)snprintf(pcBuf, uiBufSize, "%-7.2f KB", dSize);
    } else if ((ulValue >= REGR_MCR_SIZE_1MB) && (ulValue < REGR_MCR_SIZE_1GB)) {
        double dSize = (ulValue / (double)REGR_MCR_SIZE_1MB);

        /* Representing the value in MB */
        (void)snprintf(pcBuf, uiBufSize, "%-7.2f MB", dSize);
    } else if (ulValue >= REGR_MCR_SIZE_1GB) {
        double dSize = (ulValue / (double)REGR_MCR_SIZE_1GB);

        /* Representing the value in GB */
        (void)snprintf(pcBuf, uiBufSize, "%-7.2f GB", dSize);
    }
}

#define TwoPhaseTxntTestDirInput "2PCTxnt/input"
#define TwoPhaseTxntTestDirOutput "2PCTxnt/output"

/*
 * Gets the process id of the GAUSS Server process and the result will be
 * stored in g_iPostmasterPid
 */
static int regrGetServPid(char* pcBuff, unsigned int uiBuffLen, 
                                        unsigned int* puiPidChanged)
{
    PID_TYPE iPid = INVALID_PID;
    int iIter = REGR_NUM_OF_RETRY_FOR_PID_FILE_OPEN;
    FILE* fpPMPid = NULL;
    int type = test_single_node ? DATANODE : COORD;
    const char* data_folder = get_node_info_name(0, type, false);
    char cwd[MAXPGPATH] = {0};

#ifdef BUILD_BY_CMAKE // temp_install
    if (!snprintf(cwd, MAXPGPATH, "%s", temp_install)) {
        fprintf(stderr, _("\n Get temp_install dir fail.\n"));
        return -1;
    }
    if (strlen(cwd) + strlen(data_folder) + strlen("/postmaster.pid") >= uiBuffLen) {
        fprintf(stderr,
            _("\n Buffer space not enough for holding the path of "
              "the \'postmaster.pid\' file. Buffer Len: %u. Buffer "
              "length needed: %lu.\n"),
            uiBuffLen,
            (strlen(cwd) + strlen(data_folder) + strlen("/postmaster.pid") + 1));
            return REGR_ERRCODE_BUFF_NOT_ENOUGH;
    }

    (void)memset(pcBuff, '\0', uiBuffLen);
    (void)snprintf(pcBuff, uiBuffLen, SYSTEMQUOTE "%s/%s/postmaster.pid" SYSTEMQUOTE, cwd, data_folder);
#else
    if (!getcwd(cwd, MAXPGPATH)) {
        fprintf(stderr, _("\n Get current dir fail.\n"));
        return -1;
    }
    if (strlen(cwd) + strlen("/tmp_check/") + strlen(data_folder) + strlen("/postmaster.pid") >= uiBuffLen) {
        fprintf(stderr,
            _("\n Buffer space not enough for holding the path of "
              "the \'postmaster.pid\' file. Buffer Len: %u. Buffer "
              "length needed: %lu.\n"),
            uiBuffLen,
            (strlen(cwd) + strlen("/") + strlen("tmp_check/") + strlen(data_folder) + strlen("/postmaster.pid") + 1));
            return REGR_ERRCODE_BUFF_NOT_ENOUGH;
    }

    (void)memset(pcBuff, '\0', uiBuffLen);
    (void)snprintf(pcBuff, uiBuffLen, SYSTEMQUOTE "%s/tmp_check/%s/postmaster.pid" SYSTEMQUOTE, cwd, data_folder);
#endif

    do {
        fpPMPid = fopen(pcBuff, "r");
        if (NULL != fpPMPid)
            break;

        iIter--;
        pg_usleep(REGR_DELAY_FOR_PID_FILE_OPEN);
    } while (iIter > 0);

    free((char*)data_folder);

    if (NULL == fpPMPid) {
        fprintf(stderr, _("\n Could not open: [%s] for getting Server PID. \n"), pcBuff);
        return REGR_ERRCODE_FOPEN_FAILURE;
    }

    iPid = g_iPostmasterPid;
    g_iPostmasterPid = INVALID_PID;

    fscanf(fpPMPid, "%d", &g_iPostmasterPid);
    fclose(fpPMPid);

    if ((INVALID_PID != iPid) && (g_iPostmasterPid != iPid)) {
        *puiPidChanged = true;
    }

    return REGR_SUCCESS;
}

/*
 * read /proc data into the passed REGR_RESRC_STAT_STRU
 * returns 0 on success, -1 on error
 */
static int regrGetResrcUsage(const pid_t pid, REGR_RESRC_STAT_STRU* result)
{
    char pid_s[20]; /* convert  pid to string */
    int i;
    int iRet = 0;
    long int slRss;
    unsigned long int aulCpuTime[10] = {0};
    unsigned long int ulTotalMemory = 0;
    FILE* fpstat = NULL;
    FILE* fcstat = NULL;
    FILE* fpMemInfo = NULL;
    char stat_filepath[30] = "/proc/";

    (void)snprintf(pid_s, sizeof(pid_s), "%d", pid);

    strncat(stat_filepath, pid_s, (sizeof(stat_filepath) - strlen(stat_filepath) - 1));
    strncat(stat_filepath, "/stat", (sizeof(stat_filepath) - strlen(stat_filepath) - 1));

    fpstat = fopen(stat_filepath, "r");
    if (fpstat == NULL) {
        fprintf(stderr, "fopen %s : %s", stat_filepath, strerror(errno));
        return -1;
    }

    fcstat = fopen("/proc/stat", "r");
    if (fcstat == NULL) {
        iRet = -2;
        goto LB_ERR_LVL_1;
    }

    fpMemInfo = fopen("/proc/meminfo", "r");
    if (NULL == fpMemInfo) {
        iRet = -3;
        goto LB_ERR_LVL_2;
    }

    (void)memset(result, 0, sizeof(REGR_RESRC_STAT_STRU));

    /* Read values from /proc/pid/stat */
    if (fscanf(fpstat,
            "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu"
            "%lu %ld %ld %*d %*d %*d %*d %*u %lu %ld",
            &result->ulUtimeTicks,
            &result->ulStimeTicks,
            &result->lCutimeTicks,
            &result->lCstimeTicks,
            &result->vsize,
            &slRss) == EOF) {
        iRet = -4;
        goto LB_ERR_LVL_3;
    }

    /* slRss will show the total number of pages actually in memory */
    result->rss = slRss * getpagesize();

    /* Read and calculate 'cpu total time' from /proc/stat */
    if (fscanf(fcstat,
            "%*s %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
            &aulCpuTime[0],
            &aulCpuTime[1],
            &aulCpuTime[2],
            &aulCpuTime[3],
            &aulCpuTime[4],
            &aulCpuTime[5],
            &aulCpuTime[6],
            &aulCpuTime[7],
            &aulCpuTime[8],
            &aulCpuTime[9]) == EOF) {

        iRet = -5;
        goto LB_ERR_LVL_3;
    }

    for (i = 0; i < 10; i++)
        result->ulCpuTotalTime += aulCpuTime[i];

    /* Get the total memory (in kb) from /proc/meminfo file */
    if (fscanf(fpMemInfo, "%*s %lu", &ulTotalMemory) == EOF) {
        iRet = -6;
        goto LB_ERR_LVL_3;
    }

    ulTotalMemory *= 1024;
    result->dMemPct = (result->rss * 100) / ulTotalMemory;

    /*printf("\n Rss (size of pages in memory): [%lu bytes]. Total Memory:"
    " [%lu bytes].  Mem Pct: [%lf]...\n", result->rss, ulTotalMemory, result->dMemPct);*/

    iRet = 0;

LB_ERR_LVL_3:
    fclose(fpMemInfo);

LB_ERR_LVL_2:
    fclose(fcstat);

LB_ERR_LVL_1:
    fclose(fpstat);
    return iRet;
}

/* Calculates the elapsed CPU usage between 2 measuring points. in percent */
static void regrCalcCpuUsagePct(const REGR_RESRC_STAT_STRU* pstCurrUsage, const REGR_RESRC_STAT_STRU* pstPrevUsage,
    double* pdUcpuUsagePct, double* pdScpuUsagePct)
{
    const long unsigned int total_time_diff = pstCurrUsage->ulCpuTotalTime - pstPrevUsage->ulCpuTotalTime;

    if (0 != total_time_diff) {
        *pdUcpuUsagePct = 100 * (((pstCurrUsage->ulUtimeTicks + pstCurrUsage->lCutimeTicks) -
                                     (pstPrevUsage->ulUtimeTicks + pstPrevUsage->lCutimeTicks)) /
                                    (double)total_time_diff);

        *pdScpuUsagePct = 100 * ((((pstCurrUsage->ulStimeTicks + pstCurrUsage->lCstimeTicks) -
                                     (pstPrevUsage->ulStimeTicks + pstPrevUsage->lCstimeTicks))) /
                                    (double)total_time_diff);
    } else {
        *pdUcpuUsagePct = 0.0;
        *pdScpuUsagePct = 0.0;
    }
}

/* Get the CPU and Memory usage details from /proc/[pid]/stat file. If the
 * server pid has become invalid (may be because of the server restart by the
 * recovery framework), then the new server pid is fetched again and with this
 * new value, we try to get CPU usage details. If the server PID has changed
 * then puiPidChanged will be set to TRUE.
 * In this case, all the readings taken for the old server is obsolete.
 * So, the readings which are taken till that point, will be discarded. Thus
 * the readings will not be accurate, for the tests which involves recovery.
 */
static int regrGetRsrcUsageWithRepeatOnFail(REGR_RESRC_STAT_STRU* pstResrcUsageStat, unsigned int* puiPidChanged)
{
    int iRet = REGR_SUCCESS;
    int iRetServPid = REGR_SUCCESS;
    unsigned int uiExit = false;

    do {
        iRet = regrGetResrcUsage(g_iPostmasterPid, pstResrcUsageStat);
        if (REGR_SUCCESS == iRet)
            break;

        /* Could not get the CPU usage for the current PID value.
         * Server may have been restarted. So, get the updated ServerPID value*/
        if ((true == uiExit) ||
            (REGR_SUCCESS !=
                (iRetServPid = regrGetServPid((char*)g_acDataPath, MAXPGPATH + MAX_ADD_DATA_LEN, puiPidChanged)))) {
            fprintf(stderr,
                _("\n Could not get the Resource usage details. "
                  "Internal error code: %d. regrGetServPid EC: %d. "
                  "Exit flag status: %u. \n"),
                iRet,
                iRetServPid,
                uiExit);
            return REGR_ERRCODE_RESRC_STAT_FETCH_FAILED;
        }

        if (true == *puiPidChanged) {
            /* Server would have restarted. Thus PID changed. So reset the
             * values based on the old PID */
            (void)memset(&g_stResourceUsageDetails, 0, sizeof(REGR_AVG_RSRCE_USAGE_STRU));
        }

        uiExit = true;
    } while (true);

    return REGR_SUCCESS;
}

/* Get the subsequent reading for the CPU and Memory usage. For calculating
 * the CPU usage, we need two set of readings of REGR_RESRC_STAT_STRU instance*/
static void regrGetSubsequentUsage()
{
    unsigned int uiPidChanged = false;
    double dUcpuUsagePct = 0.0;
    double dScpuUsagePct = 0.0;

    /* If the first CPU usage reading is not available, then there is no point
     * in getting second CPU usage readings */
    if (0 == g_stPrevCpuUsage.ulCpuTotalTime) {
        return;
    }

    /* Getting the CPU Usage details */
    if (REGR_SUCCESS != regrGetRsrcUsageWithRepeatOnFail(&g_stCurrCpuUsage, &uiPidChanged)) {
        fprintf(stderr, _("Could not get the CPU Usage info!! \n"));
        return;
    }

    if (false == uiPidChanged) {
        /* In case of Server restart i.e. uiPidChanged is TRUE, g_stPrevCpuUsage
         * value is based on the old pid which can't be compared against the
         * current value. So, cannot call the below function. Otherwise,
         * getting the cpu usage in percentage, for the previous and
         * current values */
        regrCalcCpuUsagePct(&g_stCurrCpuUsage, &g_stPrevCpuUsage, &dUcpuUsagePct, &dScpuUsagePct);
    }

    /* The current reading is set as the 'previous' reading so that next time
     * this can be used to calculate the CPU %, because CPU % calculation
     * requires two set of readings. */
    g_stPrevCpuUsage = g_stCurrCpuUsage;

    /* Memory usage is added up (in MBs) and finally the sum will be divided by
     * the total reading count to get the average */
    g_stResourceUsageDetails.dAvgMemUsage += (g_stCurrCpuUsage.rss / REGR_MCR_SIZE_1MB);

    /* Adding up the Memory percents */
    g_stResourceUsageDetails.dAvgMemUsagePct += g_stCurrCpuUsage.dMemPct;

    /* Adding up the User CPU usage detail */
    g_stResourceUsageDetails.dUAvgcpuUsage += dUcpuUsagePct;

    /* Adding up the Kernel(system) CPU usage detail */
    g_stResourceUsageDetails.dSAvgcpuUsage += dScpuUsagePct;

    /* Updating the total sample counts */
    g_stResourceUsageDetails.ulCount++;
}

/*
 * Add an item at the end of a stringlist.
 */
void add_stringlist_item(_stringlist** listhead, const char* str)
{
    _stringlist* newentry = (_stringlist*)malloc(sizeof(_stringlist));
    _stringlist* oldentry = NULL;

    newentry->str = strdup(str);
    newentry->next = NULL;
    if (*listhead == NULL) {
        *listhead = newentry;
    } else {
        for (oldentry = *listhead; oldentry->next; oldentry = oldentry->next)
            /* skip */;
        oldentry->next = newentry;
    }
}

/*
 * Free a stringlist.
 */
static void free_stringlist(_stringlist** listhead)
{
    if (listhead == NULL || *listhead == NULL) {
        return;
    }
    if ((*listhead)->next != NULL) {
        free_stringlist(&((*listhead)->next));
    }
    free((*listhead)->str);
    free(*listhead);
    *listhead = NULL;
}

/*
 * Split a delimited string into a stringlist
 */
static void split_to_stringlist(const char* s, const char* delim, _stringlist** listhead)
{
    char* sc = strdup(s);
    char* tmp_token = NULL;
    char* token = strtok_r(sc, delim, &tmp_token);

    while (token) {
        add_stringlist_item(listhead, token);
        token = strtok_r(NULL, delim, &tmp_token);
    }
    free(sc);
}

/* Trim LHS and RHS of the passed string for white spaces */
static void regrTrimString(char** ppcString)
{
    while ((**ppcString == ' ') || (**ppcString == '\t'))
        (*ppcString)++;

    if ((**ppcString) != '\0') {
        int len = strlen(*ppcString);
        char* ptr = *ppcString + (len - 1);

        while (((len--) > 0) && ((*ptr == ' ') || (*ptr == '\t') || (*ptr == '\r') || (*ptr == '\n')))
            ptr--;

        *(ptr + 1) = '\0';
    }
}

/*
 * Print a progress banner on stdout.
 */
static void header(const char* fmt, ...)
{
    char tmp[1024];
    va_list ap;

    va_start(ap, fmt);
    (void)vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    fprintf(stdout, "============== %-38s ==============\n", tmp);
    (void)fflush(stdout);
}

/*
 * Print "doing something ..." --- supplied text should not end with newline
 */
static void status(const char* fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    (void)vfprintf(stdout, fmt, ap);
    (void)fflush(stdout);
    va_end(ap);

    if (logfile) {
        va_start(ap, fmt);
        (void)vfprintf(logfile, fmt, ap);
        va_end(ap);
    }
}

/*
 * Done "doing something ..."
 */
static void status_end(void)
{
    fprintf(stdout, "\n");
    (void)fflush(stdout);
    if (logfile) {
        fprintf(logfile, "\n");
    }
}

static int regr_system(const char* command)
{
    int ret = system(command);
    if (ignore_exitcode)
        return 0;
    return ret;
}

#ifdef PGXC
/*
 * Stop GTM process
 */
static void stop_gtm(void)
{
    const char* data_folder = get_node_info_name(0, GTM, false);
    int r;
    char buf[MAXPGPATH * 2];

    /* On Windows, system() seems not to force fflush, so... */
    (void)fflush(stdout);
    (void)fflush(stderr);

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gtm_ctl\" stop -Z gtm -D \"%s/%s\" -m fast" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder);

    r = system(buf);
    if (r != 0) {
        memset(buf, 0, sizeof(buf));
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gtm_ctl\" stop -Z gtm -D \"%s/%s\" -m i" SYSTEMQUOTE,
            bindir,
            temp_install,
            data_folder);
        r = regr_system(buf); /* second time we don't check exit code if --ignore-exitcode*/
        if (r != 0) {
            fprintf(stderr, _("\n%s: could not stop GTM: exit code was %d\n"), progname, r);
            free((char*)data_folder);
            exit(2); /* not exit(), that would be recursive */
        }
    }
    free((char*)data_folder);
}

static void gen_startstop_script()
{
    int i;
    int port_number;
    char buf[MAXPGPATH * 4];

    if (temp_install == NULL)
        return;

    (void)snprintf(buf, sizeof(buf), "%s/start.sh", temp_install);
    start_script = fopen(buf, PG_BINARY_W);

    (void)snprintf(buf, sizeof(buf), "%s/stop.sh", temp_install);
    stop_script = fopen(buf, PG_BINARY_W);

    const char* data_folder = get_node_info_name(0, GTM, false);
    fprintf(start_script, "#! /bin/bash \n");
    fprintf(start_script, "find %s  -name postmaster.pid | xargs rm -rf \n", temp_install);

    fprintf(start_script, "%s/gs_gtm -n gtm -D %s/%s -p %d & \n", bindir, temp_install, data_folder, myinfo.gtm_port);

    free((char*)data_folder);

    fprintf(stop_script, "#! /bin/bash \n");
    for (i = 0; i < myinfo.co_num; i++) {
        data_folder = get_node_info_name(i, COORD, false);
        port_number = myinfo.co_port[i];

        fprintf(start_script,
            "%s/gaussdb --coordinator  -i -p %d -D %s/%s -c log_statement=all -c logging_collector=true -c "
            "log_filename='cn.log' & \n",
            bindir,
            port_number,
            temp_install,
            data_folder);

        fprintf(stop_script, "%s/gs_ctl stop -D %s/%s \n", bindir, temp_install, data_folder);
        free((char*)data_folder);
    }
    for (i = 0; i < myinfo.dn_num; i++) {
        data_folder = get_node_info_name(i, DATANODE, false);
        port_number = myinfo.dn_port[i];
        fprintf(start_script,
            "%s/gaussdb --datanode  -i -p %d -D %s/%s -c log_statement=all -c logging_collector=true -c "
            "log_filename='dn.log' & \n",
            bindir,
            port_number,
            temp_install,
            data_folder);

        fprintf(stop_script, "%s/gs_ctl stop -D %s/%s \n", bindir, temp_install, data_folder);
        free((char*)data_folder);
    }

    const char* gtm_folder = get_node_info_name(0, GTM, false);
    fprintf(stop_script, "%s/gtm_ctl stop -Z gtm -D %s/%s -m fast \n", bindir, temp_install, gtm_folder);

    (void)snprintf(buf, sizeof(buf), "chmod +x %s/start.sh", temp_install);
    (void)system(buf);
    (void)snprintf(buf, sizeof(buf), "chmod +x %s/stop.sh", temp_install);
    (void)system(buf);
    fclose(start_script);
    fclose(stop_script);

    free((char*)gtm_folder);
}

/*
 * Stop the given node
 */
static void stop_node(int i, int type, bool standby)
{
    char* data_folder = get_node_info_name(i, type, false);
    char buf[MAXPGPATH * 2];
    int r;

    /* On Windows, system() seems not to force fflush, so... */
    (void)fflush(stdout);
    (void)fflush(stderr);

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gs_ctl\" stop -D \"%s/%s\" -s -m fast >>/dev/null" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder);
    r = system(buf);
    if (r != 0) {
        memset(buf, 0, sizeof(buf));
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_ctl\" stop -D \"%s/%s\" -s -m i >>/dev/null" SYSTEMQUOTE,
            bindir,
            temp_install,
            data_folder);
        r = regr_system(buf);
        if (r != 0) {
            fprintf(stderr, _("\n%s: could not stop postmaster: exit code was %d\n"), progname, r);
            exit(2); /* not exit(), that would be recursive */
        }
    }

    if (!standby) {
        free(data_folder);
        return;
    }

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gs_ctl\" stop -D \"%s/%s_standby\" -s -m fast >>/dev/null" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder);

    r = system(buf);
    if (r != 0) {
        memset(buf, 0, sizeof(buf));
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_ctl\" stop -D \"%s/%s_standby\" -s -m i >>/dev/null" SYSTEMQUOTE,
            bindir,
            temp_install,
            data_folder);
        r = regr_system(buf);
        if (r != 0) {
            fprintf(stderr, _("\n%s: could not stop postmaster: exit code was %d\n"), progname, r);
            exit(2); /* not exit(), that would be recursive */
        }
    }
    free(data_folder);
}
#endif

/*
 * shut down temp postmaster
 */
static void stop_postmaster(void)
{
    int i;
    printf("stop postmaster now!\n");
    if (postmaster_running) {
#ifdef PGXC

        /*
         * stop cn\dn
         */
        for (i = 0; i < myinfo.co_num; i++) {
            stop_node(i, COORD, false);
        }
        for (i = 0; i < myinfo.dn_num; i++) {
            stop_node(i, DATANODE, standby_defined);
        }

        for (i = 0; i < myinfo.shell_count; i++) {
            (void)kill(myinfo.shell_pid[i], SIGKILL);
        }

        if (!test_single_node) {
            /* Stop GTM at the end */
            stop_gtm();
        }

#else
        /* We use pg_ctl to issue the kill and wait for stop */
        char buf[MAXPGPATH * 2];
        int r;

        /* On Windows, system() seems not to force fflush, so... */
        fflush(stdout);
        fflush(stderr);

        snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_ctl\" stop -D \"%s/data\" -s -m fast" SYSTEMQUOTE,
            bindir,
            temp_install);
        r = regr_system(buf);
        if (r != 0) {
            fprintf(stderr, _("\n%s: could not stop postmaster: exit code was %d\n"), progname, r);
            exit(2); /* not exit(), that would be recursive */
        }
#endif

        postmaster_running = false;
    }
}

static void atexit_cleanup()
{
    stop_postmaster();
    cleanup_environment();
}

#ifdef PGXC
/*
 * Handy subroutine for setting an environment variable "var" to "val"
 */
/*lint -e429*/
static void doputenv(const char* var, const char* val)
{
    char* s = (char*)malloc(strlen(var) + strlen(val) + 2);

    sprintf(s, "%s=%s", var, val);
    (void)putenv(s);
}
/*lint +e429*/

/*
 * Get node port of associated node
 */
static int get_port_number(int i, int type)
{
    switch (type) {
        case COORD:
            return (test_single_node ? 0 : myinfo.co_port[i]);
        case DATANODE:
            return myinfo.dn_port[i];
        case GTM:
            return myinfo.gtm_port;
        default:
            /* Should not happen */
            return -1;
    }
}

/*
 * Get PID number for given node
 */
static PID_TYPE get_node_pid(int i, int type)
{
    switch (type) {
        case COORD:
            return myinfo.co_pid[i];
        case DATANODE:
            return myinfo.dn_pid[i];
        case GTM:
            return myinfo.gtm_pid;
        default:
            /* Should not happen */
            return -1;
    }
}

/*
 * Start GTM process
 */
static void start_gtm(void)
{
    char buf[MAXPGPATH * 4];
    const char* data_folder = get_node_info_name(0, GTM, false);
    PID_TYPE node_pid;
    FILE* gtm_conf_file = NULL;

    (void)snprintf(buf, sizeof(buf), "%s/%s/gtm.conf", temp_install, data_folder);
    gtm_conf_file = fopen(buf, PG_BINARY_W);
    if (gtm_conf_file == NULL) {
        fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"), progname, buf, strerror(errno));
        exit_nicely(2);
    }
    if (fclose(gtm_conf_file)) {
        fprintf(stderr, _("%s: could not write file \"%s\": %s\n"), progname, buf, strerror(errno));
        exit_nicely(2);
    }

    header(_("starting GTM process"));
    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gs_gtm\" -n \"gtm\" -D \"%s/%s\" -p %d > \"%s/log/gtm.log\" 2>&1" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder,
        myinfo.gtm_port,
        outputdir);

    /* Start process */
    node_pid = spawn_process(buf);
    if (node_pid == INVALID_PID) {
        fprintf(stderr, _("\n%s: could not spawn GTM: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }

    /* Save static PID number */
    myinfo.gtm_pid = node_pid;
}

/* Start single datanode for test */
static void start_single_node()
{
    char buf[MAXPGPATH * 4];
    int port_number = myinfo.dn_port[0];
    char* data_folder = get_node_info_name(0, DATANODE, false);
    PID_TYPE guc_pid;
    char guc_buf[MAXPGPATH * 4];
    char upgrade_from_str[MAXPGPATH] = {'\0'};

    if (inplace_upgrade || grayscale_upgrade != -1) {
        (void)snprintf(guc_buf,
            sizeof(guc_buf),
            SYSTEMQUOTE "\"%s/gs_guc\" set -Z datanode -D \"%s/%s\" -c \"%s\" > /dev/null 2>&1" SYSTEMQUOTE,
            bindir,
            temp_install,
            data_folder,
            upgrade_from == 0 ? "upgrade_mode=0" : grayscale_upgrade == -1 ? "upgrade_mode=1" : "upgrade_mode=2");
        guc_pid = spawn_process(guc_buf);
        if (guc_pid == INVALID_PID) {
            fprintf(stderr, _("\n%s: could not spawn GUC parameter: %s\n"), progname, strerror(errno));
            exit_nicely(2);
        }
        // check if guc parameter is set successfully
        int result = 0;
        int count = 0;
        while (result != 1) {
            count++;
            char cmd[MAXPGPATH] = {'\0'};
#ifdef BUILD_BY_CMAKE
            (void)snprintf(cmd,
                sizeof(cmd),
                "find %s/%s/ -name \"postgresql.conf\" | xargs grep \"upgrade_mode = %s\" | wc -l",
                temp_install,
                data_folder,
                upgrade_from == 0 ? "0" : grayscale_upgrade == -1 ? "1" : "2");

#else
            (void)snprintf(cmd,
                sizeof(cmd),
                "sync; find ./tmp_check/%s/ -name \"postgresql.conf\" | xargs grep \"upgrade_mode = %s\" | wc -l",
                data_folder,
                upgrade_from == 0 ? "0" : grayscale_upgrade == -1 ? "1" : "2");
#endif

            FILE* fstream = NULL;
            char buf[1000];
            memset(buf, 0, sizeof(buf));
            fstream = popen(cmd, "r");
            if (NULL != fgets(buf, sizeof(buf), fstream)) {
                result = atoi(buf);
            }
            if (result != 1) {
                sleep(1);
            }
            if (count > 180) {
                fprintf(stderr, _("\n%s: fail to set upgrade_mode GUC\n count %d"), progname, count);
                exit(2);
            }
            if (NULL != fstream) {
                pclose(fstream);
            }
        }

        if (upgrade_from)
            (void)snprintf(
                upgrade_from_str, sizeof(upgrade_from_str), "-u %d -c allow_system_table_mods=true", upgrade_from);
    }

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gaussdb\" %s %s %s -i -p %d -D \"%s/%s\"%s -c log_statement=all -c logging_collector=true -c "
                    "\"listen_addresses=%s\" > \"%s/log/postmaster_%s.log\" 2>&1" SYSTEMQUOTE,
        bindir,
        upgrade_from_str,
        "--single_node",
        (securitymode) ? "--securitymode" : " ",
        port_number,
        temp_install,
        data_folder,
        debug ? " -d 5" : "",
        hostname ? hostname : "*",
        outputdir,
        data_folder);
    header( _("\nstart cmd is : %s\n"), buf);
    PID_TYPE datanode_pid = spawn_process(buf);
    if (datanode_pid == INVALID_PID) {
        fprintf(stderr, _("\n%s: could not spawn postmaster: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }

    printf("Single datanode: node_pid %d.\n", datanode_pid);

    myinfo.dn_pid[0] = datanode_pid;

    free(data_folder);
}

/*
 * Start given node
 */
static void start_my_node(int i, int type, bool is_main, bool standby, int upgrade_from)
{
    int port_number;
    char* data_folder = get_node_info_name(i, type, false);
    if (type == COORD) {
        port_number = myinfo.co_port[i];
    } else {
        port_number = myinfo.dn_port[i];
    }

    PID_TYPE guc_pid;
    PID_TYPE node_pid;
    char guc_buf[MAXPGPATH * 4];
    char buf[MAXPGPATH * 4];
    char upgrade_from_str[MAXPGPATH] = {'\0'};

    (void)snprintf(guc_buf,
        sizeof(guc_buf),
        SYSTEMQUOTE "\"%s/gs_guc\" set -Z %s -D \"%s/%s\" -c \"%s\" " SYSTEMQUOTE,
        bindir,
        (type == COORD) ? "coordinator" : "datanode",
        temp_install,
        data_folder,
        upgrade_from == 0 ? "upgrade_mode=0" : grayscale_upgrade == -1 ? "upgrade_mode=1" : "upgrade_mode=2");
    guc_pid = spawn_process(guc_buf);
    if (guc_pid == INVALID_PID) {
        fprintf(stderr, _("\n%s: could not spawn GUC parameter: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }
    // check if guc parameter is set successfully
    int result = 0;
    int count = 0;
    while (result != 1) {
        count++;
        char cmd[MAXPGPATH] = {'\0'};
#ifdef BUILD_BY_CMAKE
        (void)snprintf(cmd,
            sizeof(cmd),
            "find %s/%s/ -name \"postgresql.conf\" | xargs grep \"upgrade_mode = %s\" | wc -l",
            temp_install,
            data_folder,
            upgrade_from == 0 ? "0" : grayscale_upgrade == -1 ? "1" : "2");

#else
        (void)snprintf(cmd,
            sizeof(cmd),
            "find ./tmp_check/%s/ -name \"postgresql.conf\" | xargs grep \"upgrade_mode = %s\" | wc -l",
            data_folder,
            upgrade_from == 0 ? "0" : grayscale_upgrade == -1 ? "1" : "2");
#endif
        
        FILE* fstream = NULL;
        char buf[50];
        memset(buf, 0, sizeof(buf));
        fstream = popen(cmd, "r");
        if (NULL != fgets(buf, sizeof(buf), fstream)) {
            result = atoi(buf);
        }
        if (result != 1) {
            sleep(1);
        }
        if (count > 180) {
            fprintf(stderr, _("\n%s: fail to set upgrade_mode GUC\n count %d"), progname, count);
            exit(2);
        }
        if (NULL != fstream) {
            pclose(fstream);
        }
    }

    if (upgrade_from)
        (void)snprintf(
            upgrade_from_str, sizeof(upgrade_from_str), "-u %d -c allow_system_table_mods=true", upgrade_from);

    /* Start the node */
    if (is_main) {
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE
            "\"%s/gaussdb\" %s %s %s -i -p %d -D \"%s/%s\"%s -c log_statement=all -c logging_collector=true -c "
            "\"listen_addresses=%s\" > \"%s/log/postmaster_%s.log\" 2>&1" SYSTEMQUOTE,
            bindir,
            upgrade_from_str,
            (type == COORD) ? "--coordinator" : "--datanode",
            (securitymode) ? "--securitymode" : " ",
            port_number,
            temp_install,
            data_folder,
            debug ? " -d 5" : "",
            hostname ? hostname : "*",
            outputdir,
            data_folder);

    } else {
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gaussdb\" %s %s %s -i -p %d -c log_statement=all -M %s -c logging_collector=true -D "
                        "\"%s/%s\"%s > \"%s/log/postmaster_%s.log\" 2>&1" SYSTEMQUOTE,
            bindir,
            upgrade_from_str,
            (type == COORD) ? "--coordinator" : "--datanode",
            (securitymode) ? "--securitymode" : " ",
            port_number,
            (standby) ? "primary" : "normal",
            temp_install,
            data_folder,
            debug ? " -d 5" : "",
            outputdir,
            data_folder);
    }
    header( _("\nstart cmd is : %s\n"), buf);
    node_pid = spawn_process(buf);
    if (node_pid == INVALID_PID) {
        fprintf(stderr, _("\n%s: could not spawn postmaster: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }

    printf("node name: %s%d, node_pid: %d.\n", (type == COORD ? "Coordinator" : "Datanode"), i + 1, node_pid);
    /* Save static PID number */
    if (type == COORD) {
        myinfo.co_pid[i] = node_pid;
    } else {
        myinfo.dn_pid[i] = node_pid;
    }
    free(data_folder);
}

/*
 * Use gs_ctl build to setup datanode replication if requested.
 */
static void setup_datanode_replication(int index)
{
    char* data_folder = get_node_info_name(index, DATANODE, false);
    PID_TYPE node_pid;
    char buf[MAXPGPATH * 4];

    if (!standby_defined) {
        free(data_folder);
        return; /* nothing to do */
    }

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gs_ctl\" build -b full -Z datanode -D \"%s/%s_standby\" > \"%s/log/build_%s_standby.log\" "
                    "2>&1" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder,
        outputdir,
        data_folder);

    node_pid = spawn_process(buf);
    if (node_pid == INVALID_PID) {
        fprintf(stderr, _("\n%s: could not spawn gs_ctl: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }
    free(data_folder);
}

static char* get_node_info_name(int i, int type, bool is_node_name)
{
    char* mybuf = NULL;
    int len = 100 * sizeof(char);
    mybuf = (char*)malloc(len);
    switch (type) {
        case COORD: {
            if (is_node_name) {
                (void)snprintf(mybuf, len, "coordinator%d", i + 1);
            } else {
                (void)snprintf(mybuf, len, "coordinator%d", i + 1);
            }
        } break;
        case DATANODE: {
            if (is_node_name) {
                (void)snprintf(mybuf, len, "datanode%d", i + 1);
            } else {
                (void)snprintf(mybuf, len, "datanode%d", i + 1);
            }
        } break;
        case GTM: {
            (void)snprintf(mybuf, len, "datagtm");
        } break;
    }

    return mybuf;
}

/*
 * Initialize GTM
 */
static void init_gtm(void)
{
    if (test_single_node)
        return;

    const char* data_folder = get_node_info_name(0, GTM, false);
    char buf[MAXPGPATH * 4];

    (void)snprintf(buf,
        sizeof(buf),
        SYSTEMQUOTE "\"%s/gs_initgtm\" -Z gtm -D \"%s/%s\" --noclean%s > \"%s/log/initgtm.log\" 2>&1 &" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder,
        debug ? " --debug" : "",
        outputdir);
    free((char*)data_folder);
    if (regr_system(buf)) {
        fprintf(stderr,
            _("\n%s: gs_initgtm failed\nExamine %s/log/initgtm.log for the reason.\nCommand was: %s\n"),
            progname,
            outputdir,
            buf);
        exit_nicely(2);
    }
}

thread_desc* create_thread_desc(int thn)
{
    thread_desc* ret = (thread_desc*)malloc(sizeof(thread_desc));

    ret->thn = thn;
    ret->thi = 0;
    ret->thd = (pthread_t*)malloc(sizeof(pthread_t) * thn);
    ret->args = (char**)malloc(sizeof(char*) * thn);

    for (int i = 0; i < ret->thn; i++) {
        ret->args[i] = (char*)malloc(sizeof(char) * (MAXPGPATH * 4));
    }

    return ret;
}

void free_thread_desc(thread_desc* desc)
{
    int i;
    /* free memory */
    for (i = 0; i < desc->thn; i++) {
        free(desc->args[i]);
    }

    free(desc->args);
    free(desc->thd);
    free(desc);
}
/*
 * the thread function that invoke the gs_initdb
 *
 * @arg: the argument for the thread
 */
static void* thread_initdb(void* arg)
{
    if (regr_system((const char*)arg)) {
        pthread_exit((void*)"fail");
    }

    pthread_exit((void*)"ok");
}

/*
 * formulate the args to gs_initdb and spawn a thread to run
 *
 * @is_cn: cn or dn?
 * @is_standby: is standby ?
 * @i: which thread?
 * @thd: pthread_t array
 * @thi:  thread counter
 * @args: arguments for each thread
 */
static void start_thread(thread_desc* thread, bool is_cn, bool is_standby, int i)
{
    char* data_folder = get_node_info_name(i, is_cn ? COORD : DATANODE, false);
    char** args = thread->args;
    int thi = thread->thi;
    pthread_t* thd = &(thread->thd[thi]);

    printf("bindir: %s\n", bindir);
    (void)snprintf(args[thi],
        MAXPGPATH * 4,
        SYSTEMQUOTE "\"%s/gs_initdb\" --nodename %s %s -w \"gauss@123\" -D \"%s/%s%s\" -L \"%s\" --noclean%s%s > "
                    "\"%s/log/initdb%d.log\" 2>&1" SYSTEMQUOTE,
        bindir,
        (char*)get_node_info_name(i, is_cn ? COORD : DATANODE, true),
        init_database ? "-U upcheck" : "",
        temp_install,
        data_folder,
        is_standby ? "_standby" : "",
        datadir,
        debug ? " --debug" : "",
        nolocale ? " --no-locale" : "",
        outputdir,
        thi);

    /* ready to play */
    if (0 != pthread_create(thd, NULL, &thread_initdb, (void*)args[thi])) {
        fprintf(stderr, _("\n%s: gs_initdb failed to spawn thread for gs_initdb\n"), progname);
        exit(2);
    } else {
        fprintf(stdout, " %d", thi);
        (void)fflush(stdout);
    }

    thread->thi++;

    free(data_folder);
}

/*
 * wait the thread to finish
 *
 * @thn: how many thread to wait
 * @thd: pthread_t array
 * @args: arguments for each thread
 */
static void wait_thread(thread_desc* thread)
{
    int i;
    bool has_error = false;
    pthread_t* thd = thread->thd;
    char** args = thread->args;
    int thn = thread->thn;

    fprintf(stdout, "\n==============waithing for thread to finish==============\n");
    (void)fflush(stdout);

    /* wait for thread join and free memory */
    for (i = 0; i < thn; i++) {
        char* retval = NULL;

        if (0 != pthread_join(thd[i], (void**)&retval)) {
            fprintf(stderr, _("\n%s: gs_initdb failed to join threads\n"), progname);
            has_error = true;
        }

        if (0 != strncmp(retval, "ok", 2)) {
            fprintf(stderr,
                _("\n%s: thread %d failed to initdb, please check \"%s/log/initdb%d.log\", command was %s\n"),
                progname,
                i,
                outputdir,
                i,
                args[i]);
            has_error = true;
        }
    }

    if (has_error) {
        fprintf(stdout, "==============initdb failed==============\n");
        exit(2);
    } else {
        fprintf(stdout, "==============initdb finished==============\n");
        (void)fflush(stdout);
    }
}

/* Copy data file to data dir */
static void CopyFile()
{
    char dstDatadir[MAXPGPATH];
    char cmdBuf[MAXPGPATH * 2];

    errno_t rc = snprintf_s(dstDatadir, sizeof(dstDatadir), sizeof(dstDatadir) - 1, "%s/%s/pg_copydir",
        temp_install, get_node_info_name(0, DATANODE, false));
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(cmdBuf, sizeof(cmdBuf), sizeof(cmdBuf) - 1,
        SYSTEMQUOTE "mkdir -p %s ; cp ./data/* %s -r" SYSTEMQUOTE, dstDatadir, dstDatadir);
    securec_check_ss_c(rc, "", "");
    if (regr_system(cmdBuf)) {
        (void)fprintf(stderr,
            _("\n%s: cp data file failed\nCommand was: %s\n"),
            progname,
            cmdBuf);
        exit_nicely(2);
    }

    /* mkdir results dir */
    rc = snprintf_s(dstDatadir, sizeof(dstDatadir), sizeof(dstDatadir) - 1, "%s/%s/pg_copydir/results",
        temp_install, get_node_info_name(0, DATANODE, false));
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(cmdBuf, sizeof(cmdBuf), sizeof(cmdBuf) - 1, SYSTEMQUOTE "mkdir -p %s" SYSTEMQUOTE, dstDatadir);
    securec_check_ss_c(rc, "", "");
    if (regr_system(cmdBuf)) {
        (void)fprintf(stderr,
            _("\n%s: mkdir failed\nCommand was: %s\n"),
            progname,
            cmdBuf);
        exit_nicely(2);
    }
}

/*
 * initdb for cn and dn
 *
 * @standby: if init standby node
 */
static void initdb_node_info_parallel(bool standby)
{
    int i;
    thread_desc* thread = create_thread_desc(myinfo.co_num + myinfo.dn_num * (standby ? 2 : 1));

    fprintf(stdout, "==============spawned thread");
    (void)fflush(stdout);

    for (i = 0; i < myinfo.co_num; i++) {
        start_thread(thread, true, false, i);
    }

    for (i = 0; i < myinfo.dn_num; i++) {
        start_thread(thread, false, false, i);
    }

    if (standby) {
        for (i = 0; i < myinfo.dn_num; i++) {
            start_thread(thread, false, true, i);
        }
    }

    fprintf(stdout, " for parallel initdb==============");
    (void)fflush(stdout);

    /* wait for thread join */
    wait_thread(thread);

    /* free the memory */
    free_thread_desc(thread);

    CopyFile();
}

static void initdb_node_info(bool standby)
{
    int i;

    for (i = 0; i < myinfo.co_num; i++) {
        char buf[MAXPGPATH * 4];

        printf("bindir: %s\n", bindir);
        char* data_folder = get_node_info_name(i, COORD, false);
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_initdb\" --nodename %s %s -w \"gauss@123\" -D \"%s/%s\" -L \"%s\" --noclean%s%s > "
                        "\"%s/log/initdb.log\" 2>&1" SYSTEMQUOTE,
            bindir,
            (char*)get_node_info_name(i, COORD, true),
            init_database ? "-U upcheck" : "",
            temp_install,
            data_folder,
            datadir,
            debug ? " --debug" : "",
            nolocale ? " --no-locale" : "",
            outputdir);
        free(data_folder);
        if (regr_system(buf)) {
            fprintf(stderr,
                _("\n%s: gs_initdb failed\nExamine %s/log/initdb.log for the reason.\nCommand was: %s\n"),
                progname,
                outputdir,
                buf);
            exit_nicely(2);
        }
    }

    for (i = 0; i < myinfo.dn_num; i++) {
        char buf[MAXPGPATH * 4];

        char* data_folder = get_node_info_name(i, DATANODE, false);
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_initdb\" --nodename %s %s -w \"gauss@123\" -D \"%s/%s\" -L \"%s\" --noclean%s%s > "
                        "\"%s/log/initdb.log\" 2>&1" SYSTEMQUOTE,
            bindir,
            (char*)get_node_info_name(i, DATANODE, true),
            init_database ? "-U upcheck" : "",
            temp_install,
            data_folder,
            datadir,
            debug ? " --debug" : "",
            nolocale ? " --no-locale" : "",
            outputdir);
        if (regr_system(buf)) {
            fprintf(stderr,
                _("\n%s: gs_initdb failed\nExamine %s/log/initdb.log for the reason.\nCommand was: %s\n"),
                progname,
                outputdir,
                buf);
            exit_nicely(2);
        }
        free(data_folder);
    }

    CopyFile();

    /* no standby configed, skip to init standby. */
    if (!standby)
        return;

    for (i = 0; i < myinfo.dn_num; i++) {
        char buf[MAXPGPATH * 4];

        char* data_folder = get_node_info_name(i, DATANODE, false);
        (void)snprintf(buf,
            sizeof(buf),
            SYSTEMQUOTE "\"%s/gs_initdb\" --nodename %s %s -w \"gauss@123\" -D \"%s/%s_standby\" -L \"%s\" "
                        "--noclean%s%s > \"%s/log/initdb.log\" 2>&1" SYSTEMQUOTE,
            bindir,
            (char*)get_node_info_name(i, DATANODE, true),
            init_database ? "-U upcheck" : "",
            temp_install,
            data_folder,
            datadir,
            debug ? " --debug" : "",
            nolocale ? " --no-locale" : "",
            outputdir);
        if (regr_system(buf)) {
            fprintf(stderr,
                _("\n%s: gs_initdb failed\nExamine %s/log/initdb.log for the reason.\nCommand was: %s\n"),
                progname,
                outputdir,
                buf);
            exit_nicely(2);
        }
        free(data_folder);
    }
}

int get_local_ip(char* local_ip)
{
    char buf[100];
    struct addrinfo hints;
    struct addrinfo *res, *curr;
    struct sockaddr_in* sa;
    errno_t ss_rc = 0;

    ss_rc = memset_s(&hints, sizeof(addrinfo), 0x0, sizeof(addrinfo));
    securec_check_c(ss_rc, "\0", "\0");
    hints.ai_flags = AI_CANONNAME;
    hints.ai_family = AF_INET;

    if (gethostname(buf, sizeof(buf)) < 0) {
        return -1;
    }

    if (getaddrinfo(buf, 0, &hints, &res) != 0) {
        return -1;
    }

    if (NULL == res) {
        return -1;
    }

    curr = res;

    sa = (struct sockaddr_in*)curr->ai_addr;
    (void)snprintf(local_ip, LOCAL_IP_LEN, "%s", inet_ntop(AF_INET, &sa->sin_addr.s_addr, buf, sizeof(buf)));

    if (res) {
        freeaddrinfo(res);
    }

    return 0;
}

static void config_cn();
static void config_dn(bool standby);
static void config_standby();

static void initdb_node_config_file(bool standby)
{
    config_cn();

    config_dn(standby);

    if (!standby)
        return;
    config_standby();
}

static void config_cn()
{
    int i;
    char local_ip[LOCAL_IP_LEN];
    for (i = 0; i < myinfo.co_num; i++) {
        char* data_folder = get_node_info_name(i, COORD, false);
        FILE* pg_conf = NULL;
        char buf[MAXPGPATH * 4];

        (void)snprintf(buf, sizeof(buf), "%s/%s/postgresql.conf", temp_install, data_folder);
        pg_conf = fopen(buf, "a");
        if (pg_conf == NULL) {
            fprintf(
                stderr, _("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
            exit_nicely(2);
        }
        fputs("\n# Configuration added by pg_regress\n\n", pg_conf);

        (void)snprintf(buf, sizeof(buf), "port = %d\n", myinfo.co_port[i]);
        fputs(buf, pg_conf);

        /*
         * Cluster uses 2PC for write transactions involving multiple nodes
         * This has to be set at least to a value corresponding to the maximum
         * number of tests run in parallel.
         */
        fputs("max_prepared_transactions = 50\n", pg_conf);

        /* Set GTM connection information */
        fputs("gtm_host = 'localhost'\n", pg_conf);
        (void)snprintf(buf, sizeof(buf), "gtm_port = %d\n", myinfo.gtm_port);
        fputs(buf, pg_conf);

        (void)snprintf(buf, sizeof(buf), "pooler_port = %d\n", myinfo.co_pool_port[i]);
        fputs(buf, pg_conf);

        /*Turn on the guc control support_extended_features*/
        fputs("support_extended_features = on\n", pg_conf);

        /* make enable_fast_query_shipping to true */
        fputs("enable_fast_query_shipping = on\n", pg_conf);

        /* always use LLVM optimization */
        fputs("codegen_cost_threshold=0\n", pg_conf);

#ifdef USE_ASSERT_CHECKING
        if (g_enable_segment) {
            fputs("enable_segment = on\n", pg_conf);
        }
#endif
        if (dop > 0) {
            (void)snprintf(buf, sizeof(buf), "query_dop = %d\n", dop);
            fputs(buf, pg_conf);
        }

        if (temp_config != NULL) {
            FILE* extra_conf = NULL;
            char line_buf[1024];

            extra_conf = fopen(temp_config, "r");
            if (extra_conf == NULL) {
                fprintf(stderr, _("\n%s: could not open \"%s\" to read extra config: %s\n"),
                    progname, temp_config, strerror(errno));
                exit_nicely(2);
            }
            while (fgets(line_buf, sizeof(line_buf), extra_conf) != NULL) {
                fputs(line_buf, pg_conf);
            }
            fclose(extra_conf);
        }

        fclose(pg_conf);

        memset(local_ip, 0, sizeof(local_ip));
        if (0 != get_local_ip(local_ip)) {
            strncpy(local_ip, "localhost", LOCAL_IP_LEN);
        }

        (void)snprintf(buf, sizeof(buf), "%s/%s/pg_hba.conf", temp_install, data_folder);
        pg_conf = fopen(buf, "a");
        if (pg_conf == NULL) {
            fprintf(
                stderr, _("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
            exit_nicely(2);
        }
        fputs("\n# Configuration added by pg_regress\n\n", pg_conf);

        (void)snprintf(buf, sizeof(buf), " host all all %s/32 trust\n", local_ip);
        fputs(buf, pg_conf);

        fclose(pg_conf);
        free(data_folder);
    }
}

static void config_dn(bool standby)
{
    int i;
    char local_ip[LOCAL_IP_LEN];
    for (i = 0; i < myinfo.dn_num; i++) {
        char* data_folder = get_node_info_name(i, DATANODE, false);
        FILE* pg_conf = NULL;
        char buf[MAXPGPATH * 4];

        (void)snprintf(buf, sizeof(buf), "%s/%s/postgresql.conf", temp_install, data_folder);
        pg_conf = fopen(buf, "a");
        if (pg_conf == NULL) {
            fprintf(
                stderr, _("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
            exit_nicely(2);
        }
        fputs("\n# Configuration added by pg_regress\n\n", pg_conf);

        (void)snprintf(buf, sizeof(buf), "port = %d\n", myinfo.dn_port[i]);
        fputs(buf, pg_conf);

        /*
         * Cluster uses 2PC for write transactions involving multiple nodes
         * This has to be set at least to a value corresponding to the maximum
         * number of tests run in parallel.
         */
        fputs("max_prepared_transactions = 50\n", pg_conf);

        if (!test_single_node) {
            (void)snprintf(buf, sizeof(buf), "pooler_port = %d\n", myinfo.dn_pool_port[i]);
            fputs(buf, pg_conf);

            if (comm_tcp_mode == false)
                fputs("comm_tcp_mode = false\n", pg_conf);
        }

        /*Turn on the guc control support_extended_features*/
        fputs("support_extended_features = on\n", pg_conf);

        if (!test_single_node) {
            /* make enable_fast_query_shipping to true */
            fputs("enable_fast_query_shipping = on\n", pg_conf);
        }

        /* always use LLVM optimization */
        fputs("codegen_cost_threshold=0\n", pg_conf);

#ifdef USE_ASSERT_CHECKING
        if (g_enable_segment) {
            fputs("enable_segment = on\n", pg_conf);
        }
#endif

        if (dop > 0) {
            (void)snprintf(buf, sizeof(buf), "query_dop = %d\n", dop);
            fputs(buf, pg_conf);
        }

        if (!test_single_node) {
            /* Set GTM connection information */
            fputs("gtm_host = 'localhost'\n", pg_conf);
            (void)snprintf(buf, sizeof(buf), "gtm_port = %d\n", myinfo.gtm_port);
            fputs(buf, pg_conf);
        }

        if (standby) {
            (void)snprintf(buf, sizeof(buf),
                "replconninfo1 = 'localhost=127.0.0.1 localport=%d remotehost=127.0.0.1 remoteport=%d'\n",
                myinfo.dn_primary_port[i], myinfo.dn_standby_port[i]);
            fputs(buf, pg_conf);

            (void)snprintf(buf, sizeof(buf),
                "replconninfo2 = 'localhost=127.0.0.1 localport=%d remotehost=127.0.0.1 remoteport=%d'\n",
                myinfo.dn_primary_port[i], myinfo.dn_secondary_port[i]);
            fputs(buf, pg_conf);
        }

        if (temp_config != NULL) {
            FILE* extra_conf = NULL;
            char line_buf[1024];

            extra_conf = fopen(temp_config, "r");
            if (extra_conf == NULL) {
                fprintf(stderr, _("\n%s: could not open \"%s\" to read extra config: %s\n"),
                    progname, temp_config, strerror(errno));
                exit_nicely(2);
            }
            while (fgets(line_buf, sizeof(line_buf), extra_conf) != NULL) {
                if (test_single_node && (strncmp(line_buf, "comm_tcp_mode", 13) == 0 ||
                    strncmp(line_buf, "enable_stateless_pooler_reuse", 29) == 0)) {
                    continue;
                }
                fputs(line_buf, pg_conf);
            }
            fclose(extra_conf);
        }

        fclose(pg_conf);

        memset(local_ip, 0, sizeof(local_ip));
        if (0 != get_local_ip(local_ip)) {
            strncpy(local_ip, "localhost", LOCAL_IP_LEN);
        }

        (void)snprintf(buf, sizeof(buf), "%s/%s/pg_hba.conf", temp_install, data_folder);
        pg_conf = fopen(buf, "a");
        if (pg_conf == NULL) {
            fprintf(
                stderr, _("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
            exit_nicely(2);
        }
        fputs("\n# Configuration added by pg_regress\n\n", pg_conf);

        (void)snprintf(buf, sizeof(buf), " host all all %s/32 trust\n", local_ip);
        fputs(buf, pg_conf);

        fclose(pg_conf);
        free(data_folder);
    }
}

static void config_standby()
{
    int i;
    for (i = 0; i < myinfo.dn_num; i++) {
        char* data_folder = get_node_info_name(i, DATANODE, false);
        FILE* pg_conf = NULL;
        char buf[MAXPGPATH * 4];

        (void)snprintf(buf, sizeof(buf), "%s/%s_standby/postgresql.conf", temp_install, data_folder);
        pg_conf = fopen(buf, "a");
        if (pg_conf == NULL) {
            fprintf(
                stderr, _("\n%s: could not open \"%s\" for adding extra config: %s\n"), progname, buf, strerror(errno));
            exit_nicely(2);
        }
        fputs("\n# Configuration added by pg_regress\n\n", pg_conf);

        (void)snprintf(buf, sizeof(buf), "port = %d\n", myinfo.dns_port[i]);
        fputs(buf, pg_conf);

        (void)snprintf(buf, sizeof(buf),
            "replconninfo1 = 'localhost=127.0.0.1 localport=%d remotehost=127.0.0.1 remoteport=%d'\n",
            myinfo.dn_standby_port[i], myinfo.dn_primary_port[i]);
        fputs(buf, pg_conf);

        (void)snprintf(buf, sizeof(buf),
            "replconninfo2 = 'localhost=127.0.0.1 localport=%d remotehost=127.0.0.1 remoteport=%d'\n",
            myinfo.dn_standby_port[i], myinfo.dn_secondary_port[i]);
        fputs(buf, pg_conf);

        if (temp_config != NULL) {
            FILE* extra_conf = NULL;
            char line_buf[1024];

            extra_conf = fopen(temp_config, "r");
            if (extra_conf == NULL) {
                fprintf(stderr, _("\n%s: could not open \"%s\" to read extra config: %s\n"),
                    progname, temp_config, strerror(errno));
                exit_nicely(2);
            }
            while (fgets(line_buf, sizeof(line_buf), extra_conf) != NULL) {
                fputs(line_buf, pg_conf);
            }
            fclose(extra_conf);
        }

        fclose(pg_conf);
        free(data_folder);
    }
}

static void psql_command(const char* database, const char* query, ...)
{
    char query_formatted[1024];
    char query_escaped[2048];
    char psql_cmd[MAXPGPATH + 2048];
    va_list args;
    char* s = NULL;
    char* d = NULL;

    /* Generate the query with insertion of sprintf arguments */
    va_start(args, query);
    (void)vsnprintf(query_formatted, sizeof(query_formatted), query, args);
    va_end(args);

    /* Now escape any shell double-quote metacharacters */
    d = query_escaped;
    for (s = query_formatted; *s; s++) {
        if (strchr("\\\"$`", *s)) {
            *d++ = '\\';
        }
        *d++ = *s;
    }
    *d = '\0';

    int port = test_single_node ? myinfo.dn_port[0] : myinfo.co_port[0];

    /* And now we can build and execute the shell command */
    (void)snprintf(psql_cmd,
        sizeof(psql_cmd),
        SYSTEMQUOTE "\"%s%sgsql\" -X %s -p %d -c \"%s\" \"%s\"" SYSTEMQUOTE,
        psqldir ? psqldir : "",
        psqldir ? "/" : "",
        super_user_altered ? "" : (passwd_altered ? "-U upcheck -W Gauss@123" : "-U upcheck -W gauss@123"),
        port,
        query_escaped,
        database);

    if (regr_system(psql_cmd) != 0) {
        /* psql probably already reported the error */
        fprintf(stderr, _("command failed: %s\n"), psql_cmd);
        exit_nicely(2);
    }
}

/*
 * Issue a command via psql, connecting to the specified database on wanted node
 * Since we use system(), this doesn't return until the operation finishes
 * This code is duplicated with psql_command but this way code impact is limited.
 */
static void psql_command_node(const char* database, int i, int type, const char* query, ...)
{
    char query_formatted[1024];
    char query_escaped[2048];
    char psql_cmd[MAXPGPATH + 2048];
    va_list args;
    char* s = NULL;
    char* d = NULL;

    /* Generate the query with insertion of sprintf arguments */
    va_start(args, query);
    (void)vsnprintf((char*)query_formatted, sizeof(query_formatted), query, args);
    va_end(args);

    /* Now escape any shell double-quote metacharacters */
    d = query_escaped;
    for (s = query_formatted; *s; s++) {
        if (strchr("\\\"$`", *s)) {
            *d++ = '\\';
        }
        *d++ = *s;
    }
    *d = '\0';
    int port = test_single_node ? myinfo.dn_port[0] : get_port_number(i, type);
    /* And now we can build and execute the shell command */
    (void)snprintf(psql_cmd,
        sizeof(psql_cmd),
        SYSTEMQUOTE "\"%s%sgsql\" %s %s -X -p %d -c \"%s\" \"%s\"" SYSTEMQUOTE,
        psqldir ? psqldir : "",
        psqldir ? "/" : "",
        super_user_altered ? "" : (passwd_altered ? "-U upcheck -W Gauss@123" : "-U upcheck -W gauss@123"),
        (type == DATANODE && !super_user_altered) ? "-m" : "",
        port,
        query_escaped,
        database);

    if (regr_system(psql_cmd) != 0) {
        /* psql probably already reported the error */
        fprintf(stderr, _("command failed: %s\n"), psql_cmd);
        exit_nicely(2);
    }
}

/*
 * strdup() and malloc() replacements that prints an error and exits
 * if something goes wrong. Can never return NULL.
 */
static char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (!result) {
        fprintf(stderr, _("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

/*
 * find the current user
 *
 * on unix make sure it isn't really root
 */
static char* get_id(void)
{
    struct passwd* pw;

    if (geteuid() == 0) /* 0 is root's uid */
    {
        fprintf(stderr,
            _("%s: cannot be run as root\n"
              "Please log in (using, e.g., \"su\") as the "
              "(unprivileged) user that will\n"
              "own the server process.\n"),
            progname);
        exit(1);
    }

    pw = getpwuid(geteuid());
    if (!pw) {
        fprintf(stderr, _("%s: could not obtain information about current user: %s\n"), progname, strerror(errno));
        exit(1);
    }

    return xstrdup(pw->pw_name);
}

static void rebuild_node_group()
{
    int i = 0;
    int j = 0;
    char nname_list[256];
    char create_nodegroup[512];
    char drop_nodegroup[512];
    char* pname = NULL;

    sprintf(nname_list, "(");
    for (j = 0; j < myinfo.dn_num; j++) {
        pname = get_node_info_name(j, DATANODE, true);
        strcat(nname_list, pname);
        if (j < myinfo.dn_num - 1)
            strcat(nname_list, ",");
        free(pname);
        pname = NULL;
    }
    strcat(nname_list, ")");

    sprintf(drop_nodegroup, "DROP NODE GROUP group1");
    sprintf(create_nodegroup, "CREATE NODE GROUP group1 WITH %s", nname_list);
    fprintf(stderr, "%s\n", drop_nodegroup);
    fprintf(stderr, "%s\n", create_nodegroup);
    for (i = 0; i < myinfo.co_num; i++) {
        psql_command_node("postgres", i, COORD, "%s", drop_nodegroup);
        psql_command_node("postgres", i, COORD, "%s", create_nodegroup);
    }

    for (i = 0; i < myinfo.co_num; i++) {
        psql_command_node("postgres", i, COORD, "SELECT pgxc_pool_reload();");
        psql_command_node("postgres",
            i,
            COORD,
            "SELECT oid, node_name, node_type,"
            "node_host, node_port, node_host1, node_port1, "
            "sctp_port, control_port, sctp_port1, control_port1 "
            "from pgxc_node;");
    }
}

/*
 * Setup connection information to remote nodes for coordinator running regression
 */
static void setup_connection_information(bool standby)
{

    header(_("setting connection information"));
    /* Datanodes on Coordinator 1*/
    header(_("sleeping"));

    if (temp_config && strcmp(temp_config, "./make_wlmcheck_postgresql.conf") == 0)
        sleep(25);

    sleep(60);

    int i, j;
    char nname_list[256], sql_tmp[512];
    char* pname = NULL;
    char local_ip[LOCAL_IP_LEN];

    memset(local_ip, 0, sizeof(local_ip));
    if (0 != get_local_ip(local_ip)) {
        strncpy(local_ip, "localhost", LOCAL_IP_LEN);
    }

    for (i = 0; i < myinfo.co_num; i++) {
        for (j = 0; j < myinfo.dn_num; j++) {
            char* node_info_name = get_node_info_name(j, DATANODE, true);
            if (standby) {
                psql_command_node("postgres",
                    i,
                    COORD,
                    "CREATE NODE %s WITH (type='datanode', "
                    "host='%s', port=%d, host1='%s', port1=%d, "
                    "sctp_port=%d, control_port=%d, sctp_port1=%d, control_port1=%d);",
                    node_info_name,
                    "localhost",
                    get_port_number(j, DATANODE),
                    "localhost",
                    myinfo.dns_port[j],
                    myinfo.dn_sctp_port[j],
                    myinfo.dn_ctl_port[j],
                    myinfo.dns_sctp_port[j],
                    myinfo.dns_ctl_port[j]);
            } else {
                psql_command_node("postgres",
                    i,
                    COORD,
                    "CREATE NODE %s WITH (type='datanode', "
                    "host='%s', port=%d, "
                    "sctp_port=%d, control_port=%d);",
                    node_info_name,
                    "localhost",
                    get_port_number(j, DATANODE),
                    myinfo.dn_sctp_port[j],
                    myinfo.dn_ctl_port[j]);
            }
            free(node_info_name);
        }
    }
    sprintf(nname_list, "(");

    for (j = 0; j < myinfo.dn_num; j++) {
        pname = get_node_info_name(j, DATANODE, true);
        strcat(nname_list, pname);
        if (j < myinfo.dn_num - 1)
            strcat(nname_list, ",");
        free(pname);
        pname = NULL;
    }
    strcat(nname_list, ")");
    sprintf(sql_tmp, "CREATE NODE GROUP group1 WITH %s", nname_list);
    fprintf(stderr, "%s\n", sql_tmp);
    for (i = 0; i < myinfo.co_num; i++)
        psql_command_node("postgres", i, COORD, "%s", sql_tmp);

    header(_("sleeping"));
    sleep(10);

    /* Remote Coordinator on other Coordinator  */
    for (i = 0; i < myinfo.co_num; i++) {
        for (j = 0; j < myinfo.co_num; j++) {
            if (j != i) {
                const char* central_node = "";

                if (j == 1)
                    central_node = ", NODEIS_CENTRAL=true";

                char* node_info_name = get_node_info_name(j, COORD, true);
                psql_command_node("postgres",
                    i,
                    COORD,
                    "CREATE NODE %s WITH (HOST = '%s',"
                    " type = 'coordinator', PORT = %d,"
                    " sctp_port = %d, control_port = %d%s);",
                    node_info_name,
                    "localhost",
                    get_port_number(j, COORD),
                    myinfo.co_sctp_port[j],
                    myinfo.co_ctl_port[j],
                    central_node);
                free(node_info_name);
            }
        }
    }

    /* We only need do this in MAKEFASTCHECK */
    if (change_password) {
        char* current_user = get_id();
        sleep(10);
        psql_command("postgres",
            "Alter user \"%s\" identified by 'Gauss@123' replace 'gauss@123';",
            inplace_upgrade ? "upcheck" : current_user);
        free(current_user);
        passwd_altered = true;
    }
    /* Then reload the connection data */
    for (i = 0; i < myinfo.co_num; i++) {
        psql_command_node("postgres",
            i,
            COORD,
            "%s",
            "UPDATE pgxc_group SET is_installation = true "
            "WHERE group_name = 'group1'");
        psql_command_node("postgres", i, COORD, "SELECT pgxc_pool_reload();");
        psql_command_node("postgres",
            i,
            COORD,
            "SELECT oid, node_name, node_type,"
            "node_host, node_port, node_host1, node_port1, "
            "sctp_port, control_port, sctp_port1, control_port1 "
            "from pgxc_node;");
    }
}

/*
 * Check if given node has failed during startup
 */
static void check_node_fail(int i, int type)
{
    PID_TYPE pid_number = INVALID_PID;
    switch (type) {
        case DATANODE:
            pid_number = myinfo.dn_pid[i];
            break;
        case GTM:
            pid_number = myinfo.gtm_pid;
            break;
        case COORD:
            pid_number = myinfo.co_pid[i];
            break;
    }

#ifndef WIN32
    if (kill(pid_number, 0) != 0)
#else
    if (WaitForSingleObject(pid_number, 0) == WAIT_OBJECT_0)
#endif /* WIN32 */
    {
        fprintf(stderr,
            _("\n%s: postmaster failed for %s%d.\nExamine %s/log/postmaster_%d.log for the reason\n"),
            progname,
            type == GTM ? "gtm" : (type == COORD ? "Coordinator" : "Datanode"),
            i + 1,
            outputdir,
            pid_number);
        exit_nicely(2);
    }
}

/*
 * Kill given node but do not exit
 */
static void kill_node(int i, int type)
{
    PID_TYPE pid_number = INVALID_PID;
    switch (type) {
        case DATANODE:
            pid_number = myinfo.co_pid[i];
            break;
        case GTM:
            pid_number = myinfo.gtm_pid;
            break;
        case COORD:
            pid_number = myinfo.dn_pid[i];
            break;
        case SHELL:
            pid_number = myinfo.shell_pid[i];
            break;
    }

    fprintf(stderr,
        _("\n%s: postmaster did not respond within 60 seconds\nExamine %s/log/postmaster_%d.log for the reason\n"),
        progname,
        outputdir,
        pid_number);

#ifndef WIN32
    if (kill(pid_number, SIGKILL) != 0 && errno != ESRCH)
        fprintf(stderr, _("\n%s: could not kill failed postmaster: %s\n"), progname, strerror(errno));
#else
    if (TerminateProcess(pid_number, 255) == 0)
        fprintf(stderr, _("\n%s: could not kill failed postmaster: %lu\n"), progname, GetLastError());
#endif
}
#endif

/*
 * Always exit through here, not through plain exit(), to ensure we make
 * an effort to shut down a temp postmaster
 */
void exit_nicely(int code)
{
    if (temp_install && clean) {
        stop_postmaster();
    }
    if (temp_install) {
        free(temp_install);
        temp_install = NULL;
    }
    exit(code);
}

/*
 * Check whether string matches pattern
 *
 * In the original shell script, this function was implemented using expr(1),
 * which provides basic regular expressions restricted to match starting at
 * the string start (in conventional regex terms, there's an implicit "^"
 * at the start of the pattern --- but no implicit "$" at the end).
 *
 * For now, we only support "." and ".*" as non-literal metacharacters,
 * because that's all that anyone has found use for in resultmap.  This
 * code could be extended if more functionality is needed.
 */
static bool string_matches_pattern(const char* str, const char* pattern)
{
    while (*str && *pattern) {
        if (*pattern == '.' && pattern[1] == '*') {
            pattern += 2;
            /* Trailing .* matches everything. */
            if (*pattern == '\0') {
                return true;
            }

            /*
             * Otherwise, scan for a text position at which we can match the
             * rest of the pattern.
             */
            while (*str) {
                /*
                 * Optimization to prevent most recursion: don't recurse
                 * unless first pattern char might match this text char.
                 */
                if (*str == *pattern || *pattern == '.') {
                    if (string_matches_pattern(str, pattern)) {
                        return true;
                    }
                }

                str++;
            }

            /*
             * End of text with no match.
             */
            return false;
        } else if (*pattern != '.' && *str != *pattern) {
            /*
             * Not the single-character wildcard and no explicit match? Then
             * time to quit...
             */
            return false;
        }

        str++;
        pattern++;
    }

    if (*pattern == '\0') {
        return true;
    } /* end of pattern, so declare match */

    /* End of input string.  Do we have matching pattern remaining? */
    while (*pattern == '.' && pattern[1] == '*') {
        pattern += 2;
    }
    if (*pattern == '\0') {
        return true;
    } /* end of pattern, so declare match */

    return false;
}

/*
 * Replace all occurances of a string in a string with a different string.
 * NOTE: Assumes there is enough room in the target buffer!
 */
void replace_string(char* string, char* replace, char* replacement)
{
    char* ptr = NULL;

    while ((ptr = strstr(string, replace)) != NULL) {
        char* cdup = strdup(string);

        (void)strlcpy(string, cdup, ptr - string + 1);
        strcat(string, replacement);
        strcat(string, cdup + (ptr - string) + strlen(replace));
        free(cdup);
    }
}

/*
 * Check for sub directories within the input folders and convert the
 * .source files to appropriate .sql files within the sql folder.
 *
 * Return true if the object is an existing directory otherwise return false
 *
 */
static bool CheckForSubdirAndConvertSourceFiles(char* source, char* destination, char* pcSourceSubdir, char* pcDestDir,
    char* pcDestSubdir, char* indir, char* object, char* suffix)
{
    (void)snprintf(source, MAXPGPATH, "%s/%s", indir, object);
    if (!directory_exists(source))
        return false;

    /* If the directory name is starting with '.' char, then it is meta folder
     * like .svn , .gitignore etc. These folders can't be considered for test
     * script scanning. So skip them and return false indicating sub directory
     * is not found for 'pcSourceSubdir' */
    if (*object == '.')
        return false;

    sprintf(source, "%s/%s", pcSourceSubdir, object);
    sprintf(destination, "%s/%s", pcDestSubdir, object);
    convertSourcefilesIn(source, pcDestDir, destination, suffix);
    return true;
}

bool is_cmdline(const char* src)
{
    const char* ptr = src;

    while (isspace(*ptr)) {
        ptr++;
    }
    return (strncmp(ptr, "@@", 2) == 0);
}

static void get_cmdline(const char* src, char* dsr)
{
    const char* ptr = src;
    while (isspace(*ptr)) {
        ptr++;
    }
    if (!is_cmdline(ptr)) {
        return;
    }
    ptr += 2;
    strcpy(dsr, ptr);
}

static void exec_cmds_from_inputfiles()
{
    char indir[MAXPGPATH];
    struct stat st;
    int ret;
    char** name;
    char** names;
    int count = 0;
    FILE* infile = NULL;

    (void)snprintf(indir, MAXPGPATH, "%s/%s", inputdir, "input");

    /* Check that indir actually exists and is a directory */
    ret = stat(indir, &st);
    if (ret != 0 || !S_ISDIR(st.st_mode)) {
        /*
         * No warning, to avoid noise in tests that do not have these
         * directories; for example, ecpg, contrib and src/pl.
         */
        return;
    }

    names = pgfnames(indir);
    if (!names)
    /* Error logged in pgfnames */
    {
        exit_nicely(2);
    }

    /* finally loop on each file and do the replacement */
    for (name = names; *name; name++) {
        char srcfile[MAXPGPATH + MAXPGPATH + 2];

        char prefix[MAXPGPATH];

        char line[1024];

        /* reject filenames not finishing in ".source" */
        if (strlen(*name) < 8) {
            continue;
        }
        if (strcmp(*name + strlen(*name) - 7, ".source") != 0) {
            continue;
        }

        count++;

        /* build the full actual paths to open */
        (void)snprintf(prefix, strlen(*name) - 6, "%s", *name);
        (void)snprintf(srcfile, MAXPGPATH + strlen(*name) + 2, "%s/%s", indir, *name);

        infile = fopen(srcfile, "r");
        if (!infile) {
            fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, srcfile, strerror(errno));
            exit_nicely(2);
        }

        while (fgets(line, sizeof(line), infile)) {
            if (is_cmdline(line)) {
                char* cmdline = (char*)malloc(1024);
                get_cmdline(line, cmdline);
                replace_string(cmdline, "@login_user@", loginuser);
                replace_string(cmdline, "@abs_bindir@", bindir);
                replace_string(cmdline, "@abs_srcdir@", inputdir);
                replace_string(cmdline, "@abs_builddir@", outputdir);
                replace_string(cmdline, "@hdfshostname@", hdfshostname);
                replace_string(cmdline, "@hdfsstoreplus@", hdfsstoreplus);
                replace_string(cmdline, "@hdfscfgpath@", hdfscfgpath);
                replace_string(cmdline, "@hdfsport@", hdfsport);
                replace_string(cmdline, "@obshostname@", obshostname);
                replace_string(cmdline, "@obsbucket@", obsbucket);
                replace_string(cmdline, "@ak@", ak);
                replace_string(cmdline, "@sk@", sk);
                replace_string(cmdline, "@aie_host@", g_aiehost);
                replace_string(cmdline, "@aie_port@", g_aieport);
                replace_string(cmdline, "@abs_gausshome@", gausshomedir);
                replace_string(cmdline, "@pgbench_dir@", pgbenchdir);
                myinfo.shell_pid[myinfo.shell_count++] = spawn_process(cmdline);
                free(cmdline);
            }
        }
        fclose(infile);
    }

    /*
     * If we didn't process any files, complain because it probably means
     * somebody neglected to pass the needed --inputdir argument.
     */
    if (count <= 0) {
        fprintf(stderr, _("%s: no *.source files found in \"%s\"\n"), progname, indir);
        exit_nicely(2);
    }

    pgfnames_cleanup(names);
}

// Get start node SQL command
//
static char* GetStartNodeCmdString(int i, int type)
{
    int port_number;
    const char* data_folder = get_node_info_name(i, type, false);
    if (type == COORD) {
        port_number = test_single_node ? 0 : myinfo.co_port[i];
    } else {
        port_number = myinfo.dn_port[i];
    }

    // PID_TYPE	node_pid;
    char* buf = (char*)malloc(MAXPGPATH * 4);
    MemSet(buf, 0, MAXPGPATH * 4);
    (void)snprintf(buf,
        MAXPGPATH * 4,
        SYSTEMQUOTE "\\! \"%s/gaussdb\" %s -i -p %d -D %s/%s >>startup.log 2>&1 &" SYSTEMQUOTE,
        bindir,
        (type == COORD) ? "\"--coordinator\"" : "\"--datanode\"",
        port_number,
        temp_install,
        data_folder);
    free((char*)data_folder);
    return buf;
}

// Get pgxc_clean node SQL command
//
static char* GetPgxcCleanCmdString(int i, int type)
{
    int port_number;
    if (type == COORD) {
        port_number = test_single_node ? 0 : myinfo.co_port[i];
    } else {
        port_number = myinfo.dn_port[i];
    }

    // PID_TYPE	node_pid;
    char* buf = (char*)malloc(MAXPGPATH * 4);
    MemSet(buf, 0, MAXPGPATH * 4);
    (void)snprintf(buf,
        MAXPGPATH * 4,
        SYSTEMQUOTE "\\! \"%s/gs_clean\" -s -v -a -p %d -r >>gs_clean_test.log 2>&1 &" SYSTEMQUOTE,
        bindir,
        port_number);
    return buf;
}

/* Get gs_gtm node SQL command */
static char* GetStartGtmCmdString(int i, int type)
{
    const char* data_folder = get_node_info_name(i, type, false);
    if (*data_folder == '\0')
        return NULL;
    char* buf = (char*)malloc(MAXPGPATH * 4);
    MemSet(buf, 0, MAXPGPATH * 4);
    (void)snprintf(buf,
        MAXPGPATH * 4,
        SYSTEMQUOTE "\\! \"%s/gs_gtm\" -r -n gtm -D %s/%s -p %d" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder,
        myinfo.gtm_port);
    free((char*)data_folder);
    return buf;
}

/* Get gtm_ctl  SQL command to stop GTM */
static char* GetStopGtmCmdString(int i, int type)
{
    const char* data_folder = get_node_info_name(i, type, false);
    if (*data_folder == '\0')
        return NULL;
    char* buf = (char*)malloc(MAXPGPATH * 4);
    MemSet(buf, 0, MAXPGPATH * 4);
    (void)snprintf(buf,
        MAXPGPATH * 4,
        SYSTEMQUOTE "\\! \"%s/gtm_ctl\" stop -Z gtm -D %s/%s -m i >>gtm.log 2>&1 &" SYSTEMQUOTE,
        bindir,
        temp_install,
        data_folder);
    free((void*)data_folder);
    return buf;
}

/*
 * Convert *.source found in the "source" directory, replacing certain tokens
 * in the file contents with their intended values, and put the resulting files
 * in the "dest" directory, replacing the ".source" prefix in their names with
 * the given suffix.
 */
static void convertSourcefilesIn(char* pcSourceSubdir, char* pcDestDir, char* pcDestSubdir, char* suffix)
{
    char testtablespace[MAXPGPATH];
    char testtablespace_my[MAXPGPATH];
    char indir[MAXPGPATH];
    struct stat st;
    int iIter;
    int ret;
    char* datadirPath = NULL;
    char** name;
    char** names;
    int count = 0;
    bool bIisContainingDir = false;

    (void)snprintf(indir, MAXPGPATH, "%s/%s", inputdir, pcSourceSubdir);

    /* Check that indir actually exists and is a directory */
    ret = stat(indir, &st);
    if (ret != 0 || !S_ISDIR(st.st_mode)) {
        /*
         * No warning, to avoid noise in tests that do not have these
         * directories; for example, ecpg, contrib and src/pl.
         */
        return;
    }

    names = pgfnames(indir);
    if (!names)
    /* Error logged in pgfnames */
    {
        exit_nicely(2);
    }

    (void)snprintf(testtablespace, MAXPGPATH, "%s/testtablespace", outputdir);
    (void)snprintf(testtablespace_my, MAXPGPATH, "%s/gausstablespace", outputdir);

#ifdef WIN32

    /*
     * On Windows only, clean out the test tablespace dir, or create it if it
     * doesn't exist.  On other platforms we expect the Makefile to take care
     * of that.  (We don't migrate that functionality in here because it'd be
     * harder to cope with platform-specific issues such as SELinux.)
     *
     * XXX it would be better if pg_regress.c had nothing at all to do with
     * testtablespace, and this were handled by a .BAT file or similar on
     * Windows.  See pgsql-hackers discussion of 2008-01-18.
     */
    if (directory_exists(testtablespace))
        rmtree(testtablespace, true);
    make_directory(testtablespace);
    if (directory_exists(testtablespace_my))
        rmtree(testtablespace_my, true);
    make_directory(testtablespace_my);
#endif

    /* finally loop on each file and do the replacement */
    for (name = names; *name; name++) {
        char srcfile[MAXPGPATH * 2 + 2];
        char destfile[MAXPGPATH * 2 + 4];
        char prefix[MAXPGPATH];
        FILE *infile, *outfile;
        char line[1024];
        char bufTemp[MAXPGPATH];

        /* reject filenames not finishing in ".source" */
        if (strlen(*name) < 8) {
            if (true == CheckForSubdirAndConvertSourceFiles(
                            srcfile, destfile, pcSourceSubdir, pcDestDir, pcDestSubdir, indir, *name, suffix))
                bIisContainingDir = true;
            continue;
        }

        if (strcmp(*name + strlen(*name) - 7, ".source") != 0) {
            if (true == CheckForSubdirAndConvertSourceFiles(
                            srcfile, destfile, pcSourceSubdir, pcDestDir, pcDestSubdir, indir, *name, suffix))
                bIisContainingDir = true;

            continue;
        }

        count++;

        /* build the full actual paths to open */
        (void)snprintf(prefix, strlen(*name) - 6, "%s", *name);
        (void)snprintf(srcfile, MAXPGPATH + strlen(*name) + 2, "%s/%s", indir, *name);
        (void)snprintf(destfile, MAXPGPATH * 2 + 4, "%s/%s/%s.%s", pcDestDir, pcDestSubdir, prefix, suffix);

        infile = fopen(srcfile, "r");
        if (!infile) {
            fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, srcfile, strerror(errno));
            exit_nicely(2);
        }
        outfile = fopen(destfile, "w");
        if (!outfile) {
            fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"), progname, destfile, strerror(errno));
            exit_nicely(2);
        }
        add_stringlist_item(&destination_files, destfile);
        while (fgets(line, sizeof(line), infile)) {
            if (!is_cmdline(line)) {
                for (iIter = 0; iIter < g_stRegrReplcPatt.iNumOfPatterns; iIter++)
                    replace_string(line,
                        &g_stRegrReplcPatt.pcBuf[g_stRegrReplcPatt.puiPatternOffset[iIter]],
                        &g_stRegrReplcPatt.pcBuf[g_stRegrReplcPatt.puiPattReplValOffset[iIter]]);

                replace_string(line, "@login_user@", loginuser);
                replace_string(line, "@abs_srcdir@", inputdir);
                replace_string(line, "@abs_builddir@", outputdir);
                replace_string(line, "@abs_bindir@", psqldir);
                replace_string(line, "@testtablespace@", testtablespace);
                replace_string(line, "@gausstablespace@", testtablespace_my);
                replace_string(line, "@libdir@", dlpath);
                replace_string(line, "@DLSUFFIX@", DLSUFFIX);
                replace_string(line, "@gsqldir@", psqldir);
                replace_string(line, "@datanode1@", get_node_info_name(0, DATANODE, true));
                replace_string(line, "@datanode2@", get_node_info_name(1, DATANODE, true));
                replace_string(line, "@coordinator1@", get_node_info_name(0, COORD, true));
                replace_string(line, "@coordinator2@", get_node_info_name(1, COORD, true));
                replace_string(line, "@pgbench_dir@", pgbenchdir);
                replace_string(line, "@client_logic_hook@", client_logic_hook);
                char* ptr = GetStartNodeCmdString(0, DATANODE);
                replace_string(line, "@start_datanode1@", ptr);
                free(ptr);
#ifdef ENABLE_MULTIPLE_NODES
                ptr = GetStartNodeCmdString(1, DATANODE);
                replace_string(line, "@start_datanode2@", ptr);
                free(ptr);
                ptr = GetStartNodeCmdString(0, COORD);
                replace_string(line, "@start_coordinator1@", ptr);
                free(ptr);
                ptr = GetStartNodeCmdString(1, COORD);
                replace_string(line, "@start_coordinator2@", ptr);
                free(ptr);
#endif
                ptr = GetStopGtmCmdString(0, GTM);
                replace_string(line, "@stop_gtm@", ptr);
                free(ptr);
                ptr = GetStartGtmCmdString(0, GTM);
                replace_string(line, "@start_gtm@", ptr);
                free(ptr);

                ptr = GetPgxcCleanCmdString(0, COORD);
                replace_string(line, "@start_pgxc_clean@", ptr);
                free(ptr);
                char s[16];
                sprintf(s, "%d", get_port_number(0, DATANODE));
                replace_string(line, "@portstring@", s);
                sprintf(s, "%d", get_port_number(1, COORD));
                replace_string(line, "@cn2_portstring@", s);
                if (temp_install) {
                    char tmpdatadir[1024];
                    (void)snprintf(tmpdatadir, MAXPGPATH, "\"%s\"", temp_install);
                    replace_string(line, "@cndata@", tmpdatadir);
                    (void)snprintf(bufTemp, MAXPGPATH, "\"%s/data\"", temp_install);
                    replace_string(line, "@datadir@", bufTemp);
                    (void)snprintf(bufTemp, MAXPGPATH, "\"%s/data1\"", temp_install);
                    replace_string(line, "@data1dir@", bufTemp);
                    (void)snprintf(bufTemp, MAXPGPATH, "-p%u", 0xC000 | (PG_VERSION_NUM & 0x3FFF));
                    replace_string(line, "@psqlport@", bufTemp);
                } else {
                    datadirPath = getenv("GAUSSDATA");
                    if (datadirPath) {
                        (void)snprintf(bufTemp, MAXPGPATH, "\"%s\"", datadirPath);
                        replace_string(line, "@datadir@", bufTemp);
                    } else
                        replace_string(line, "@datadir@", ".");

                    datadirPath = getenv("GAUSSDATA1");
                    if (datadirPath) {
                        (void)snprintf(bufTemp, MAXPGPATH, "\"%s\"", datadirPath);
                        replace_string(line, "@data1dir@", bufTemp);
                    } else
                        replace_string(line, "@data1dir@", ".");

                    replace_string(line, "@psqlport@", "");
                }
                replace_string(line, "@hdfshostname@", hdfshostname);
                replace_string(line, "@hdfsstoreplus@", hdfsstoreplus);
                replace_string(line, "@hdfscfgpath@", hdfscfgpath);
                replace_string(line, "@hdfsport@", hdfsport);
                replace_string(line, "@obshostname@", obshostname);
                replace_string(line, "@obsbucket@", obsbucket);
                replace_string(line, "@ak@", ak);
                replace_string(line, "@sk@", sk);
                replace_string(line, "@aie_host@", g_aiehost);
                replace_string(line, "@aie_port@", g_aieport);
                replace_string(line, "@abs_gausshome@", gausshomedir);
                sprintf(s, "%d", get_port_number(0, DATANODE));
                replace_string(line, "@dn1port@", s);

                fputs(line, outfile);
            }
        }
        fclose(infile);
        fclose(outfile);
    }

    /*
     * If we didn't process any files, complain because it probably means
     * somebody neglected to pass the needed --inputdir argument.
     */
    if (count <= 0 && !bIisContainingDir) {
        fprintf(stderr, _("%s: no *.source files found in \"%s\"\n"), progname, indir);
        exit_nicely(2);
    }

    pgfnames_cleanup(names);
}

/* Create the .sql and .out files from the .source files, if any */
static void convert_sourcefiles(void)
{
    errno_t rc = EOK;
    convertSourcefilesIn("input", inputdir, "sql", "sql");
    convertSourcefilesIn("output", outputdir, "expected", "out");

    // Convert source file to SQL file according some rules
    //
    convertSourcefilesIn(TwoPhaseTxntTestDirInput, inputdir, "sql", "sql");
    convertSourcefilesIn(TwoPhaseTxntTestDirOutput, outputdir, "expected", "out");

    if (g_stRegrReplcPatt.puiPatternOffset != NULL)
        REGR_FREE(g_stRegrReplcPatt.puiPatternOffset);

    /*
     * backup the sql directory
     */
    char buff[MAXPGPATH * 4] = {0};
    char src[MAXPGPATH] = {0};
    char dest[MAXPGPATH] = {0};
    rc = snprintf_s(src, sizeof(src), sizeof(src) - 1, "%s/sql", inputdir);
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(dest, sizeof(dest), sizeof(dest) - 1, "%s/.sql", inputdir);
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(buff, sizeof(buff), sizeof(buff) - 1, "cp -r %s %s", src, dest);
    securec_check_ss_c(rc, "", "");
    system(buff);
}

/*
 * Scan resultmap file to find which platform-specific expected files to use.
 *
 * The format of each line of the file is
 *		   testname/hostplatformpattern=substitutefile
 * where the hostplatformpattern is evaluated per the rules of expr(1),
 * namely, it is a standard regular expression with an implicit ^ at the start.
 * (We currently support only a very limited subset of regular expressions,
 * see string_matches_pattern() above.)  What hostplatformpattern will be
 * matched against is the config.guess output.	(In the shell-script version,
 * we also provided an indication of whether gcc or another compiler was in
 * use, but that facility isn't used anymore.)
 */
static void load_resultmap(void)
{
    char buf[MAXPGPATH];
    FILE* f = NULL;

    /* scan the file ... */
    (void)snprintf(buf, sizeof(buf), "%s/resultmap", inputdir);
    f = fopen(buf, "r");
    if (!f) {
        /* OK if it doesn't exist, else complain */
        if (errno == ENOENT) {
            return;
        }
        fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, buf, strerror(errno));
        exit_nicely(2);
    }

    while (fgets(buf, sizeof(buf), f)) {
        char* platform = NULL;
        char* file_type = NULL;
        char* expected = NULL;
        int i;

        /* strip trailing whitespace, especially the newline */
        i = strlen(buf);
        while (i > 0 && isspace((unsigned char)buf[i - 1])) {
            buf[--i] = '\0';
        }

        /* parse out the line fields */
        file_type = strchr(buf, ':');
        if (!file_type) {
            fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"), buf);
            exit_nicely(2);
        }
        *file_type++ = '\0';

        platform = strchr(file_type, ':');
        if (!platform) {
            fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"), buf);
            exit_nicely(2);
        }
        *platform++ = '\0';
        expected = strchr(platform, '=');
        if (!expected) {
            fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"), buf);
            exit_nicely(2);
        }
        *expected++ = '\0';

        /*
         * if it's for current platform, save it in resultmap list. Note: by
         * adding at the front of the list, we ensure that in ambiguous cases,
         * the last match in the resultmap file is used. This mimics the
         * behavior of the old shell script.
         */
        if (string_matches_pattern(host_platform, platform)) {
            _resultmap* entry = (_resultmap*)malloc(sizeof(_resultmap));

            entry->test = strdup(buf);
            entry->type = strdup(file_type);
            entry->resultfile = strdup(expected);
            entry->next = resultmap;
            resultmap = entry;
        }
    }
    fclose(f);
}

/*
 * Check in resultmap if we should be looking at a different file
 */
static const char* get_expectfile(const char* testname, const char* file)
{
    char* file_type = NULL;
    _resultmap* rm = NULL;

    /*
     * Determine the file type from the file name. This is just what is
     * following the last dot in the file name.
     */
    if (!file || !(file_type = strrchr((char*)file, '.'))) {
        return NULL;
    }

    file_type++;

    for (rm = resultmap; rm != NULL; rm = rm->next) {
        if (strcmp(testname, rm->test) == 0 && strcmp(file_type, rm->type) == 0) {
            return rm->resultfile;
        }
    }

    return NULL;
}

#ifndef PGXC
/*
 * Handy subroutine for setting an environment variable "var" to "val"
 */
static void doputenv(const char* var, const char* val)
{
    char* s = (char*)malloc(strlen(var) + strlen(val) + 2);

    sprintf(s, "%s=%s", var, val);
    putenv(s);
}
#endif

/*
 * Set the environment variable "pathname", prepending "addval" to its
 * old value (if any).
 */
/*lint -e429*/
static void add_to_path(const char* pathname, char separator, const char* addval)
{
    char* oldval = getenv(pathname);
    char* newval = NULL;

    if (!oldval || !oldval[0]) {
        /* no previous value */
        newval = (char*)malloc(strlen(pathname) + strlen(addval) + 2);
        sprintf(newval, "%s=%s", pathname, addval);
    } else {
        newval = (char*)malloc(strlen(pathname) + strlen(addval) + strlen(oldval) + 3);
        sprintf(newval, "%s=%s%c%s", pathname, addval, separator, oldval);
    }
    (void)putenv(newval);
}
/*lint +e429*/

/*
 * Prepare environment variables for running regression tests
 */
static void initialize_environment(void)
{
    if (nolocale) {
        /*
         * Clear out any non-C locale settings
         */
        unsetenv("LC_COLLATE");
        unsetenv("LC_CTYPE");
        unsetenv("LC_MONETARY");
        unsetenv("LC_NUMERIC");
        unsetenv("LC_TIME");
        unsetenv("LANG");
        /* On Windows the default locale cannot be English, so force it */
#if defined(WIN32) || defined(__CYGWIN__)
        putenv("LANG=en");
#endif
    }

    /*
     * Set translation-related settings to English; otherwise psql will
     * produce translated messages and produce diffs.  (XXX If we ever support
     * translation of pg_regress, this needs to be moved elsewhere, where psql
     * is actually called.)
     */
    unsetenv("LANGUAGE");
    unsetenv("LC_ALL");
    (void)putenv("LC_MESSAGES=C");

    /* test for a bug of psqlrc contain ~ */
    (void)putenv("PSQLRC='~/x'");

    /*
     * Set encoding as requested
     */
    if (encoding) {
        doputenv("PGCLIENTENCODING", encoding);
    } else {
        unsetenv("PGCLIENTENCODING");
    }

    /*
     * Set timezone and datestyle for datetime-related tests
     */
    (void)putenv("PGTZ=PST8PDT");
    (void)putenv("PGDATESTYLE=Postgres, MDY");

    /*
     * Likewise set intervalstyle to ensure consistent results.  This is a bit
     * more painful because we must use PGOPTIONS, and we want to preserve the
     * user's ability to set other variables through that.
     */
    /*lint -e429*/
    {
        const char* my_pgoptions = "-c intervalstyle=postgres_verbose";
        const char* old_pgoptions = getenv("PGOPTIONS");
        char* new_pgoptions = NULL;

        if (!old_pgoptions) {
            old_pgoptions = "";
        }
        new_pgoptions = (char*)malloc(strlen(old_pgoptions) + strlen(my_pgoptions) + 12);
        (void)sprintf(new_pgoptions, "PGOPTIONS=%s %s", old_pgoptions, my_pgoptions);
        (void)putenv(new_pgoptions);
    }
    /*lint +e429*/

    if (temp_install) {
        /*
         * Clear out any environment vars that might cause psql to connect to
         * the wrong postmaster, or otherwise behave in nondefault ways. (Note
         * we also use psql's -X switch consistently, so that ~/.psqlrc files
         * won't mess things up.)  Also, set PGPORT to the temp port, and set
         * or unset PGHOST depending on whether we are using TCP or Unix
         * sockets.
         */
        unsetenv("PGDATABASE");
        unsetenv("PGUSER");
        unsetenv("PGSERVICE");
        unsetenv("PGSSLMODE");
        unsetenv("PGREQUIRESSL");
        unsetenv("PGCONNECT_TIMEOUT");
        unsetenv("PGDATA");
        if (hostname != NULL) {
            doputenv("PGHOST", hostname);
        } else {
            unsetenv("PGHOST");
        }
        unsetenv("PGHOSTADDR");
        if (port != -1) {
            char s[16];

            sprintf(s, "%d", port);
            doputenv("PGPORT", s);
        }
        setBinAndLibPath(false);

    } else {
        const char* pghost = NULL;
        const char* pgport = NULL;

        /*
         * When testing an existing install, we honor existing environment
         * variables, except if they're overridden by command line options.
         */
        if (hostname != NULL) {
            doputenv("PGHOST", hostname);
            unsetenv("PGHOSTADDR");
        }
        if (port != -1) {
            char s[16];

            sprintf(s, "%d", port);
            doputenv("PGPORT", s);
        }
        if (user != NULL) {
            doputenv("PGUSER", user);
        }
        /*
         * Report what we're connecting to
         */
        pghost = getenv("PGHOST");
        pgport = getenv("PGPORT");
#ifndef HAVE_UNIX_SOCKETS
        if (!pghost) {
            pghost = "localhost";
        }
#endif

        if (pghost && pgport) {
            printf(_("(using postmaster on %s, port %s)\n"), pghost, pgport);
        }
        if (pghost && !pgport) {
            printf(_("(using postmaster on %s, default port)\n"), pghost);
        }
        if (!pghost && pgport) {
            printf(_("(using postmaster on Unix socket, port %s)\n"), pgport);
        }
        if (!pghost && !pgport) {
            printf(_("(using postmaster on Unix socket, default port)\n"));
        }
    }

    convert_sourcefiles();
    load_resultmap();
}

static void cleanup_environment()
{ 
    if (is_skip_environment_cleanup) { 
        return;
    }
    for (; destination_files != NULL; destination_files = destination_files->next) {
        unlink(destination_files->str);
    }
    for (; destination_files != NULL; destination_files = destination_files->next) {
        unlink(destination_files->str);
    }
}

#define MAKEFILE_BUF_MORE_LEN 32
static void setBinAndLibPath(bool isOld)
{
    char* tmp = NULL;
    // set to old binary
    if (isOld) {
        /*
         * Adjust path variables to point into the temp-install tree
         */
        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen("/bin"));
        sprintf(tmp, "%s/%s", temp_install, "bin");
        bindir = tmp;

        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen("/lib"));
        sprintf(tmp, "%s/%s", temp_install, "lib");
        libdir = tmp;

        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen("/share/postgresql"));
        sprintf(tmp, "%s/%s", temp_install, "share/postgresql");
        datadir = tmp;

        /* psql will be installed into temp-install bindir */
        psqldir = bindir;

        /*
         * Set up shared library paths to include the temp install.
         *
         * LD_LIBRARY_PATH covers many platforms.  DYLD_LIBRARY_PATH works on
         * Darwin, and maybe other Mach-based systems.	LIBPATH is for AIX.
         * Windows needs shared libraries in PATH (only those linked into
         * executables, not dlopen'ed ones). Feel free to account for others
         * as well.
         */
        add_to_path("LD_LIBRARY_PATH", ':', libdir);
        add_to_path("DYLD_LIBRARY_PATH", ':', libdir);
        add_to_path("LIBPATH", ':', libdir);
#if defined(WIN32)
        add_to_path("PATH", ';', libdir);
#elif defined(__CYGWIN__)
        add_to_path("PATH", ':', libdir);
#endif
    } else {  // set to new binary
        /*
         * Adjust path variables to point into the temp-install tree
         */
#ifdef BUILD_BY_CMAKE
	
        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN  + strlen(CMAKE_PGBINDIR));
        sprintf(tmp, "%s/install/%s/bin", temp_install, CMAKE_PGBINDIR);
        bindir = tmp; 
        printf("bindir: %s\n", bindir);
        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN  + strlen(CMAKE_LIBDIR));
        sprintf(tmp, "%s/install/%s/lib", temp_install, CMAKE_LIBDIR);
        libdir = tmp; 

        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN  + strlen(CMAKE_PGSHAREDIR));
        sprintf(tmp, "%s/install/%s/share/postgresql", temp_install, CMAKE_PGSHAREDIR);
        datadir = tmp; 

        psqldir = bindir;
#else 
        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen(PGBINDIR));
        sprintf(tmp, "%s/install/%s", temp_install, PGBINDIR);
        bindir = tmp;

        printf("bindir: %s\n", bindir);
        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen(LIBDIR));
        sprintf(tmp, "%s/install/%s", temp_install, LIBDIR);
        libdir = tmp;

        tmp = (char*)malloc(strlen(temp_install) + MAKEFILE_BUF_MORE_LEN + strlen(PGSHAREDIR));
        sprintf(tmp, "%s/install/%s", temp_install, PGSHAREDIR);
        datadir = tmp;

        psqldir = bindir;
#endif
        add_to_path("LD_LIBRARY_PATH", ':', libdir);
        add_to_path("DYLD_LIBRARY_PATH", ':', libdir);
        add_to_path("LIBPATH", ':', libdir);
#if defined(WIN32)
        add_to_path("PATH", ';', libdir);
#elif defined(__CYGWIN__)
        add_to_path("PATH", ':', libdir);
#endif
    }
}

/*
 * Spawn a process to execute the given shell command; don't wait for it
 *
 * Returns the process ID (or HANDLE) so we can wait for it later
 */
PID_TYPE
spawn_process(const char* cmdline)
{
#ifndef WIN32
    pid_t pid;

    /*
     * Must flush I/O buffers before fork.	Ideally we'd use fflush(NULL) here
     * ... does anyone still care about systems where that doesn't work?
     */
    (void)fflush(stdout);
    (void)fflush(stderr);
    if (logfile) {
        (void)fflush(logfile);
    }

    pid = fork();
    if (pid == -1) {
        fprintf(stderr, _("%s: could not fork: %s\n"), progname, strerror(errno));
        exit_nicely(2);
    }
    /*lint -e429*/
    if (pid == 0) {
        /*
         * In child
         *
         * Instead of using system(), exec the shell directly, and tell it to
         * "exec" the command too.	This saves two useless processes per
         * parallel test case.
         */
        char* cmdline2 = (char*)malloc(strlen(cmdline) + 6);

        sprintf(cmdline2, "exec %s", cmdline);
        (void)execl(shellprog, shellprog, "-c", cmdline2, (char*)NULL);
        fprintf(stderr, _("%s: could not exec \"%s\": %s\n"), progname, shellprog, strerror(errno));
        exit(1); /* not exit here... */
    }
    /*lint +e429*/
    /* in parent */
    return pid;
#else
    char* cmdline2 = NULL;
    BOOL b;
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    HANDLE origToken;
    HANDLE restrictedToken;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    SID_AND_ATTRIBUTES dropSids[2];
    __CreateRestrictedToken _CreateRestrictedToken = NULL;
    HANDLE Advapi32Handle;

    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);

    Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
    if (Advapi32Handle != NULL) {
        _CreateRestrictedToken = (__CreateRestrictedToken)GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
    }

    if (_CreateRestrictedToken == NULL) {
        if (Advapi32Handle != NULL) {
            FreeLibrary(Advapi32Handle);
        }
        fprintf(stderr, _("%s: cannot create restricted tokens on this platform\n"), progname);
        exit_nicely(2);
    }

    /* Open the current token to use as base for the restricted one */
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken)) {
        fprintf(stderr, _("could not open process token:  error code %lu\n"), GetLastError());
        exit_nicely(2);
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
        fprintf(stderr, _("could not allocate SIDs: %lu\n"), GetLastError());
        exit_nicely(2);
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
        fprintf(stderr, _("could not create restricted token:  error code %lu\n"), GetLastError());
        exit_nicely(2);
    }

    cmdline2 = malloc(strlen(cmdline) + 8);
    sprintf(cmdline2, "cmd /c %s", cmdline);

#ifndef __CYGWIN__
    AddUserToTokenDacl(restrictedToken);
#endif

    if (!CreateProcessAsUser(
            restrictedToken, NULL, cmdline2, NULL, NULL, TRUE, CREATE_SUSPENDED, NULL, NULL, &si, &pi)) {
        fprintf(stderr, _("could not start process for \"%s\": %lu\n"), cmdline2, GetLastError());
        exit_nicely(2);
    }

    free(cmdline2);

    ResumeThread(pi.hThread);
    CloseHandle(pi.hThread);
    return pi.hProcess;
#endif
}

/* start a script execution process for specified file and return process ID */
static PID_TYPE scriptExecute(
    const char* scriptname, _stringlist** resultfiles, _stringlist** expectfiles, _stringlist** tags)
{
    PID_TYPE pid;
    char infile[MAXPGPATH];
    char outfile[MAXPGPATH];
    char expectfile[MAXPGPATH];
    char scriptCmd[MAXPGPATH * 3];
    size_t offset = 0;

    /*
     * Look for files in the output dir first, consistent with a vpath search.
     * This is mainly to create more reasonable error messages if the file is
     * not found.  It also allows local test overrides when running pg_regress
     * outside of the source tree.
     */
    (void)snprintf(infile, sizeof(infile), "%s/scripts/%s", outputdir, scriptname);
    if (!file_exists(infile))
        (void)snprintf(infile, sizeof(infile), "%s/scripts/%s", inputdir, scriptname);

    (void)snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, scriptname);

    (void)snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", outputdir, scriptname);
    if (!file_exists(expectfile))
        (void)snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, scriptname);

    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    if (launcher)
        offset += snprintf(scriptCmd + offset, sizeof(scriptCmd) - offset, "%s ", launcher);

    (void)snprintf(scriptCmd + offset,
        sizeof(scriptCmd) - offset,
        SYSTEMQUOTE "\"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
        infile,
        outfile);

    pid = spawn_process(scriptCmd);
    if (pid == INVALID_PID) {
        fprintf(stderr, _("could not start process for test %s\n"), scriptname);
        exit(2);
    }

    return pid;
}

/*
 * Count bytes in file
 */
static long file_size(const char* file)
{
    long r;
    FILE* f = fopen(file, "r");

    if (!f) {
        fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, file, strerror(errno));
        return -1;
    }
    fseek(f, 0, SEEK_END);
    r = ftell(f);
    fclose(f);
    return r;
}

/*
 * Count lines in file
 */
static int file_line_count(const char* file)
{
    int c;
    int l = 0;
    FILE* f = fopen(file, "r");

    if (!f) {
        fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, file, strerror(errno));
        return -1;
    }
    while ((c = fgetc(f)) != EOF) {
        if (c == '\n') {
            l++;
        }
    }
    fclose(f);
    return l;
}

bool file_exists(const char* file)
{
    FILE* f = fopen(file, "r");

    if (!f) {
        return false;
    }
    fclose(f);
    return true;
}

static bool directory_exists(const char* dir)
{
    struct stat st;

    if (stat(dir, &st) != 0)
        return false;
    if (S_ISDIR(st.st_mode))
        return true;
    return false;
}

/* Create nested directory structure */
static void makeNestedDirectory(const char* results_file_path)
{
    char* pcOutFileLastSep = NULL;
    char outfile[MAXPGPATH];
    int iSepCount = 0;
    char cSeparator;

    (void)snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, results_file_path);

    do {
        pcOutFileLastSep = strrchr(outfile, '/');
        if (NULL == pcOutFileLastSep)
            pcOutFileLastSep = strrchr(outfile, '\\');

        if (NULL != pcOutFileLastSep) {
            cSeparator = *pcOutFileLastSep;
            *pcOutFileLastSep = '\0';
            iSepCount++;

            if (directory_exists(outfile)) {
                do {
                    *pcOutFileLastSep = cSeparator;
                    iSepCount--;

                    if ((iSepCount > 0) && (!directory_exists(outfile))) {
                        make_directory(outfile);
                        pcOutFileLastSep = outfile + strlen(outfile);
                    }
                } while (iSepCount > 0);
            }
        }
    } while (iSepCount > 0);
}

/* Create a directory */
static void make_directory(const char* dir)
{
    if (mkdir(dir, S_IRWXU | S_IRWXG | S_IRWXO) < 0) {
        fprintf(stderr, _("%s: could not create directory \"%s\": %s\n"), progname, dir, strerror(errno));
        exit_nicely(2);
    }
}

/*
 * In: filename.ext, Return: filename_i.ext, where 0 < i <= 9
 */
static char* get_alternative_expectfile(const char* expectfile, int i)
{
    char* last_dot = NULL;
    int ssize = strlen(expectfile) + 2 + 1;
    char* tmp = (char*)malloc(ssize);
    char* s = (char*)malloc(ssize);

    strcpy(tmp, expectfile);
    last_dot = strrchr(tmp, '.');
    if (!last_dot) {
        free(tmp);
        free(s);
        return NULL;
    }
    *last_dot = '\0';
    (void)snprintf(s, ssize, "%s_%d.%s", tmp, i, last_dot + 1);
    free(tmp);
    return s;
}

/*
 * Run a "diff" command and also check that it didn't crash
 */
static int run_diff(const char* cmd, const char* filename)
{
    int r;

    r = system(cmd);
    if (!WIFEXITED(r) || WEXITSTATUS(r) > 1) {
        fprintf(stderr, _("diff command failed with status %d: %s\n"), r, cmd);
        exit_nicely(2);
    }
#ifdef WIN32

    /*
     * On WIN32, if the 'diff' command cannot be found, system() returns 1,
     * but produces nothing to stdout, so we check for that here.
     */
    if (WEXITSTATUS(r) == 1 && file_size(filename) <= 0) {
        fprintf(stderr, _("diff command not found: %s\n"), cmd);
        exit_nicely(2);
    }
#endif

    return WEXITSTATUS(r);
}

/*
 * Check the actual result file for the given test against expected results
 *
 * Returns true if different (failure), false if correct match found.
 * In the true case, the diff is appended to the diffs file.
 */
static bool results_differ(const char* testname, const char* resultsfile, const char* default_expectfile)
{
    char expectfile[MAXPGPATH];
    char smartmatchfile[MAXPGPATH + 12];
    char diff[MAXPGPATH];
    char smartmatch[MAXPGPATH];
    char cmd[MAXPGPATH * 3];
    char acBestExpectFile[MAXPGPATH];
    FILE* difffile = NULL;
    int best_line_count;
    int i;
    int l;
    const char* platform_expectfile = NULL;
    int result;
    char* pcPrettyDiffFile = NULL;

    /*
     * We can pass either the resultsfile or the expectfile, they should have
     * the same type (filename.type) anyway.
     */
    platform_expectfile = get_expectfile(testname, resultsfile);

    strlcpy(expectfile, default_expectfile, sizeof(expectfile));
    if (platform_expectfile) {
        /*
         * Replace everything afer the last slash in expectfile with what the
         * platform_expectfile contains.
         */
        char* p = strrchr(expectfile, '/');

        if (p) {
            strlcpy(++p, platform_expectfile, MAXPGPATH);
        }
    }

    /* Name to use for temporary diff file */
    (void)snprintf(diff, sizeof(diff), "%s.diff", resultsfile);

    /* OK, run the diff */
    (void)snprintf(cmd,
        sizeof(cmd),
        SYSTEMQUOTE "diff %s \"%s\" \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
        basic_diff_opts,
        expectfile,
        resultsfile,
        diff);

    /* Is the diff file empty? */
    if (run_diff(cmd, diff) == 0) {
        unlink(diff);
        return false;
    }

    /* There may be secondary comparison files that match better */
    best_line_count = file_line_count(diff);
    strlcpy(acBestExpectFile, expectfile, sizeof(acBestExpectFile));

    /* framing smartmatch.pl script complete file name */
    int rc = snprintf_s(smartmatch, sizeof(smartmatch), sizeof(smartmatch) - 1, "%s/%s", outputdir, "smartmatch.pl");
    securec_check_ss_c(rc, "\0", "\0");
    if (!file_exists(smartmatch)) {
        int rc = snprintf_s(smartmatch, sizeof(smartmatch), sizeof(smartmatch) - 1, "%s/%s", inputdir, "smartmatch.pl");
        securec_check_ss_c(rc, "\0", "\0");
    }

    for (i = 0; i <= 9; i++) {
        char* alt_expectfile = NULL;

        alt_expectfile = get_alternative_expectfile(expectfile, i);
        if (!file_exists(alt_expectfile)) {
            free(alt_expectfile);
            continue;
        }

        (void)snprintf(cmd,
            sizeof(cmd),
            SYSTEMQUOTE "diff %s \"%s\" \"%s\" > \"%s\"" SYSTEMQUOTE,
            basic_diff_opts,
            alt_expectfile,
            resultsfile,
            diff);

        if (run_diff(cmd, diff) == 0) {
            unlink(diff);
            free(alt_expectfile);
            return false;
        }

        l = file_line_count(diff);
        if (l < best_line_count) {
            /* This diff was a better match than the last one */
            best_line_count = l;
            strlcpy(acBestExpectFile, alt_expectfile, sizeof(acBestExpectFile));
        }

        if (file_exists(smartmatch)) {
            /* Name to use for temporary diff file */
            rc = snprintf_s(smartmatchfile, sizeof(smartmatchfile), sizeof(smartmatchfile) - 1, "%s.smartmatch",
                acBestExpectFile);
            securec_check_ss_c(rc, "\0", "\0");
            /*
            * Is the unordered result set is leading to test failure? check and
            * verify the unordered result set as if test specifies that the result
            * can vary with comments of unordered.
            */
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, SYSTEMQUOTE "%s \"%s\" \"%s\" \"%s\" " SYSTEMQUOTE,
                smartmatch, alt_expectfile, resultsfile, smartmatchfile);
            securec_check_ss_c(rc, "\0", "\0");
            result = system(cmd);
            if (WEXITSTATUS(result) == 1) {
                /* In a success of smartmatch, there is no need of smartmatch file */
                unlink(smartmatchfile);
                unlink(diff);
                free(alt_expectfile);
                return false;
            } else if (WEXITSTATUS(result) == 2) {
                unlink(smartmatchfile);
                pcPrettyDiffFile = acBestExpectFile;
            } else
                pcPrettyDiffFile = smartmatchfile;
        } else
            pcPrettyDiffFile = acBestExpectFile;

        /*
        * Use the best comparison file to generate the "pretty" diff, which we
        * append to the diffs summary file.
        */
        rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1,
            SYSTEMQUOTE "diff %s \"%s\" \"%s\" >> \"%s\" 2>&1" SYSTEMQUOTE, pretty_diff_opts, pcPrettyDiffFile,
            resultsfile, difffilename);
        securec_check_ss_c(rc, "\0", "\0");
        (void)run_diff(cmd, difffilename);

        l = file_line_count(diff);
        if (l < best_line_count) {
            /* This diff was a better match than the last one */
            best_line_count = l;
            strlcpy(acBestExpectFile, pcPrettyDiffFile, sizeof(acBestExpectFile));
        }
        free(alt_expectfile);
    }

    /*
     * fall back on the canonical results file if we haven't tried it yet and
     * haven't found a complete match yet.
     */

    if (platform_expectfile) {
        (void)snprintf(cmd,
            sizeof(cmd),
            SYSTEMQUOTE "diff %s \"%s\" \"%s\" > \"%s\"" SYSTEMQUOTE,
            basic_diff_opts,
            default_expectfile,
            resultsfile,
            diff);

        if (run_diff(cmd, diff) == 0) {
            /* No diff = no changes = good */
            unlink(diff);
            return false;
        }

        l = file_line_count(diff);
        if (l < best_line_count) {
            /* This diff was a better match than the last one */
            best_line_count = l;
            strlcpy(acBestExpectFile, default_expectfile, sizeof(acBestExpectFile));
        }
    }

    if (file_exists(smartmatch)) {
        /* Name to use for temporary diff file */
        (void)snprintf(smartmatchfile, sizeof(smartmatchfile), "%s.smartmatch", acBestExpectFile);

        /*
         * Is the unordered result set is leading to test failure? check and
         * verify the unordered result set as if test specifies that the result
         * can vary with comments of unordered.
         */
        (void)snprintf(cmd,
            sizeof(cmd),
            SYSTEMQUOTE "%s \"%s\" \"%s\" \"%s\" " SYSTEMQUOTE,
            smartmatch,
            default_expectfile,
            resultsfile,
            smartmatchfile);
        result = system(cmd);
        if (WEXITSTATUS(result) == 1) {
            /* In a success of smartmatch, there is no need of smartmatch file */
            unlink(smartmatchfile);
            unlink(diff);
            return false;
        } else if (WEXITSTATUS(result) == 2) {
            unlink(smartmatchfile);
            pcPrettyDiffFile = acBestExpectFile;
        } else
            pcPrettyDiffFile = smartmatchfile;
    } else
        pcPrettyDiffFile = acBestExpectFile;

    /*
     * Use the best comparison file to generate the "pretty" diff, which we
     * append to the diffs summary file.
     */
    (void)snprintf(cmd,
        sizeof(cmd),
        SYSTEMQUOTE "diff %s \"%s\" \"%s\" >> \"%s\" 2>&1" SYSTEMQUOTE,
        pretty_diff_opts,
        pcPrettyDiffFile,
        resultsfile,
        difffilename);
    (void)run_diff(cmd, difffilename);

    /* And append a separator */
    difffile = fopen(difffilename, "a");
    if (difffile) {
        fprintf(difffile, "\n======================================================================\n\n");
        fclose(difffile);
    }

    unlink(diff);
    return true;
}

/*
 * Wait for specified subprocesses to finish, and return their exit
 * statuses into statuses[]
 *
 * If names isn't NULL, print each subprocess's name as it finishes
 *
 * Note: it's OK to scribble on the pids array, but not on the names array
 */
static void wait_for_tests(PID_TYPE* pids, int* statuses, char** names, int num_tests)
{
    int tests_left;
    int i;

#ifdef WIN32
    PID_TYPE* active_pids = malloc(num_tests * sizeof(PID_TYPE));

    memcpy(active_pids, pids, num_tests * sizeof(PID_TYPE));
#endif

    if (g_bEnablePerfDataPrint) {
        memset(&g_stResourceUsageDetails, 0, sizeof(REGR_AVG_RSRCE_USAGE_STRU));
    }

    tests_left = num_tests;
    while (tests_left > 0) {
        PID_TYPE p;

#ifndef WIN32
        int exit_status;
        unsigned int uiPidChanged;

        if (false == g_bEnablePerfDataPrint) {
            p = wait(&exit_status);
            if (p == INVALID_PID) {
                fprintf(stderr, _("failed to wait for subprocesses: %s\n"), gs_strerror(errno));
                exit(2);
            }
        } else {
            if (REGR_SUCCESS != regrGetRsrcUsageWithRepeatOnFail(&g_stPrevCpuUsage, &uiPidChanged)) {
                fprintf(stderr, _("Could not get the CPU Usage info! \n"));
                g_stPrevCpuUsage.ulCpuTotalTime = 0;
            }

            do {
                p = waitpid(REGR_PID_WAIT_FOR_ANY_CHILD, &exit_status, WNOHANG);
                if (p == INVALID_PID) {
                    fprintf(stderr, _("failed to wait for subprocesses: %s\n"), gs_strerror(errno));
                    exit(2);
                }

                if (NO_CHILD_PROC_HAS_CHANGED_STATE != p) {
                    /* Getting subsequent CpuMemUsageReading to get usage in %*/
                    regrGetSubsequentUsage();
                    break;
                }

                pg_usleep(REGR_DELAY_FOR_PERF_COLLECTION);

                /* Getting the subsequent CPU / Memory usage details */
                regrGetSubsequentUsage();
            } while (true);
        }

#else
        DWORD exit_status;
        int r;

        r = WaitForMultipleObjects(tests_left, active_pids, FALSE, INFINITE);
        if (r < WAIT_OBJECT_0 || r >= WAIT_OBJECT_0 + tests_left) {
            fprintf(stderr, _("failed to wait for subprocesses: %lu\n"), GetLastError());
            exit_nicely(2);
        }
        p = active_pids[r - WAIT_OBJECT_0];
        /* compact the active_pids array */
        active_pids[r - WAIT_OBJECT_0] = active_pids[tests_left - 1];
#endif /* WIN32 */

        for (i = 0; i < num_tests; i++) {
            if (p == pids[i]) {
#ifdef WIN32
                GetExitCodeProcess(pids[i], &exit_status);
                CloseHandle(pids[i]);
#endif
                pids[i] = INVALID_PID;
                statuses[i] = (int)exit_status;
                if (names) {
                    status(" %s", names[i]);
                }
                tests_left--;
                REGR_STOP_TIMER_TEMP(i);
                break;
            }
        }
    }

    if ((g_bEnablePerfDataPrint) && (0 != g_stResourceUsageDetails.ulCount)) {
        /* Average System CPU Usage */
        g_stResourceUsageDetails.dSAvgcpuUsage /= g_stResourceUsageDetails.ulCount;

        /* Average User CPU usage */
        g_stResourceUsageDetails.dUAvgcpuUsage /= g_stResourceUsageDetails.ulCount;

        /* Average Memory Usage in MBs and Converting the MB value into Bytes */
        g_stResourceUsageDetails.dAvgMemUsage =
            (g_stResourceUsageDetails.dAvgMemUsage / g_stResourceUsageDetails.ulCount) * REGR_MCR_SIZE_1MB;

        regrConvertSizeInBytesToReadableForm((unsigned long)g_stResourceUsageDetails.dAvgMemUsage,
            (char*)g_stResourceUsageDetails.acMemUsage,
            (unsigned int)MAX_REGR_TOKEN_LEN);

        /* Average Memory Usage in percentage */
        g_stResourceUsageDetails.dAvgMemUsagePct /= g_stResourceUsageDetails.ulCount;
    }

#ifdef WIN32
    free(active_pids);
#endif
}

/*
 * report nonzero exit code from a test process
 */
static void log_child_failure(int exitstatus)
{
    if (WIFEXITED(exitstatus))
        status(_(" (test process exited with exit code %d)"), WEXITSTATUS(exitstatus));
    else if (WIFSIGNALED(exitstatus)) {
#if defined(WIN32)
        status(_(" (test process was terminated by exception 0x%X)"), WTERMSIG(exitstatus));
#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
        status(_(" (test process was terminated by signal %d: %s)"),
            WTERMSIG(exitstatus),
            WTERMSIG(exitstatus) < NSIG ? sys_siglist[WTERMSIG(exitstatus)] : "(unknown))");
#else
        status(_(" (test process was terminated by signal %d)"), WTERMSIG(exitstatus));
#endif
    } else
        status(_(" (test process exited with unrecognized status %d)"), exitstatus);
}

/* Allocates a block of memory which is required to execute run_schedule().
 *
 * When the function is called for the first time, i.e. iOldSize is 0 and
 * *pppcMemBlockStart is NULL:
 * 1. A fresh block of memory is allocated and the pointer for holding the
 *    'start of the memory block' is updated. (pppcMemBlockStart is the address
 *    of the pointer holding memory block)
 * 2. Various other pointers, in the caller function (like resultfiles
 *    expectfiles etc), which are supposed to point to the different parts of
 *    this memory block, are updated.
 *
 * When the initial block of memory is exhausted, then this function is called
 * subsequently to:
 * 1. Backup the old memory block address
 * 2. Get a fresh block of memory based on the value: iReqSize
 * 3. Various other pointers, in the caller function (like resultfiles
 *    expectfiles etc), which are pointing to the different parts of the old
 *    memory block, are updated to point to the appropriate area in the new
 *	  memory block
 * 4. Copy the contents of the old block (based on the iOldSize) into the
 * 	  appropriate area of the new block.
 *
 * Returns 0 in case of success and REGR_ERRCODE_MALLOC_FAILURE on memory
 * allocation failure
 */
static int regrMallocForTest(int iReqSize, int iOldSize, char*** pppcMemBlockStart, _stringlist*** pppstResultfiles,
    _stringlist*** pppstExpectfiles, _stringlist*** pppstTags, PID_TYPE** ppPids, int** ppiStatuses)
{
    char** ppcOldMemBlockStart = NULL;
    char* pcTmp = NULL;

    /* When the function is called for the subsequent times, *pppcMemBlockStart
     * will be pointing to the old memory block. This address need to be
     * backuped, so that the contents can be copied into the newly allocating
     * block */
    if (*pppcMemBlockStart != NULL)
        ppcOldMemBlockStart = *pppcMemBlockStart;

    /* Allocating memory for test file path storage, result file path storage,
     * expected file path storage, tags (not used as of now) storage, PID TYPE
     * storage and the status storage for the scheduler test execution
     *
     * Memory Layout:
     *	+-----------+-----------+-----------+-----------+-----------+----------+
     *	| Test		| Result	| Expected	| Tags		| PID		| Statuses |
     *	| file		| file		| file		|			| TYPE		| 		   |
     *	| path		| path		| path		| storage	| storage	| storage  |
     *	| storage	| storage	| storage	|			|			|		   |
     *	+-----------+-----------+-----------+-----------+-----------+----------+
     */
    *pppcMemBlockStart = (char**)malloc((sizeof(char*) + sizeof(_stringlist*) + sizeof(_stringlist*) +
                                            sizeof(_stringlist*) + sizeof(PID_TYPE) + sizeof(int)) *
                                        iReqSize);
    if (*pppcMemBlockStart == NULL) {
        fprintf(stderr,
            _("Could not allocate memory for schedule test "
              "execution. Requested size: %d.\n"),
            (int)((sizeof(char*) + sizeof(_stringlist*) + sizeof(_stringlist*) + sizeof(_stringlist*) +
                      sizeof(PID_TYPE) + sizeof(int)) *
                  iReqSize));
        *pppcMemBlockStart = ppcOldMemBlockStart;
        return REGR_ERRCODE_MALLOC_FAILURE;
    }

    /* Updating the output parameter to point to the result file storage area
     * in the allocated memory block */
    *pppstResultfiles = (_stringlist**)(void*)((char*)(void*)(*pppcMemBlockStart) + (sizeof(char*) * iReqSize));

    /* Updating the output parameter to point to the expected file storage area
     * in the allocated memory block */
    *pppstExpectfiles = (_stringlist**)(void*)((char*)(void*)(*pppstResultfiles) + (sizeof(_stringlist*) * iReqSize));

    /* Updating the output parameter to point to the Tags storage area
     * in the allocated memory block */
    *pppstTags = (_stringlist**)(void*)((char*)(void*)(*pppstExpectfiles) + (sizeof(_stringlist*) * iReqSize));

    /* Updating the output parameter to point to the PID TYPE storage area
     * in the allocated memory block */
    *ppPids = (PID_TYPE*)(void*)((char*)(void*)(*pppstTags) + (sizeof(_stringlist*) * iReqSize));

    /* Updating the output parameter to point to the STATUS storage area
     * in the allocated memory block */
    *ppiStatuses = (int*)(void*)((char*)(void*)(*ppPids) + (sizeof(PID_TYPE) * iReqSize));

    /* Resetting the Result file, expected file and tags sections of the
     * allocated memory block */
    memset(*pppstResultfiles, 0, sizeof(_stringlist*) * iReqSize);
    memset(*pppstExpectfiles, 0, sizeof(_stringlist*) * iReqSize);
    memset(*pppstTags, 0, sizeof(_stringlist*) * iReqSize);

    /* Case of Reallocating the memory. So copy the data from the old memory
     * into the freshly allocated block
     */
    if ((iOldSize != 0) && (ppcOldMemBlockStart != NULL)) {
        /* Copying test file path pointers */
        pcTmp = (char*)(void*)ppcOldMemBlockStart;
        memcpy(*pppcMemBlockStart, pcTmp, (sizeof(char*) * iOldSize));

        /* Copying result file path pointers */
        pcTmp = pcTmp + (sizeof(char*) * iOldSize);
        memcpy(*pppstResultfiles, pcTmp, (sizeof(_stringlist*) * iOldSize));

        /* Copying expected file path pointers */
        pcTmp = pcTmp + (sizeof(_stringlist*) * iOldSize);
        memcpy(*pppstExpectfiles, pcTmp, (sizeof(_stringlist*) * iOldSize));

        /* Copying tags pointers */
        pcTmp = pcTmp + (sizeof(_stringlist*) * iOldSize);
        memcpy(*pppstTags, pcTmp, (sizeof(_stringlist*) * iOldSize));

        /* Copying PID TYPES */
        pcTmp = pcTmp + (sizeof(_stringlist*) * iOldSize);
        memcpy(*ppPids, pcTmp, (sizeof(PID_TYPE) * iOldSize));

        /* Copying statuses */
        pcTmp = pcTmp + (sizeof(PID_TYPE) * iOldSize);
        memcpy(*ppiStatuses, pcTmp, (sizeof(int) * iOldSize));

        /* Freeing the old memory block */
        free(ppcOldMemBlockStart);
    }

    return REGR_SUCCESS;
}

/*
 * Get the time difference between the start and end time of the test execution.
 * Also updates the total time taken by the entire test suite.
 */
static void regrGetElapsedTime(void)
{
    g_dTotalTime = (g_stRegrStopTime.tv_sec - g_stRegrStartTime.tv_sec) +
                   (g_stRegrStopTime.tv_usec - g_stRegrStartTime.tv_usec) * 0.000001;
    g_dGroupTotalTime = g_dGroupTotalTime + g_dTotalTime;
}

/*
 * Parse the line buffer read from the schedule file and populate the test
 * details
 */
static int regrParseLineBuffer(char* pcLineBuf, char** ppcLastSpace, int* piNumTests, int* piMaxParallelTests,
    char*** pppctests, _stringlist*** pppstResultfiles, _stringlist*** pppstExpectfiles, _stringlist*** pppstTags,
    PID_TYPE** ppPids, int** ppiStatuses)
{
    int iRet = REGR_SUCCESS;
    char* pcTemp = NULL;
    bool bInsideWord = false;

    *ppcLastSpace = NULL;

    for (pcTemp = pcLineBuf; *pcTemp; pcTemp++) {
        if (isspace((unsigned char)*pcTemp)) {
            *pcTemp = '\0';
            bInsideWord = false;
            *ppcLastSpace = pcTemp;
        } else if (!bInsideWord) {
            if (*piNumTests >= *piMaxParallelTests) {
                /* Pre-allocated memory got exhausted. So extend it by
                 * reallocating and copying data from old block to the new
                 * block. Also make sure that the pointer are pointing to
                 * the appropriate positions in the new block */
                iRet = regrMallocForTest(*piMaxParallelTests + REGR_MAX_PARALLEL_TESTS,
                    *piMaxParallelTests,
                    pppctests,
                    pppstResultfiles,
                    pppstExpectfiles,
                    pppstTags,
                    ppPids,
                    ppiStatuses);
                if (iRet != REGR_SUCCESS) {
                    return iRet;
                }

                /* Memory has been extented for an additional
                 * REGR_MAX_PARALLEL_TESTS */
                *piMaxParallelTests += REGR_MAX_PARALLEL_TESTS;
            }

            (*pppctests)[*piNumTests] = pcTemp;
            (*piNumTests)++;
            bInsideWord = true;
        }
    }

    return REGR_SUCCESS;
}

/*
 * Reload line buffer and pasrse the line read from the schedule file
 */
static int regrReloadAndParseLineBuffer(bool* pbBuffReloadReq, bool* pbHalfReadTest, char** ppcLastSpace, FILE* scf,
    int* piNumTests, int* piMaxParallelTests, char*** pppctests, _stringlist*** pppstResultfiles,
    _stringlist*** pppstExpectfiles, _stringlist*** pppstTags, PID_TYPE** ppPids, int** ppiStatuses)
{
    int iRet = REGR_SUCCESS;
    int j = 0;
    char* pcLineBuf = NULL;
    char* pcTemp = NULL;

    g_uiCurLineBufIdx++;

    if (g_uiCurLineBufIdx == REGR_MAX_NUM_OF_LINE_BUFF) {
        fprintf(stderr,
            _("\n Length of the line in schedule file is too long. "
              "Maximum supported length is %d.\n"),
            (REGR_MAX_NUM_OF_LINE_BUFF * REGR_MAX_SCH_LINE_SIZE));
        return REGR_ERRCODE_MAX_NUM_OF_LINE_BUFF;
    }

    /* g_uiCurLineBufIdx is the index to the buffer and g_uiTotalBuf is the
     * total number of buffers that are allocated */
    if (g_uiCurLineBufIdx == g_uiTotalBuf) {
        g_ppcBuf[g_uiCurLineBufIdx] = (char*)malloc(REGR_MAX_SCH_LINE_SIZE);
        if (g_ppcBuf[g_uiCurLineBufIdx] == NULL) {
            fprintf(stderr,
                _("Could not allocate memory for reading line from "
                  "schedule file. Requested size: %d.\n"),
                REGR_MAX_SCH_LINE_SIZE);
            return REGR_ERRCODE_MALLOC_FAILURE;
        }

        g_uiTotalBuf++;
    }

    pcLineBuf = g_ppcBuf[g_uiCurLineBufIdx];
    memset(pcLineBuf, 0, REGR_MAX_SCH_LINE_SIZE);

    if (true == *pbHalfReadTest) {
        /* Copy the 'half read' test file detail into the new buffer's start */
        for (j = 0, pcTemp = *ppcLastSpace + 1; *pcTemp != '\0'; pcTemp++, j++)
            pcLineBuf[j] = *pcTemp;

        *pbHalfReadTest = false;
    }

    /* Loading the line buffer with the 'left out' characters of the current
     * line in the schedule file */
    if (NULL == fgets(&(pcLineBuf[j]), REGR_MAX_SCH_LINE_SIZE - j, scf))
        return REGR_EOF_REACHED;

    j = strlen(pcLineBuf);
    if ((j > 0) && ('\n' != pcLineBuf[j - 1]) && (!feof(scf)))
        *pbBuffReloadReq = true;

    /* Parse the Line buffer to get the test file details */
    iRet = regrParseLineBuffer(pcLineBuf,
        ppcLastSpace,
        piNumTests,
        piMaxParallelTests,
        pppctests,
        pppstResultfiles,
        pppstExpectfiles,
        pppstTags,
        ppPids,
        ppiStatuses);
    if (iRet != REGR_SUCCESS)
        return iRet;

    if ((true == *pbBuffReloadReq) && (NULL != *ppcLastSpace) && (0 != strlen(*ppcLastSpace + 1))) {
        *pbHalfReadTest = true;
        (*piNumTests)--;
    }

    return iRet;
}

/*
 * Run all the tests specified in one schedule file
 */
static void run_schedule(const char* schedule, test_function tfunc, diag_function dfunc)
{
    char** tests = NULL;
    _stringlist** resultfiles;
    _stringlist** expectfiles;
    _stringlist** tags;
    PID_TYPE* pids = NULL;
    int* statuses = NULL;
    int i;
    int iRet = REGR_SUCCESS;
    int iMaxParallelTests = REGR_MAX_PARALLEL_TESTS;
    _stringlist* ignorelist = NULL;
    FILE* scf = NULL;
    int line_num = 0;
    bool bRhsSpacePresent = false;
    bool bscript = false;
    bool bIgnoreLineOnReload = false;
    bool isSystemTableDDL = false;
    bool isPlanAndProto = false;
    if (grayscale_upgrade != -1) {
        g_uiTotalBuf = 0;
    }

    /* Allocating memory for holding the addresses of all line buffers.
     * Each line buffer is used to hold a part (or full) of the line, read from
     * the scheduler file */
    g_ppcBuf = (char**)malloc(sizeof(char*) * REGR_MAX_NUM_OF_LINE_BUFF);
    if (NULL == g_ppcBuf) {
        fprintf(stderr,
            _("Could not allocate memory for buffer to hold line "
              "buffer addresses. Requested size: %lu.\n"),
            (REGR_MAX_NUM_OF_LINE_BUFF * sizeof(char*)));
        exit(2);
    }

    /* Allocating memory for holding a line to be read from the schedule file */
    g_ppcBuf[g_uiCurLineBufIdx] = (char*)malloc(REGR_MAX_SCH_LINE_SIZE);
    if (g_ppcBuf[g_uiCurLineBufIdx] == NULL) {
        fprintf(stderr,
            _("Could not allocate memory for reading line from "
              "schedule file. Requested size: %d.\n"),
            REGR_MAX_SCH_LINE_SIZE);
        iRet = REGR_ERRCODE_MALLOC_FAILURE;
        goto LB_ERR_HNDL_LVL_1;
    }

    g_uiTotalBuf++;
    memset(g_ppcBuf[g_uiCurLineBufIdx], 0, REGR_MAX_SCH_LINE_SIZE);

    /* Allocating memory for test file path storage, result file path storage,
     * expected file path storage, tags (not used as of now) storage, PID TYPE
     * storage and the status storage for the scheduler test execution */
    iRet = regrMallocForTest(iMaxParallelTests, 0, &tests, &resultfiles, &expectfiles, &tags, &pids, &statuses);
    if (iRet != REGR_SUCCESS) {
        iRet = REGR_ERRCODE_MALLOC_FAILURE;
        goto LB_ERR_HNDL_LVL_2;
    }

    /* Initializing the "total time taken by the test suite execution" */
    g_dGroupTotalTime = 0;

    scf = fopen(schedule, "r");
    if (!scf) {
        fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, schedule, gs_strerror(errno));
        iRet = REGR_ERRCODE_FOPEN_FAILURE;
        goto LB_ERR_HNDL_LVL_3;
    }

    while (fgets(g_ppcBuf[g_uiCurLineBufIdx], REGR_MAX_SCH_LINE_SIZE, scf)) {
        char* test = NULL;
        char* pcLastSpace = NULL;
        char* c = NULL;
        int num_tests;
        bool bBuffReloadReq = false;
        bool bHalfReadTest = false;

        i = strlen(g_ppcBuf[g_uiCurLineBufIdx]);
        if ((i > 0) && ('\n' != g_ppcBuf[g_uiCurLineBufIdx][i - 1]) && (!feof(scf))) {
            bBuffReloadReq = true;
        }

        if (bIgnoreLineOnReload) {
            if (!bBuffReloadReq)
                bIgnoreLineOnReload = false;

            continue;
        }

        line_num++;

        for (i = 0; i < iMaxParallelTests; i++) {
            if (resultfiles[i] == NULL)
                break;
            free_stringlist(&resultfiles[i]);
            free_stringlist(&expectfiles[i]);
            free_stringlist(&tags[i]);
        }

        /* strip trailing whitespace, especially the newline */
        i = strlen(g_ppcBuf[g_uiCurLineBufIdx]);

        while ((i > 0) && (isspace((unsigned char)g_ppcBuf[g_uiCurLineBufIdx][i - 1]))) {
            i--;

            if ((g_ppcBuf[g_uiCurLineBufIdx][i] == ' ') || (g_ppcBuf[g_uiCurLineBufIdx][i] == '\t'))
                bRhsSpacePresent = true;

            g_ppcBuf[g_uiCurLineBufIdx][i] = '\0';
        }

        if ((g_ppcBuf[g_uiCurLineBufIdx][0] == '\0') || (g_ppcBuf[g_uiCurLineBufIdx][0] == '#')) {
            if (bBuffReloadReq)
                bIgnoreLineOnReload = true;

            continue;
        }

        if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "test: ", 6) == 0) {
            test = g_ppcBuf[g_uiCurLineBufIdx] + 6;
        } else if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "system_table_ddl_test: ", 23) == 0) {
            isSystemTableDDL = true;
            test = g_ppcBuf[g_uiCurLineBufIdx] + 23;
        } else if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "plan_proto_test: ", 17) == 0) {
            isPlanAndProto = true;
            test = g_ppcBuf[g_uiCurLineBufIdx] + 17;
        } else if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "script: ", 8) == 0) {
            bscript = true;
            test = g_ppcBuf[g_uiCurLineBufIdx] + 8;
        } else if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "ignore: ", 8) == 0) {
            c = g_ppcBuf[g_uiCurLineBufIdx] + 8;
            while (*c && isspace((unsigned char)*c))
                c++;
            add_stringlist_item(&ignorelist, c);

            /*
             * Note: ignore: lines do not run the test, they just say that
             * failure of this test when run later on is to be ignored. A bit
             * odd but that's how the shell-script version did it.
             */
            continue;
        } else if (strncmp(g_ppcBuf[g_uiCurLineBufIdx], "testcon: ", 9) == 0) {
#ifdef ENABLED_DEBUG_SYNC
            test = g_ppcBuf[g_uiCurLineBufIdx] + 9;
#else
            c = g_ppcBuf[g_uiCurLineBufIdx] + 9;
            while (*c && isspace((unsigned char)*c))
                c++;
            add_stringlist_item(&ignorelist, c);
            continue;
#endif
        } else {
            fprintf(stderr,
                _("Syntax error in ScheduleFile \"%s\" line %d: %s\n"),
                schedule,
                line_num,
                g_ppcBuf[g_uiCurLineBufIdx]);
            iRet = REGR_ERRCODE_SYNTAX_ERR_IN_SCH;
            goto LB_ERR_HNDL_LVL_4;
        }

        num_tests = 0;

        iRet = regrParseLineBuffer(test,
            &pcLastSpace,
            &num_tests,
            &iMaxParallelTests,
            &tests,
            &resultfiles,
            &expectfiles,
            &tags,
            &pids,
            &statuses);
        if (iRet != REGR_SUCCESS) {
            goto LB_ERR_HNDL_LVL_4;
        }

        if ((!bRhsSpacePresent) && (true == bBuffReloadReq) && (NULL != pcLastSpace) &&
            (0 != strlen(pcLastSpace + 1))) {
            bHalfReadTest = true;
            num_tests--;
        }

        /* Resetting the flag */
        bRhsSpacePresent = false;

        if (num_tests == 0) {
            fprintf(stderr,
                _("Syntax error in schedule file \"%s\" line %d: %s\n"),
                schedule,
                line_num,
                g_ppcBuf[g_uiCurLineBufIdx]);
            iRet = REGR_ERRCODE_SYNTAX_ERR_IN_SCH;
            goto LB_ERR_HNDL_LVL_4;
        }

        if (bscript && (num_tests > 1)) {
            fprintf(stderr,
                _("Syntax error in schedule file \"%s\" line %d: %s\n"),
                schedule,
                line_num,
                g_ppcBuf[g_uiCurLineBufIdx]);
            iRet = REGR_ERRCODE_SYNTAX_ERR_IN_SCH;
            goto LB_ERR_HNDL_LVL_4;
        }

        /* exception protect */
        if (REG_MAX_NUM <= num_tests) {
            fprintf(stderr,
                _("The max test num:%d of parallel group is large than  REG_MAX_NUM:%d. \n"
                  "Syntax error in schedule file \"%s\" line %d: %s\n"),
                num_tests,
                REG_MAX_NUM,
                schedule,
                line_num,
                g_ppcBuf[g_uiCurLineBufIdx]);
            iRet = REGR_ERRCODE_SYNTAX_ERR_IN_SCH;
            goto LB_ERR_HNDL_LVL_4;
        }

        if (num_tests == 1) {
            if (bscript) {
                bscript = false;

                status(_("script %-22s .... "), tests[0]);
                REGR_START_TIMER_TEMP(0);

                pids[0] = scriptExecute(tests[0], &resultfiles[0], &expectfiles[0], &tags[0]);
            } else {
                if (isSystemTableDDL) {
                    status(_("system_table_ddl_test %-24s .... "), tests[0]);
                } else if (isPlanAndProto) {
                    status(_("plan_proto_test %-24s .... "), tests[0]);
                } else if (use_jdbc_client) {
                    status(_("jdbc test %-24s .... "), tests[0]);
                } else {
                    status(_("test %-24s .... "), tests[0]);
                }
                makeNestedDirectory(tests[0]);

                REGR_START_TIMER_TEMP(0);

                pids[0] = (tfunc)(tests[0], &resultfiles[0], &expectfiles[0], &tags[0], use_jdbc_client);
            }

            wait_for_tests(pids, statuses, NULL, 1);
            REGR_STOP_TIMER_TEMP(0);
            REGR_PRINT_ONEGROUP_ELAPSED_TIME;
            /* status line is finished below */
        } else if (max_connections > 0 && max_connections < num_tests) {
            int oldest = 0;

            i = 0;

            do {
                while (i < num_tests) {
                    if (i - oldest >= max_connections) {
                        wait_for_tests(pids + oldest, statuses + oldest, tests + oldest, i - oldest);
                        oldest = i;
                    }

                    makeNestedDirectory(tests[i]);

                    REGR_START_TIMER;

                    /* Invoke the single test file */
                    pids[i] = (tfunc)(tests[i], &resultfiles[i], &expectfiles[i], &tags[i], use_jdbc_client);
                    i++;
                }

                if (false == bBuffReloadReq)
                    break;

                /* Resetting the flag */
                bBuffReloadReq = false;

                iRet = regrReloadAndParseLineBuffer(&bBuffReloadReq,
                    &bHalfReadTest,
                    &pcLastSpace,
                    scf,
                    &num_tests,
                    &iMaxParallelTests,
                    &tests,
                    &resultfiles,
                    &expectfiles,
                    &tags,
                    &pids,
                    &statuses);
                if (iRet == REGR_EOF_REACHED) {
                    break;
                }

                if (iRet != REGR_SUCCESS) {
                    goto LB_ERR_HNDL_LVL_4;
                }
            } while (true);
            if (isSystemTableDDL) {
                status(_("parallel group (%d system_table_ddl_tests, in groups of %d): "), num_tests, max_connections);
            } else if (isPlanAndProto) {
                status(_("parallel group (%d plan_proto_tests, in groups of %d): "), num_tests, max_connections);
            } else {
                status(_("parallel group (%d tests, in groups of %d): "), num_tests, max_connections);
            }

            wait_for_tests(pids + oldest, statuses + oldest, tests + oldest, i - oldest);
            status_end();

            g_uiCurLineBufIdx = 0;
        } else {
            i = 0;

            do {
                while (i < num_tests) {
                    makeNestedDirectory(tests[i]);

                    REGR_START_TIMER_TEMP(i);

                    pids[i] = (tfunc)(tests[i], &resultfiles[i], &expectfiles[i], &tags[i], use_jdbc_client);

                    i++;
                }

                if (false == bBuffReloadReq)
                    break;

                /* Resetting the flag */
                bBuffReloadReq = false;

                iRet = regrReloadAndParseLineBuffer(&bBuffReloadReq,
                    &bHalfReadTest,
                    &pcLastSpace,
                    scf,
                    &num_tests,
                    &iMaxParallelTests,
                    &tests,
                    &resultfiles,
                    &expectfiles,
                    &tags,
                    &pids,
                    &statuses);
                if (iRet == REGR_EOF_REACHED) {
                    break;
                }

                if (iRet != REGR_SUCCESS) {
                    goto LB_ERR_HNDL_LVL_4;
                }
            } while (true);
            if (isSystemTableDDL) {
                status(_("parallel group (%d system_table_ddl_tests): "), num_tests);
            } else if (isPlanAndProto) {
                status(_("parallel group (%d plan_proto_tests): "), num_tests);
            } else {
                status(_("parallel group (%d tests): "), num_tests);
            }

            wait_for_tests(pids, statuses, tests, num_tests);
            REGR_PRINT_ONEGROUP_ELAPSED_TIME;
            status_end();

            g_uiCurLineBufIdx = 0;
        }

        /* Check results for all tests */
        for (i = 0; i < num_tests; i++) {
            _stringlist *rl, *el, *tl;
            bool differ = false;

            if (num_tests > 1)
                status(_("     %-24s .... "), tests[i]);

            /*
             * Advance over all three lists simultaneously.
             *
             * Compare resultfiles[j] with expectfiles[j] always. Tags are
             * optional but if there are tags, the tag list has the same
             * length as the other two lists.
             */
            for (rl = resultfiles[i], el = expectfiles[i], tl = tags[i];
                 rl != NULL; /* rl and el have the same length */
                 rl = rl->next, el = el->next) {
                bool newdiff = false;

                if (tl)
                    tl = tl->next; /* tl has the same length as rl and el
                                    * if it exists */

                newdiff = results_differ(tests[i], rl->str, el->str);

                /* Check if diff has failed; if yes then collect the diagnosis report*/
                if (newdiff && dfunc) {
                    pids[0] = dfunc((char*)tests[i]);
                    wait_for_tests(pids, statuses, NULL, 1);
                }

                if (newdiff && tl) {
                    printf("%s ", tl->str);
                }
                differ = differ || newdiff;
            }

            if (differ) {
                bool ignore = false;
                _stringlist* sl = NULL;

                for (sl = ignorelist; sl != NULL; sl = sl->next) {
                    if (strcmp(tests[i], sl->str) == 0) {
                        ignore = true;
                        break;
                    }
                }
                if (ignore) {
                    status(_("%-18s"), "failed (ignored)");
                    fail_ignore_count++;
                } else if (isSystemTableDDL) {
                    status(_("%-18s"), "ok (grayscale stage)");
                    success_count++;
                } else if (isPlanAndProto) {
                    status(_("%-18s"), "FAILED (grayscale observe stage)");
                    fail_count++;
                } else {
                    status(_("%-18s"), "FAILED");
                    fail_count++;
                }
            } else {
                if (isSystemTableDDL) {
                    status(_("%-18s"), "FAILED (grayscale stage)");
                    fail_count++;
                } else if (isPlanAndProto) {
                    status(_("%-18s"), "FAILED (grayscale stage)");
                    fail_count++;
                } else {
                    status(_("%-18s"), "ok");
                    success_count++;
                }
            }

            REGR_PRINT_ELAPSED_TIME_TEMP(i);

            if (statuses[i] != 0) {
                log_child_failure(statuses[i]);
            }

            /*
             * Indicates the last test in the group in case of parallel test
             * So end of status shall be done after the performation info
             * printing
             */
        }
    }

    free_stringlist(&ignorelist);

LB_ERR_HNDL_LVL_4:
    fclose(scf);

LB_ERR_HNDL_LVL_3:
    REGR_FREE(tests);

LB_ERR_HNDL_LVL_2:
    for (i = 0; i < (int)g_uiTotalBuf; i++) {
        REGR_FREE(g_ppcBuf[i]);
    }

LB_ERR_HNDL_LVL_1:
    REGR_FREE(g_ppcBuf);

    if (iRet != REGR_SUCCESS)
        exit_nicely(2);

    return;
}

/*
 * Run a single test
 */
static void run_single_test(const char* test, test_function tfunc, diag_function dfunc)
{
    PID_TYPE pid;
    int exit_status;
    _stringlist* resultfiles = NULL;
    _stringlist* expectfiles = NULL;
    _stringlist* tags = NULL;
    _stringlist *rl, *el, *tl;
    bool differ = false;

    if (!use_jdbc_client) { 
        status(_("test %-24s .... "), test); 
    } else {
        status(_("jdbc test %-24s .... "), test); 
    }

    makeNestedDirectory(test);

    REGR_START_TIMER;
    pid = (tfunc)(test, &resultfiles, &expectfiles, &tags, use_jdbc_client);
    wait_for_tests(&pid, &exit_status, NULL, 1);
    REGR_STOP_TIMER;

    /*
     * Advance over all three lists simultaneously.
     *
     * Compare resultfiles[j] with expectfiles[j] always. Tags are optional
     * but if there are tags, the tag list has the same length as the other
     * two lists.
     */
    for (rl = resultfiles, el = expectfiles, tl = tags; rl != NULL; /* rl and el have the same length */
         rl = rl->next, el = el->next) {
        bool newdiff = false;

        if (tl)
            tl = tl->next; /* tl has the same length as rl and el if it
                            * exists */

        newdiff = results_differ(test, rl->str, el->str);

        /* Check if diff has failed; if yes then collect the diagnosis report*/
        if (newdiff && dfunc) {
            pid = dfunc((char*)test);
            wait_for_tests(&pid, &exit_status, NULL, 1);
        }

        if (newdiff && tl) {
            printf("%s ", tl->str);
        }
        differ = differ || newdiff;
    }

    if (differ) {
        status(_("%-18s"), "FAILED");
        fail_count++;
    } else {
        status(_("%-18s"), "ok");
        success_count++;
    }

    if (exit_status != 0)
        log_child_failure(exit_status);

    if (false == g_bEnablePerfDataPrint)
        status_end();

    REGR_PRINT_ELAPSED_TIME;
}

#define GET_VARIABLE_NAME(name) (#name)

static void CheckCleanCodeWarningInfo(const int baseNum, const int currentNum,
    const char *legacyMacrosName, const char *legacyMacrosNumName)
{
    (void)fprintf(stdout, "======================== Clean Code Committee Warning ========================\n");
    if (strcmp(legacyMacrosName, "THR_LOCAL") == 0) {
        (void)fprintf(stdout, "The macro THR_LOCAL are strictly forbidden.\n");
    } else {
        (void)fprintf(stdout, "The only authoritative macro to isolate distribute and single node\n");
        (void)fprintf(stdout, "logic is ENABLE_MULTIPLE_NODES, other legacy macros(PGXC/STREAMPLAN/IS_SINGLE_NODE)\n");
        (void)fprintf(stdout, "are strictly forbidden.\n");
    }
    (void)fprintf(stdout, "The macro(%s) baseline are %d, current are %d.\n",
        legacyMacrosName, baseNum, currentNum);

    if (currentNum < baseNum) {
        (void)fprintf(stdout, "Solution: Please adjust the baseline(%s:%s) to %d before run test.\n",
            __FILE__, legacyMacrosNumName, currentNum);
    } else {
        (void)fprintf(stdout, "Solution: Please decrease the macros at least equal to baseline %d.\n",
            baseNum);
    }
    (void)fprintf(stdout, "==============================================================================\n");
    
    (void)fflush(stdout);
    return;
}

#define BASE_GLOBAL_VARIABLE_NUM 222

#define CMAKE_CMD_BUF_LEN 1000

static void check_global_variables()
{
#ifdef BUILD_BY_CMAKE
    char cmd_buf[CMAKE_CMD_BUF_LEN+1];
    snprintf(cmd_buf, CMAKE_CMD_BUF_LEN,"find %s/common/backend/ -name \"*.cpp\" | \
        xargs grep \"THR_LOCAL\" | grep -v \"extern THR_LOCAL\" | wc -l", code_base_src);
    char* cmd = cmd_buf;
#else
    char* cmd =
        "find ../../common/backend/ -name \"*.cpp\" | xargs grep \"THR_LOCAL\" | \
        grep -v \"extern THR_LOCAL\" | wc -l";
#endif
    FILE* fstream = NULL;
    char buf[50];
    int globalnum = 0;
    printf("cmd: %s\n", cmd);
    memset(buf, 0, sizeof(buf));
    fstream = popen(cmd, "r");
    if (NULL != fgets(buf, sizeof(buf), fstream)) {
        globalnum = atoi(buf);
    }
    pclose(fstream);
#ifdef BUILD_BY_CMAKE
    snprintf(cmd_buf, CMAKE_CMD_BUF_LEN,"find  %s/gausskernel/ -name \"*.cpp\" | \
    xargs grep \"THR_LOCAL\" | grep -v \"extern THR_LOCAL\" | wc -l", code_base_src);
    cmd = cmd_buf;
#else
    cmd = "find ../../gausskernel/ -name \"*.cpp\" | xargs grep \"THR_LOCAL\" | \
    grep -v \"extern THR_LOCAL\" | wc -l";
#endif
    memset(buf, 0, sizeof(buf));
    fstream = popen(cmd, "r");
    if (NULL != fgets(buf, sizeof(buf), fstream)) {
        globalnum += atoi(buf);
    }
    pclose(fstream);
    if (globalnum != BASE_GLOBAL_VARIABLE_NUM) {
        CheckCleanCodeWarningInfo(BASE_GLOBAL_VARIABLE_NUM, globalnum,
            GET_VARIABLE_NAME(THR_LOCAL), GET_VARIABLE_NAME(BASE_GLOBAL_VARIABLE_NUM));
        exit_nicely(2);
    }
}

#define BASE_PGXC_LIKE_MACRO_NUM 1402
static void check_pgxc_like_macros()
{
#ifdef BUILD_BY_CMAKE 
    char cmd_buf[1001];
    snprintf(cmd_buf, 1000,"find %s/common/backend/ %s/gausskernel/ -name \"*.cpp\" | \
    xargs grep -e \"#ifdef STREAMPLAN\" -e \"#ifdef PGXC\"  -e \"IS_SINGLE_NODE\" | \
    wc -l", code_base_src, code_base_src);
    printf("cmake..............i\n");
    printf("cmd_buf:%s\n", cmd_buf);
    char* cmd = cmd_buf;
#else
    char* cmd =
        "find ../../common/backend/ ../../gausskernel/ -name \"*.cpp\" |"
        "xargs grep -e \"#ifdef STREAMPLAN\" -e \"#ifdef PGXC\"  -e \"IS_SINGLE_NODE\" |wc -l";
#endif
    FILE* fstream = NULL;
    char buf[50];
    int macros = 0;

    memset(buf, 0, sizeof(buf));
    fstream = popen(cmd, "r");
    if (NULL != fgets(buf, sizeof(buf), fstream)) {
        macros = atoi(buf);
        if (macros != BASE_PGXC_LIKE_MACRO_NUM) {
            CheckCleanCodeWarningInfo(BASE_PGXC_LIKE_MACRO_NUM, macros,
                GET_VARIABLE_NAME(PGXC/STREAMPLAN/IS_SINGLE_NODE), GET_VARIABLE_NAME(BASE_PGXC_LIKE_MACRO_NUM));
            exit_nicely(2);
        }
    }
    pclose(fstream);
}

/*
 * Create the summary-output files (making them empty if already existing)
 */
static void open_result_files(void)
{
    char file[MAXPGPATH];
    FILE* difffile = NULL;
    errno_t rc = EOK;

    /* create the log file (copy of running status output) */
    (void)snprintf(file, sizeof(file), "%s/regression.out", outputdir);
    logfilename = strdup(file);
    logfile = fopen(logfilename, "w");
    if (!logfile) {
        fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"), progname, logfilename, strerror(errno));
        exit_nicely(2);
    }

    /* create the diffs file as empty */
    if (to_create_jdbc_user) {
        rc = snprintf_s(file, sizeof(file), sizeof(file) - 1, "%s/regression_jdbc.diffs", outputdir);
        securec_check_ss_c(rc, "", ""); 
    } else {
        rc = snprintf_s(file, sizeof(file), sizeof(file) - 1, "%s/regression.diffs", outputdir);
        securec_check_ss_c(rc, "", ""); 
    }

    difffilename = strdup(file);
    difffile = fopen(difffilename, "w");
    if (!difffile) {
        fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"), progname, difffilename, strerror(errno));
        exit_nicely(2);
    }
    /* we don't keep the diffs file open continuously */
    fclose(difffile);

    /* also create the output directory if not present */
    if (!use_jdbc_client) { 
        rc = snprintf_s(file, sizeof(file), sizeof(file) - 1, "%s/results", outputdir);
        securec_check_ss_c(rc, "", ""); 
    } else {
        rc = snprintf_s(file, sizeof(file), sizeof(file) - 1, "%s/results_jdbc", outputdir);
        securec_check_ss_c(rc, "", ""); 
    }
    if (!directory_exists(file)) {
        make_directory(file);
    }
}

/* create jdbc_user & grant all database to it */
static void create_jdbc_user(const _stringlist* granted_dbs) {
    const char *user_name = "jdbc_regress";
    const char *user_password = "1q@W3e4r";
    header(_("creating user for JDBC: \"%s\""), user_name);
    psql_command("postgres", "CREATE USER %s WITH PASSWORD '%s' LOGIN", user_name, user_password);
    psql_command("postgres", "ALTER USER %s sysadmin CREATEROLE createdb", user_name);
    for (; granted_dbs != NULL; granted_dbs = granted_dbs->next) {
        psql_command("postgres", "GRANT ALL ON DATABASE \"%s\" TO \"%s\"", granted_dbs->str, user_name);
    }
}

static void create_database(const char* dbname)
{
    _stringlist* sl = NULL;

    /*
     * We use template0 so that any installation-local cruft in template1 will
     * not mess up the tests.
     */
    header(_("creating database \"%s\""), dbname);
    if (encoding)
        psql_command("postgres",
            "CREATE DATABASE \"%s\" DBCOMPATIBILITY='A' TEMPLATE=TEMPLATE0 ENCODING='%s'%s",
            dbname,
            encoding,
            (nolocale) ? " LC_COLLATE='C' LC_CTYPE='C'" : "");
    else
        psql_command("postgres",
            "CREATE DATABASE \"%s\" DBCOMPATIBILITY='A' TEMPLATE=TEMPLATE0%s",
            dbname,
            (nolocale) ? " LC_COLLATE='C' LC_CTYPE='C'" : "");

    /*
     * Install any requested procedural languages.	We use CREATE OR REPLACE
     * so that this will work whether or not the language is preinstalled.
     */
    for (sl = loadlanguage; sl != NULL; sl = sl->next) {
        header(_("installing %s"), sl->str);
        psql_command(dbname, "CREATE OR REPLACE LANGUAGE \"%s\"", sl->str);
    }

    /*
     * Install any requested extensions.  We use CREATE IF NOT EXISTS so that
     * this will work whether or not the extension is preinstalled.
     */
    for (sl = loadextension; sl != NULL; sl = sl->next) {
        header(_("installing %s"), sl->str);
        psql_command(dbname, "CREATE EXTENSION IF NOT EXISTS \"%s\"", sl->str);
    }
}

static void create_role(const char* rolename, const _stringlist* granted_dbs)
{
    header(_("creating role \"%s\""), rolename);
    psql_command("postgres", "CREATE ROLE \"%s\" WITH LOGIN", rolename);
    for (; granted_dbs != NULL; granted_dbs = granted_dbs->next) {
        psql_command("postgres", "GRANT ALL ON DATABASE \"%s\" TO \"%s\"", granted_dbs->str, rolename);
    }
}

static char* make_absolute_path(const char* in)
{
    char* result = NULL;

    if (is_absolute_path(in)) {
        result = strdup(in);
    } else {
        static char cwdbuf[MAXPGPATH];

        if (!cwdbuf[0]) {
            if (!getcwd(cwdbuf, sizeof(cwdbuf))) {
                fprintf(stderr, _("could not get current working directory: %s\n"), strerror(errno));
                exit_nicely(2);
            }
        }

        result = (char*)malloc(strlen(cwdbuf) + strlen(in) + 2);
        sprintf(result, "%s/%s", cwdbuf, in);
    }

    canonicalize_path(result);
    return result;
}

static void help(void)
{
    printf(_("openGauss regression test driver\n"));
    printf(_("\n"));
    printf(_("Usage: %s [options...] [extra tests...]\n"), progname);
    printf(_("\n"));
    printf(_("Options:\n"));
    printf(_("  --dbname=DB               use database DB (default \"regression\")\n"));
    printf(_("  --debug                   turn on debug mode in programs that are run\n"));
    printf(_("  --inputdir=DIR            take input files from DIR (default \".\")\n"));
    printf(_("  --load-language=lang      load the named language before running the\n"));
    printf(_("                            tests; can appear multiple times\n"));
    printf(_("  --load-extension=ext      load the named extension before running the\n"));
    printf(_("                            tests; can appear multiple times\n"));
    printf(_("  --create-role=ROLE        create the specified role before testing\n"));
    printf(_("  --max-connections=N       maximum number of concurrent connections\n"));
    printf(_("                            (default is 0 meaning unlimited)\n"));
    printf(_("  --encoding=ENCODING       use ENCODING as the encoding\n"));
    printf(_("  --outputdir=DIR           place output files in DIR (default \".\")\n"));
    printf(_("  --schedule=FILE           use test ordering schedule from FILE\n"));
    printf(_("                            (can be used multiple times to concatenate)\n"));
    printf(_("  --dlpath=DIR              look for dynamic libraries in DIR\n"));
    printf(_("  --temp-install=DIR        create a temporary installation in DIR\n"));
    printf(_("  --use-existing            use an existing installation\n"));
    printf(_("  --launcher=CMD            use CMD as launcher of gsql\n"));
    printf(_("  --skip_environment_cleanup do not clean generated sql scripts\n"));
    printf(_("\n"));
    printf(_("Options for \"temp-install\" mode:\n"));
    printf(_("  --no-locale               use C locale\n"));
    printf(_("  --top-builddir=DIR        (relative) path to top level build directory\n"));
    printf(_("  --port=PORT               start postmaster on PORT\n"));
    printf(_("  --temp-config=PATH        append contents of PATH to temporary config\n"));
    printf(_("  --extra-install=DIR       additional directory to install (e.g., contrib\n"));
    printf(_("  --hdfshostname=IPAddress	  hdfs data IP address\n"));
    printf(_("  --hdfsstoreplus=hdfsstoreplus	  hdfs data store path plus information\n"));
    printf(_("  --hdfscfgpath=ConfigPath	  config path for principle and keytab file path\n"));
    printf(_("	--hdfsport=N		  hdfs data port\n"));
    printf(_("  --obshostname=IPAddress   obs data IP address\n"));
    printf(_("  --obsbucket=BucketName   obs Bucket \n"));
    printf(_("  --ak=access_key   obs server access key \n"));
    printf(_("  --sk=secret_access_key   obs server secret access key \n"));
    printf(_("  --g_aiehost=host_ip           AI engine host IP \n"));
    printf(_("  --g_aieport=port_number       AI engine port number \n"));
    printf(_("\n"));
    printf(_("Options for using an existing installation:\n"));
    printf(_("  --host=HOST               use postmaster running on HOST\n"));
    printf(_("  --port=PORT               use postmaster running at PORT\n"));
    printf(_("  --user=USER               connect as USER\n"));
    printf(_("  --psqldir=DIR             use gsql in DIR (default: find in PATH)\n"));
    printf(_("  --enable-segment          create table default with segment=on"));
    printf(_("  --jdbc          enable jdbc regression test"));
    printf(_("\n"));
    printf(_("The exit status is 0 if all tests passed, 1 if some tests failed, and 2\n"));
    printf(_("if the tests could not be run for some reason.\n"));
}

static int initialize_myinfo(
    int coordnode_num, int datanode_num, int init_port, bool keep_last_time_data, bool run_test_case)
{
    int i;
    int* coordnode_port = NULL;
    int* coordnode_pooler_port = NULL;
    int* coordnode_stream_ctl_port = NULL;
    int* coordnode_sctp_port = NULL;

    int* datanode_port = NULL;
    int* datanode_pooler_port = NULL;
    int* datanode_stream_ctl_port = NULL;
    int* datanode_sctp_port = NULL;

    /* if standby configed. */
    int* dns_port = NULL;
    int* dns_sctp_port = NULL;
    int* dns_ctl_port = NULL;
    int* dn_primary_port = NULL;
    int* dn_standby_port = NULL;
    int* dn_secondary_port = NULL;

    if (coordnode_num) {
        coordnode_port = (int*)malloc(sizeof(int) * coordnode_num);
        coordnode_pooler_port = (int*)malloc(sizeof(int) * coordnode_num);
        coordnode_stream_ctl_port = (int*)malloc(sizeof(int) * coordnode_num);
        coordnode_sctp_port = (int*)malloc(sizeof(int) * coordnode_num);
    }

    datanode_pooler_port = (int*)malloc(sizeof(int) * datanode_num);

    datanode_port = (int*)malloc(sizeof(int) * datanode_num);
    datanode_stream_ctl_port = (int*)malloc(sizeof(int) * datanode_num);
    datanode_sctp_port = (int*)malloc(sizeof(int) * datanode_num);

    if (standby_defined) {
        dns_port = (int*)malloc(sizeof(int) * datanode_num);
        dns_ctl_port = (int*)malloc(sizeof(int) * datanode_num);
        dns_sctp_port = (int*)malloc(sizeof(int) * datanode_num);
        dn_primary_port = (int*)malloc(sizeof(int) * datanode_num);
        dn_standby_port = (int*)malloc(sizeof(int) * datanode_num);
        dn_secondary_port = (int*)malloc(sizeof(int) * datanode_num);
    }

    for (i = 0; i < coordnode_num; i++) {
        coordnode_port[i] = init_port++;
        coordnode_pooler_port[i] = init_port++;
        coordnode_sctp_port[i] = init_port++;
        coordnode_stream_ctl_port[i] = init_port++;
    }

    /* reserve port for datanode instance */
    for (i = 0; i < datanode_num; i++) {
        datanode_port[i] = init_port++;
        datanode_pooler_port[i] = init_port++;
        datanode_sctp_port[i] = init_port++;  // Reserve sctp port for datanode.
        datanode_stream_ctl_port[i] = init_port++;

        if (standby_defined) {
            dns_port[i] = init_port++;
            init_port++;
            dns_sctp_port[i] = init_port++;
            init_port++;
            dns_ctl_port[i] = init_port++;
            dn_primary_port[i] = init_port++;
            dn_standby_port[i] = init_port++;
            dn_secondary_port[i] = init_port++;
        }
    }

    myinfo.co_num = coordnode_num;
    myinfo.dn_num = datanode_num;
    myinfo.co_port = coordnode_port;
    myinfo.co_ctl_port = coordnode_stream_ctl_port;
    myinfo.co_sctp_port = coordnode_sctp_port;
    myinfo.co_pool_port = coordnode_pooler_port;
    myinfo.dn_port = datanode_port;
    myinfo.dns_port = dns_port;
    myinfo.dn_pool_port = datanode_pooler_port;
    myinfo.dn_ctl_port = datanode_stream_ctl_port;
    myinfo.dn_sctp_port = datanode_sctp_port;
    myinfo.dns_ctl_port = dns_ctl_port;
    myinfo.dns_sctp_port = dns_sctp_port;
    myinfo.dn_primary_port = dn_primary_port;
    myinfo.dn_standby_port = dn_standby_port;
    myinfo.dn_secondary_port = dn_secondary_port;
    myinfo.gtm_port = init_port;
    myinfo.keep_data = keep_last_time_data;
    myinfo.run_check = run_test_case;
    myinfo.shell_count = 0;

    return 0;
}

/*
 * Compares two strings case insensitively. Returns 0 if the strings matched.
 * A non zero value otherwise
 */
static int regressStrncasecmp(const char* rs1, const char* rs2, int iMaxLenToCmp)
{
    int i;
    int* piMaxLenCmp = &iMaxLenToCmp;

    if (iMaxLenToCmp == -1)
        piMaxLenCmp = &i;

    for (i = 1; i <= *piMaxLenCmp; i++) {
        unsigned char ch1 = (unsigned char)*rs1++;
        unsigned char ch2 = (unsigned char)*rs2++;

        if (ch1 != ch2) {
            /* Convert all into lower case */
            if ((ch1 >= 'A') && (ch1 <= 'Z'))
                ch1 += 'a' - 'A';

            if ((ch2 >= 'A') && (ch2 <= 'Z'))
                ch2 += 'a' - 'A';

            /* String did not match */
            if (ch1 != ch2)
                return (int)ch1 - (int)ch2;
        }

        if (ch1 == 0)
            break;
    }

    return 0;
}

/*
 * Validate the configuration item value of type: "on" or "off"
 */
static bool regrValidateBoolConfigVal(const char* pcConfigValue, int iConfId)
{
    int len = strlen(pcConfigValue);

    if (len == 0) {
        fprintf(stderr,
            _("\n Value not specified for configuration item: [%s]."
              "Value should be \'off\' or \'on\'!\n"),
            aacRegConfItemName[iConfId]);
        return false;
    }

    if ((0 != regressStrncasecmp(pcConfigValue, "on", -1)) && (0 != regressStrncasecmp(pcConfigValue, "off", -1))) {
        fprintf(stderr,
            _("\n Value for the configuration item: [%s] should be"
              " \'on\' or \'off\'. User has given: \'%s\'\n"),
            aacRegConfItemName[iConfId],
            pcConfigValue);
        return false;
    }

    return true;
}

/* Set the value of the configuration item: "column_name_present" into the
 * globals */
static void processTuplesOnlyConfItem(char* pcConfigValue)
{
    if (false == regrValidateBoolConfigVal(pcConfigValue, REGR_TUPONLY_ID))
        exit(2);

    /* column_name_present = off ie tuples only */
    if (0 == regressStrncasecmp(pcConfigValue, "off", -1))
        (void)strncpy((char*)g_stRegrConfItems.acTuplesOnly, "-t", (MAX_TUPLE_ONLY_STRLEN - 1));
}

/* Set the value of the configuration item:"column_separator" into the globals*/
static void processColSepConfItem(const char* pcConfigValue)
{
    int len;
    char* pcTmp = (char*)pcConfigValue;

    len = strlen(pcTmp);
    if (len == 0) {
        fprintf(stderr,
            _("\n Value not specified for configuration item: [%s]!"
              "Value should be a string, indicating the field separator. \n"),
            aacRegConfItemName[REGR_COL_SEP_ID]);
        exit(2);
    }

    if (*pcTmp != '\'') {
        fprintf(stderr,
            _("\n Syntax error in the regress "
              "configuration item:%s! \' missing at the start of "
              "the value! \n"),
            aacRegConfItemName[REGR_COL_SEP_ID]);
        exit(2);
    }

    /* Ignoring the start ' character */
    pcTmp++;
    len--;
    if ((len == 0) || (pcTmp[len - 1] != '\'')) {
        fprintf(stderr,
            _("\n Syntax error in the regress "
              "configuration item:%s! \' missing at the end of "
              "the value! \n"),
            aacRegConfItemName[REGR_COL_SEP_ID]);
        exit(2);
    }

    /* Overwriting the end ' character */
    pcTmp[len - 1] = '\0';

    len = strlen(pcTmp);

    /* User has given empty string. If no column separator is required,
     * Space character shall be given but cannot be empty */
    if (len == 0) {
        fprintf(stderr,
            _("\n User has given empty string. If no column "
              "separator is required, Space character shall be given but it "
              "cannot be empty. \n"));
        exit(2);
    }

    if (len > MAX_COLUMN_SEP_STRLEN) {
        fprintf(stderr,
            _("\n Maximum allowed length of the column separator "
              "string is %d!! Given length: %d. Given string: [%s].\n"),
            MAX_COLUMN_SEP_STRLEN,
            len,
            pcTmp);
        exit(2);
    }

    sprintf((char*)g_stRegrConfItems.acFieldSepForAllText, "-C\"%s\"", pcTmp);
}

/* Process the configuration item: performance_data_printing */
static void processPerfDataLoggingConf(const char* pcConfigValue)
{
    if (false == regrValidateBoolConfigVal(pcConfigValue, REGR_PERF_DATA_LOGGING_ID))
        exit(2);

    if (0 == regressStrncasecmp(pcConfigValue, "on", -1))
        g_bEnablePerfDataPrint = true;
}

static void processDiagCollectorConf(const char* pcConfigValue)
{
    if (false == regrValidateBoolConfigVal(pcConfigValue, REGR_DIAG_COLLECT_ID))
        exit(2);

    if (0 == regressStrncasecmp(pcConfigValue, "on", -1))
        g_bEnableDiagCollection = true;
}

/* Initialize of the module-global structure instance for storing the
 * replacement pattern strings */
static void regrInitReplcPattStruct(void)
{
    g_stRegrReplcPatt.iNumOfPatterns = 0;
    g_stRegrReplcPatt.iRemainingPattBuffSize = 0;
    g_stRegrReplcPatt.iMaxNumOfPattern = REGR_NUM_OF_REPLC_ITEMS_CHUNK;
    g_stRegrReplcPatt.puiPatternOffset = NULL;
    g_stRegrReplcPatt.puiPattReplValOffset = NULL;
    g_stRegrReplcPatt.pcBuf = NULL;
}

/*
 * This functions checks whether the line starts with a global variable name i.e. @....@.
 * If yes, it returns true.
 *		 ppcRegConfLine will be pointing to the character after the global
 *      	 variable name (maybe to the '=' character)
 *		 piPattLen will be having the length of the global variable name.
 *
 * If no, it returns false.
 */
static bool regrIsStringPatternToBeReplaced(char** ppcRegConfLine, int* piPattLen)
{
    char* pcCurLine = *ppcRegConfLine;

    /* Format for specifying the replacement string is
     * @VAR@ = 'VALUE'
     * So the first character should be REGR_REPLC_PATT_START_CHAR */
    if (*(pcCurLine++) != REGR_REPLC_PATT_START_CHAR)
        return false;

    /* Setting the length of the replacement pattern string as 1 on counting @*/
    *piPattLen = 1;

    while ((*pcCurLine != '\0') && (*pcCurLine != ' ') && (*pcCurLine != '\t') && (*pcCurLine != '=')) {
        (*piPattLen)++;
        pcCurLine++;
    }

    if ((*piPattLen == 1) || ((*(pcCurLine - 1)) != REGR_REPLC_PATT_END_CHAR)) {
        fprintf(stderr,
            _("\n Missing %c at the end of the pattern replacement "
              "string! FORMAT is @PATTERN_NAME@ = \'VALUE\' . Line: [%s] \n"),
            REGR_REPLC_PATT_END_CHAR,
            *ppcRegConfLine);
        return false;
    }

    /* LHS cant be @@ */
    if (*piPattLen == 2) {
        fprintf(stderr,
            _("\n Replacement Pattern string should not be %c%c\n"),
            REGR_REPLC_PATT_START_CHAR,
            REGR_REPLC_PATT_END_CHAR);
        return false;
    }

    /* Replacement pattern string should be immediately followed by space or =*/
    if ((*pcCurLine != ' ') && (*pcCurLine != '\t') && (*pcCurLine != '=')) {
        fprintf(stderr,
            _("\n Replacement pattern string should be immediately "
              "followed by \'space\' or \'Equal to\' character."
              " Given line: [%s]\n"),
            *ppcRegConfLine);
        return false;
    }

    while ((*pcCurLine == ' ') || (*pcCurLine == '\t'))
        pcCurLine++;

    *ppcRegConfLine = pcCurLine;
    return true;
}

/* Checks whether the current line from regress.conf is an entry for the
 * replacement pattern string.
 *
 * If yes,
 * piPattLen will be pointing to the length of the replacement pattern string
 * ppcReplValue will be pointing to the string to be replaced with
 * piValLen will be pointing to the length of the string to be replaced with
 * Function returns true
 *
 * If no,
 * Function returns false.
 */
static bool regrIsItemReplcPatt(const char* pcLine, int* piPattLen, char** ppcReplValue, int* piValLen)
{
    int len;
    char* pcCurLine = (char*)pcLine;

    /* Checking whether the start of the line contains a GLOBAL VARIABLE, i.e.
     * @....@ */
    if (false == regrIsStringPatternToBeReplaced(&pcCurLine, piPattLen))
        return false;

    /* Skipping the pattern name - value separator character: '=' */
    if (*pcCurLine == '=')
        pcCurLine++;

    regrTrimString(&pcCurLine);

    if (*pcCurLine == '\0') {
        fprintf(stderr,
            _("\n Missing value for the replacement pattern"
              "string! FORMAT is @PATTERN_NAME@ = \'VALUE\' . Line: [%s] \n"),
            pcLine);
        return false;
    }

    len = strlen(pcCurLine);

    /* Validation to ensure that the value is given within ' or " characters */
    if ((len == 1) || ((*pcCurLine != '\'') && (*pcCurLine != '\"')) ||
        ((pcCurLine[len - 1] != '\'') && (pcCurLine[len - 1] != '\"'))) {
        fprintf(stderr,
            _("\n Value should be enclosed within \' or \" "
              "characters. Given Value: [%s]\n"),
            pcCurLine);
        return false;
    }

    /* Removing the ' or " at the end of the value */
    pcCurLine[len - 1] = '\0';

    /* Avoiding the ' or " at the beginning of the value and updating the
     * output parameter */
    *ppcReplValue = pcCurLine + 1;
    *piValLen = strlen(*ppcReplValue);

    return true;
}

/* Allocates memory for the storage of the replacement pattern strings, their
 * corresponding replacement strings, and the index for the both. The offset to
 * the location which is going to be used for the storage of the replacement
 * pattern string, will be initialized to the index slot to be used next.
 *
 * Memory Layout:
 *
 *		INDEX 1			INDEX 2				STORAGE SPACE
 * +----------------+-------------------+-----------------------------+
 * | Offsets to the	| Offsets to		| Actual storage for Patterns |
 * | Patterns to be	| the strings to be	| to be replaced and their	  |
 * | replaced		| replaced for 		| corresponding values        |
 * |				| the given patterns| They gets stored as pairs.  |
 * |				|					| \0 is explicitly stored.    |
 * +----------------+-------------------+-----------------------------+
 *										^
 *										|
 *									Offset start
 *									i.e. g_stRegrReplcPatt.pcBuf
 */
static int regrAllocMemForReplcPatt()
{
    int iBuffSize;
    int iOldMaxNumOfPatt = 0;
    int iSizeOfIdxSect;
    unsigned int* puiPattBkup = NULL;

    /* Case of reallocation. Back up the address of the old memory block */
    if (g_stRegrReplcPatt.puiPatternOffset != NULL) {
        puiPattBkup = g_stRegrReplcPatt.puiPatternOffset;
        iOldMaxNumOfPatt = g_stRegrReplcPatt.iMaxNumOfPattern;
        g_stRegrReplcPatt.iMaxNumOfPattern += REGR_NUM_OF_REPLC_ITEMS_CHUNK;
    }

    /* Size of the INDEX BLOCK for the easy access of the storage buffer */
    iSizeOfIdxSect = REGR_SIZE_OF_INDEX_ELEM * g_stRegrReplcPatt.iMaxNumOfPattern;

    /* Size of the buffer excluding the offset sections */
    iBuffSize = (REGR_AVG_REPLC_PATT_LEN + REGR_AVG_REPLC_PATT_VAL_LEN) * g_stRegrReplcPatt.iMaxNumOfPattern;

    /* Allocating memory for storing the pattern replacement strings
     * from the regress.conf file */
    g_stRegrReplcPatt.puiPatternOffset = (unsigned int*)malloc((iSizeOfIdxSect * 2) + iBuffSize);
    if (g_stRegrReplcPatt.puiPatternOffset == NULL) {
        fprintf(stderr,
            _("Could not allocate memory for parsing "
              "regress.conf file! Requested Size: %d. \n"),
            (int)((iSizeOfIdxSect * 2) + iBuffSize));
        return REGR_ERRCODE_MALLOC_FAILURE;
    }

    memset(g_stRegrReplcPatt.puiPatternOffset, '\0', (iSizeOfIdxSect * 2) + iBuffSize);

    /* Initialize the remaining size of the storage buffer as the storage buffer
     * size of the newly allocated memory */
    g_stRegrReplcPatt.iRemainingPattBuffSize = iBuffSize;
    g_stRegrReplcPatt.puiPattReplValOffset =
        (unsigned int*)(((char*)(void*)g_stRegrReplcPatt.puiPatternOffset) + iSizeOfIdxSect);

    g_stRegrReplcPatt.pcBuf = ((char*)(void*)g_stRegrReplcPatt.puiPattReplValOffset) + iSizeOfIdxSect;

    /* Copy contents of old memory block into newly allocated memory */
    if (puiPattBkup != NULL) {
        /* Copying the offsets of replacement pattern strings */
        memcpy(g_stRegrReplcPatt.puiPatternOffset, puiPattBkup, REGR_SIZE_OF_INDEX_ELEM * iOldMaxNumOfPatt);

        /* Copying offsets of 'values' of the 'replacement pattern strings' */
        memcpy(g_stRegrReplcPatt.puiPattReplValOffset,
            (char*)(void*)puiPattBkup + (REGR_SIZE_OF_INDEX_ELEM * iOldMaxNumOfPatt),
            REGR_SIZE_OF_INDEX_ELEM * iOldMaxNumOfPatt);

        /* Copying the core contents of the old memory block excluding the index
         * area */
        memcpy(g_stRegrReplcPatt.pcBuf,
            (char*)(void*)puiPattBkup + (REGR_SIZE_OF_INDEX_ELEM * iOldMaxNumOfPatt * 2),
            ((REGR_AVG_REPLC_PATT_LEN + REGR_AVG_REPLC_PATT_VAL_LEN) * iOldMaxNumOfPatt));

        if (g_stRegrReplcPatt.iNumOfPatterns > 0) {
            int iOffIdx = g_stRegrReplcPatt.iNumOfPatterns;
            int iPrevOffset = g_stRegrReplcPatt.puiPatternOffset[iOffIdx - 1];
            int iNewOffset = iPrevOffset + strlen(&(g_stRegrReplcPatt.pcBuf[iPrevOffset])) + 1;

            /* Updating the offset of the next string's location, which is going
             * to be used */
            g_stRegrReplcPatt.puiPatternOffset[iOffIdx] = iNewOffset;

            /* Subtracting the size of the copied contents from the
             * 'new total buffer size' */
            g_stRegrReplcPatt.iRemainingPattBuffSize -= (iBuffSize - iNewOffset);
        }

        /* Freeing the old memory block */
        REGR_FREE(puiPattBkup);
    }

    return REGR_SUCCESS;
}

/* Load the replacement pattern strings and their values into global memory */
static void regrProcessReplcPattLine(const char* pcLine, int iPattLen, const char* pcReplcPattValue, int iPattValueLen)
{

    int iIdx = g_stRegrReplcPatt.iNumOfPatterns;
    unsigned int* puiPattOff = g_stRegrReplcPatt.puiPatternOffset;
    unsigned int* puiValOff = g_stRegrReplcPatt.puiPattReplValOffset;
    char* pcBuf = g_stRegrReplcPatt.pcBuf;
    unsigned int uiCurrOff = puiPattOff[iIdx];

    /* Load the pattern string to be replaced into global memory */
    (void)strncpy((char*)&(pcBuf[puiPattOff[iIdx]]), pcLine, iPattLen);

    /* Update the offset of the replacement pattern string's value in the
     * corresponding index section. + 1 is considering the \0. Also note that
     * we are not keeping \0 explicitly because the entire buffer is memset with
     * 0 */
    puiValOff[iIdx] = uiCurrOff + iPattLen + 1;

    /* Load the value to be replaced for the pattern string, into global
     * memory */
    errno_t rc = memcpy_s((char*)&(pcBuf[puiValOff[iIdx]]), iPattValueLen, pcReplcPattValue, iPattValueLen);
    securec_check_c(rc, "\0", "\0");

    /* One pattern and its values is now loaded. So increment the loaded
     * pattern count */
    g_stRegrReplcPatt.iNumOfPatterns++;

    /* If this IF CHECK is false, then we have filled the last pattern and its
     * value. If next pair has to be loaded then memory need to be extented.
     * Next offset will be updated during that time  and here nothing is to be
     * done */
    if (g_stRegrReplcPatt.iNumOfPatterns < g_stRegrReplcPatt.iMaxNumOfPattern)
        /* Updating the offset to the next usable location */
        puiPattOff[g_stRegrReplcPatt.iNumOfPatterns] = puiValOff[iIdx] + (unsigned int)iPattValueLen + (unsigned int)1;

    /* +2 is considering the \0 */
    g_stRegrReplcPatt.iRemainingPattBuffSize -= (iPattLen + iPattValueLen + 2);
}

/* Load the regress configuration item values into the globals */
static void loadRegressConf(char* pcRegConfigFile)
{
    int len;
    int iRegConfInd;
    int iPattLen = 0;
    int iPattValueLen;
    bool bLineValid = false;
    char buf[MAX_LINE_LEN];
    char* pcLine = NULL;
    char* pcTemp = NULL;
    char* pcReplcPattValue = NULL;
    FILE* pfRegConf = NULL;

    /* Open the regress.conf file */
    pfRegConf = fopen(pcRegConfigFile, "r");
    if (NULL == pfRegConf) {
        fprintf(stderr,
            _("\n Could not open the regression configuration "
              "file: [%s]. Continuing with default values.\n"),
            pcRegConfigFile);
        return;
    }

    /* Process each line in the regress.conf file */
    while (fgets(buf, sizeof(buf), pfRegConf) != NULL) {
        pcLine = buf;

        /* Remove white space on LHS and white space/CarriageReturn on RHS */
        regrTrimString(&pcLine);

        /* # at the start of the line indicates a full comment line. Entire line
         * can be ignored */
        if (*pcLine == '#')
            continue;

        /* Empty lines can be ignored */
        if (*pcLine == '\0')
            continue;

        /* Comments can be started any where in the line. RHS of #, in a line,
         * can be ignored */
        pcTemp = strstr(pcLine, "#");
        if (pcTemp != NULL)
            *pcTemp = '\0';

        bLineValid = false;

        for (iRegConfInd = 0; iRegConfInd < MAX_REG_CONF_ITEMS; iRegConfInd++) {
            len = strlen(aacRegConfItemName[iRegConfInd]);

            if (0 == regressStrncasecmp(pcLine, aacRegConfItemName[iRegConfInd], len)) {
                pcTemp = pcLine + len;

                /* Configuration Item name should be immediately followed by  */
                if ((*pcTemp != ' ') && (*pcTemp != '\t') && (*pcTemp != '='))
                    continue;

                while ((*pcTemp == ' ') || (*pcTemp == '\t'))
                    pcTemp++;

                if (*pcTemp == '=')
                    pcTemp++;

                regrTrimString(&pcTemp);

                /* pcTemp will be now containing the value */
                switch (iRegConfInd) {
                    case REGR_TUPONLY_ID:
                        processTuplesOnlyConfItem(pcTemp);
                        break;
                    case REGR_COL_SEP_ID:
                        processColSepConfItem(pcTemp);
                        break;

                    case REGR_PERF_DATA_LOGGING_ID:
                        processPerfDataLoggingConf(pcTemp);
                        break;

                    case REGR_DIAG_COLLECT_ID:
                        processDiagCollectorConf(pcTemp);
                        break;
                }

                /* Marking the line regress.conf as valid */
                bLineValid = true;

                /* Line matches with one of the config item. So process and get
                 * next line */
                break;
            }
        }

        if (bLineValid == false) {
            regrTrimString(&pcLine);

            if (regrIsItemReplcPatt(pcLine, &iPattLen, &pcReplcPattValue, &iPattValueLen)) {
                if ((g_stRegrReplcPatt.iNumOfPatterns == 0) ||
                    (g_stRegrReplcPatt.iNumOfPatterns == g_stRegrReplcPatt.iMaxNumOfPattern) ||
                    ((iPattLen + iPattValueLen + 2) > g_stRegrReplcPatt.iRemainingPattBuffSize)) {
                    do {
                        if (regrAllocMemForReplcPatt() != REGR_SUCCESS)
                            goto LB_MALLOC_REPLC_PATT_ERR;
                    } while ((iPattLen + iPattValueLen + 2) > g_stRegrReplcPatt.iRemainingPattBuffSize);
                }

                regrProcessReplcPattLine(pcLine, iPattLen, pcReplcPattValue, iPattValueLen);
            } else
                fprintf(stderr,
                    _("\n Unknown Configuration item. Line: [%s] "
                      "ignored. \n"),
                    pcLine);
        }
    } /* while (fgets ... */

LB_MALLOC_REPLC_PATT_ERR:
    fclose(pfRegConf);
}

static void drop_role_if_exists(const char* rolename)
{
    header(_("dropping role \"%s\""), rolename);
    psql_command("postgres", "DROP ROLE IF EXISTS \"%s\"", rolename);
}

static void drop_database_if_exists(const char* dbname)
{
    header(_("dropping database \"%s\""), dbname);
    psql_command("postgres", "DROP DATABASE IF EXISTS \"%s\"", dbname);
}

static void check_upgrade_options()
{
    if ((inplace_upgrade || grayscale_upgrade != -1) && !upgrade_from) {
        fprintf(stderr, _("Must provide upgrade_from parameter when doing upgrade check\n"));
        exit_nicely(2);
    }

    if (!(inplace_upgrade || grayscale_upgrade != -1) && upgrade_from) {
        fprintf(stderr, _("Please do not provide upgrade_from parameter when not doing upgrade check\n"));
        exit_nicely(2);
    }

    if ((inplace_upgrade || grayscale_upgrade != -1) && myinfo.keep_data) {
        fprintf(stderr, _("At present, upgrade check do no support keep data test.\n"));
        exit_nicely(2);
    }
}

static void setup_super_user()
{
    header(_("setting super user for upgrade check"));
    header(_("sleeping"));

    sleep(3);

    int i, j;
    struct passwd* pwd;

    pwd = getpwuid(getuid());

    if (NULL == pwd || NULL == pwd->pw_name) {
        fprintf(stderr, _("Can not get current user name.\n"));
        exit_nicely(2);
    }

    for (i = 0; i < myinfo.co_num; i++) {
        psql_command_node("postgres", i, COORD, "update pg_authid set rolname='%s' where rolsuper=true;", pwd->pw_name);
    }

    for (j = 0; j < myinfo.dn_num; j++) {
        psql_command_node(
            "postgres", j, DATANODE, "update pg_authid set rolname='%s' where rolsuper=true;", pwd->pw_name);
    }
}

/*
 * Start the temp postmaster
 */
static void start_postmaster(void)
{
    int i;
    int j = 0;

    header(_("starting postmaster"));

    if (!test_single_node) {
        /* Start GTM */
        start_gtm();

        start_my_node(0, COORD, true, false, upgrade_from);
        for (i = 1; i < myinfo.co_num; i++) {
            start_my_node(i, COORD, false, false, upgrade_from);
        }

        for (i = 0; i < myinfo.dn_num; i++) {
            start_my_node(i, DATANODE, false, standby_defined, upgrade_from);
        }
    } else {
        assert(0 == myinfo.co_num && 1 == myinfo.dn_num);
        start_single_node();
    }

    pg_usleep(10000000L);

    /*
     * Wait till postmaster is able to accept connections (normally only a
     * second or so, but Cygwin is reportedly *much* slower).  Don't wait
     * forever, however.
     */

    /* Check node failure */
    for (i = 0; i < myinfo.co_num; i++) {
        check_node_fail(i, COORD);
    }
    for (i = 0; i < myinfo.dn_num; i++) {
        check_node_fail(i, DATANODE);
    }

    if (j >= 60) {
        /* If one node fails, all fail */

        for (i = 0; i < myinfo.co_num; i++) {
            kill_node(i, COORD);
        }
        for (i = 0; i < myinfo.dn_num; i++) {
            kill_node(i, DATANODE);
        }
        for (i = 0; i < myinfo.shell_count; i++) {
            kill_node(i, SHELL);
        }
        exit_nicely(2);
    }

    if (standby_defined) {
        for (i = 0; i < myinfo.dn_num; i++) {
            setup_datanode_replication(i);
        }
    }

    postmaster_running = true;

#ifdef WIN64
    /* need a series of two casts to convert HANDLE without compiler warning */
#define ULONGPID(x) (unsigned long)(unsigned long long)(x)
#else
#define ULONGPID(x) (unsigned long)(x)
#endif

    /* Print info for each node */
    for (i = 0; i < myinfo.co_num; i++)
        printf(_("running on port %d with PID %lu for Coordinator %d\n"),
            get_port_number(i, COORD),
            ULONGPID(get_node_pid(i, COORD)),
            i + 1);

    for (i = 0; i < myinfo.dn_num; i++)
        printf(_("running on port %d with PID %lu for Datanode %d\n"),
            get_port_number(i, DATANODE),
            ULONGPID(get_node_pid(i, DATANODE)),
            i + 1);
}

int regression_main(int argc, char* argv[], init_function ifunc, test_function tfunc, diag_function dfunc)
{
#ifdef BUILD_BY_CMAKE
    get_value_from_env(&code_base_src, "CODE_BASE_SRC");
    get_value_from_cwd(&current_exe_dir);
    get_value_from_env(&CMAKE_PGBINDIR, "PREFIX_HOME");
    get_value_from_env(&CMAKE_LIBDIR, "PREFIX_HOME");
    get_value_from_env(&CMAKE_PGSHAREDIR, "PREFIX_HOME");

    printf("--------------------------------------------------------------------------------------");
    printf("\ncode_base_src:%s\n", code_base_src);
    printf("\ncurrent_exe_dir:%s\n", current_exe_dir);
    printf("\nCMAKE_PGBINDIR:%s\n", CMAKE_PGBINDIR);

#endif
    _stringlist* ssl = NULL;
    int c;
    int i;
    int option_index;
    char buf[MAXPGPATH * 4];

    int coordnode_num = 3;
    int datanode_num = 12;
    int init_port = 25632;
    bool keep_last_time_data = false;
    bool run_test_case = true;
    bool run_qunit = false;
    char* qunit_module = "all";
    char* qunit_level = "all";
    bool override_installdir = false;
    bool only_install = false;
    int kk = 0;

    struct timeval start_time;
    struct timeval end_time;
    double total_time;

    struct timeval start_time_total;
    struct timeval end_time_total;

    (void)gettimeofday(&start_time_total, NULL);

    static struct option long_options[] = {{"help", no_argument, NULL, 'h'},
        {"version", no_argument, NULL, 'V'},
        {"dbname", required_argument, NULL, 1},
        {"debug", no_argument, NULL, 2},
        {"inputdir", required_argument, NULL, 3},
        {"load-language", required_argument, NULL, 4},
        {"max-connections", required_argument, NULL, 5},
        {"encoding", required_argument, NULL, 6},
        {"outputdir", required_argument, NULL, 7},
        {"schedule", required_argument, NULL, 8},
        {"temp-install", required_argument, NULL, 9},
        {"no-locale", no_argument, NULL, 10},
        {"top-builddir", required_argument, NULL, 11},
        {"host", required_argument, NULL, 13},
        {"port", required_argument, NULL, 14},
        {"user", required_argument, NULL, 15},
        {"psqldir", required_argument, NULL, 16},
        {"dlpath", required_argument, NULL, 17},
        {"create-role", required_argument, NULL, 18},
        {"temp-config", required_argument, NULL, 19},
        {"use-existing", no_argument, NULL, 20},
        {"launcher", required_argument, NULL, 21},
        {"load-extension", required_argument, NULL, 22},
        {"extra-install", required_argument, NULL, 23},
        {"noclean", no_argument, NULL, 24},
        {"regconf", required_argument, NULL, 25},
        {"hdfshostname", required_argument, NULL, 26},
        {"hdfsport", required_argument, NULL, 27},
        {"hdfscfgpath", required_argument, NULL, 28},
        {"hdfsstoreplus", required_argument, NULL, 29},
        {"obshostname", required_argument, NULL, 30},
        {"obsbucket", required_argument, NULL, 31},
        {"keep_last_data", required_argument, NULL, 32},
        {"securitymode", no_argument, NULL, 33},
        {"abs_gausshome", required_argument, NULL, 34},
        {"ak", required_argument, NULL, 35},
        {"sk", required_argument, NULL, 36},
        {"inplace_upgrade", no_argument, NULL, 37},
        {"init_database", no_argument, NULL, 38},
        {"data_base_dir", required_argument, NULL, 39},
        {"upgrade_from", required_argument, NULL, 40},
        {"upgrade_script_dir", required_argument, NULL, 41},
        {"ignore-exitcode", no_argument, NULL, 42},
        {"keep-run", no_argument, NULL, 43},
        {"parallel_initdb", no_argument, NULL, 44},
        {"single_node", no_argument, NULL, 45},
        {"qunit", no_argument, NULL, 46},
        {"qunit_module", required_argument, NULL, 47},
        {"qunit_level", required_argument, NULL, 48},
        {"old_bin_dir", required_argument, NULL, 49},
        {"grayscale_mode", no_argument, NULL, 50},
        {"grayscale_full_mode", no_argument, NULL, 51},
        {"uc", required_argument, NULL, 52},
        {"ud", required_argument, NULL, 53},
        {"upgrade_schedule", required_argument, NULL, 54},
        {"platform", required_argument, NULL, 55},
        {"g_aiehost", required_argument, NULL, 56},
        {"g_aieport", required_argument, NULL, 57},
        {"enable-segment", no_argument, NULL, 58},
        {"client_logic_hook", required_argument, NULL, 59},
        {"jdbc", no_argument, NULL, 60},
        {"skip_environment_cleanup", no_argument, NULL, 61},
        {NULL, 0, NULL, 0}
    };

    progname = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_regress"));

#ifndef HAVE_UNIX_SOCKETS
    /* no unix domain sockets available, so change default */
    hostname = "localhost";
#endif

    /*
     * We call the initialization function here because that way we can set
     * default parameters and let them be overwritten by the commandline.
     */
    ifunc();

    while ((c = getopt_long(argc, argv, "b:c:d:hp:r:s:V:n:w", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h':
                help();
                exit_nicely(0);
                /* fall through */
            case 'V':
                puts("pg_regress (PostgreSQL) " PG_VERSION);
                exit_nicely(0);
                /* fall through */
            case 'd':
                datanode_num = atoi(optarg);
                break;
            case 'c':
                coordnode_num = atoi(optarg);
                break;
            case 'p':
                init_port = atoi(optarg);
                break;
            case 'r':
                kk = atoi(optarg);
                if (kk == 2) {
                    only_install = true;
                    run_test_case = false;
                } else if (kk == 0)
                    run_test_case = false;
                else
                    run_test_case = true;

                break;
            case 'b':
                temp_install = make_absolute_path(optarg);
                override_installdir = true;
                break;
            case 'm':
                if (strncmp(optarg, "tcp", 3) != 0)
                    comm_tcp_mode = false;
                break;
            case 's':
                standby_defined = atoi(optarg);
                break;
            case 'w':
                change_password = true;
                break;
            case 'n':
                dop = atoi(optarg);
                break;
            case 1:

                /*
                 * If a default database was specified, we need to remove it
                 * before we add the specified one.
                 */
                free_stringlist(&dblist);
                split_to_stringlist(strdup(optarg), ", ", &dblist);
                break;
            case 2:
                debug = true;
                break;
            case 3:
                inputdir = strdup(optarg);
                break;
            case 4:
                add_stringlist_item(&loadlanguage, optarg);
                break;
            case 5:
                max_connections = atoi(optarg);
                break;
            case 6:
                encoding = strdup(optarg);
                break;
            case 7:
                outputdir = strdup(optarg);
                break;
            case 8:
                add_stringlist_item(&schedulelist, optarg);
                break;
            case 9:
                if (override_installdir == false)
                    temp_install = make_absolute_path(optarg);
                break;
            case 10:
                nolocale = true;
                break;
            case 11:
                top_builddir = strdup(optarg);
                break;
            case 13:
                hostname = strdup(optarg);
                break;
            case 14:
                port = atoi(optarg);
                port_specified_by_user = true;
                break;
            case 15:
                user = strdup(optarg);
                break;
            case 16:
                /* "--psqldir=" should mean to use PATH */
                if (strlen(optarg)) {
                    psqldir = strdup(optarg);
                }
                break;
            case 17:
                dlpath = strdup(optarg);
                break;
            case 18:
                split_to_stringlist(strdup(optarg), ", ", &extraroles);
                break;
            case 19:
                temp_config = strdup(optarg);
                break;
            case 20:
                use_existing = true;
                break;
            case 21:
                launcher = strdup(optarg);
                break;
            case 22:
                add_stringlist_item(&loadextension, optarg);
                break;
            case 23:
                add_stringlist_item(&extra_install, optarg);
                break;
            case 24:
                clean = false;
                break;
            case 25:
                pcRegConfFile = strdup(optarg);
                break;
            case 26:
                hdfshostname = strdup(optarg);
                break;
            case 27:
                hdfsport = strdup(optarg);
                break;
            case 28:
                hdfscfgpath = strdup(optarg);
                break;
            case 29:
                hdfsstoreplus = strdup(optarg);
                break;
            case 30:
                obshostname = strdup(optarg);
                break;
            case 31:
                obsbucket = strdup(optarg);
                break;
            case 32: {
                char* tmp_str = NULL;
                tmp_str = strdup(optarg);
                if (pg_strncasecmp(tmp_str, "true", 4) == 0) {
                    keep_last_time_data = true;
                } else {
                    keep_last_time_data = false;
                }
                free(tmp_str);
                break;
            }
            case 33:
                securitymode = true;
                break;
            case 34:
                gausshomedir = strdup(optarg);
                break;
            case 35:
                ak = strdup(optarg);
                break;
            case 36:
                sk = strdup(optarg);
                break;
            case 37:
                inplace_upgrade = true;
                super_user_altered = false;
                passwd_altered = false;
                break;
            case 38:
                init_database = true;
                break;
            case 39:
                data_base_dir = strdup(optarg);
                break;
            case 40:
                upgrade_from = atoi(optarg);
                break;
            case 41:
                upgrade_script_dir = strdup(optarg);
                break;
            case 42:
                ignore_exitcode = true;
                break;
            case 43:
                keep_run = true;
                break;
            case 44:
                parallel_initdb = true;
                break;
            case 45: {
                /* test single datanode mode */
                test_single_node = true;
                break;
            }
            case 46:
                run_qunit = true;
                break;
            case 47:
                qunit_module = strdup(optarg);
                break;
            case 48:
                qunit_level = strdup(optarg);
                break;
            case 49:
                old_bin_dir = strdup(optarg);
                break;
            case 50:
                grayscale_upgrade = 0;
                inplace_upgrade = true;
                super_user_altered = false;
                passwd_altered = false;
                break;
            case 51:
                grayscale_upgrade = 1;
                inplace_upgrade = true;
                super_user_altered = false;
                passwd_altered = false;
                break;
            case 52:
                upgrade_cn_num = atoi(optarg);
                if (upgrade_cn_num > 2 || upgrade_cn_num < 1) {
                    upgrade_cn_num = 1;
                }
                break;
            case 53:
                upgrade_dn_num = atoi(optarg);
                if (upgrade_dn_num > 11 || upgrade_dn_num < 1) {
                    upgrade_dn_num = 4;
                }
                break;
            case 54:
                add_stringlist_item(&upgrade_schedulelist, optarg);
                break;
            case 55:
                platform = strdup(optarg);
                break;
            case 56:
                g_aiehost = strdup(optarg);
                break;
            case 57:
                g_aieport = strdup(optarg);
                break;
            case 58:
                g_enable_segment = true;
                break;
            case 59:
                client_logic_hook = make_absolute_path(optarg);
                break;
            case 60:
                printf("\n starting with jdbc\n");
                use_jdbc_client = true;
                to_create_jdbc_user = true;
                break;
            case 61:
                is_skip_environment_cleanup = true;
                break;
            default:
                /* getopt_long already emitted a complaint */
                fprintf(stderr, _("\nTry \"%s -h\" for more information.\n"), progname);
                exit_nicely(2);
        }
    }

    if (strcmp(platform, "openeuler_aarch64") == 0) {
        grayscale_upgrade = -1;
        inplace_upgrade = false;
    }

    if (run_test_case && run_qunit) {
        fprintf(stderr, _("Can not run qunit and other check simultaneously\n"));
        exit_nicely(2);
    }

    memset(g_stRegrConfItems.acFieldSepForAllText, '\0', sizeof(g_stRegrConfItems.acFieldSepForAllText));
    memset(g_stRegrConfItems.acTuplesOnly, '\0', sizeof(g_stRegrConfItems.acTuplesOnly));

    regrInitReplcPattStruct();

    if (pcRegConfFile)
        loadRegressConf(pcRegConfFile);

    int result = initialize_myinfo(coordnode_num, datanode_num, init_port, keep_last_time_data, run_test_case);
    if (result != 0) {
        return -1;
    }

    // Upgrade check option
    (void)check_upgrade_options();

    /*
     * if we still have arguments, they are extra tests to run
     */
    while (argc - optind >= 1) {
        add_stringlist_item(&extra_tests, argv[optind]);
        optind++;
    }

    if (temp_install && !port_specified_by_user)
    /*
     * To reduce chances of interference with parallel installations, use
     * a port number starting in the private range (49152-65535)
     * calculated from the version number.
     */
    {
        port = 0xC000 | (PG_VERSION_NUM & 0x3FFF);
    }

    inputdir = make_absolute_path(inputdir);
    outputdir = make_absolute_path(outputdir);
    dlpath = make_absolute_path(dlpath);
    data_base_dir = make_absolute_path(data_base_dir);
    old_bin_dir = make_absolute_path(old_bin_dir);
    upgrade_script_dir = make_absolute_path(upgrade_script_dir);
    loginuser = get_id();

#if defined (__x86_64__)
    (void)snprintf(pgbenchdir, MAXPGPATH, "%s/%s", ".", "data/pgbench/x86_64");
#elif defined (__aarch64__)
    (void)snprintf(pgbenchdir, MAXPGPATH, "%s/%s", ".", "data/pgbench/aarch64");
#endif

    /* Check thread local varieble's num */
    check_global_variables();

    /* Check macros like STREAMLAN/PGXC/IS_SINGLE_NODE */
    check_pgxc_like_macros();

    // Do not stop postmaster if we are told not to run test cases.
    if (run_test_case)
        (void)atexit(atexit_cleanup);

    /*
     * create regression.out regression.diff result
     */
    open_result_files();
    // Initialize environment variables, including but not limited to LD_LIBRARY_PATH, PATH
    initialize_environment();

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
    unlimit_core_size();
#endif

    // temp_install != NULL; Whether to specify the installation directory
    if (temp_install) {
        /*
         * Prepare the temp installation
         */
        if (!top_builddir) {
            fprintf(stderr, _("--top-builddir must be specified when using --temp-install\n"));
            exit_nicely(2);
        }

        if (!directory_exists(temp_install)) {
            myinfo.keep_data = false;
        }
#ifndef ENABLE_LLT
        if (myinfo.keep_data) {

            /* "make install" */
#ifndef WIN32_ONLY_COMPILER
            (void)snprintf(buf,
                sizeof(buf),
                SYSTEMQUOTE
                "\"%s\" -C \"%s\" DESTDIR=\"%s/install\" install -sj > \"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
                makeprog,
                top_builddir,
                temp_install,
                outputdir);
#else
            (void)snprintf(buf,
                sizeof(buf),
                SYSTEMQUOTE
                "perl \"%s/src/tools/msvc/install.pl\" \"%s/install\" >\"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
                top_builddir,
                temp_install,
                outputdir);
#endif
            if (system(buf)) {
                fprintf(stderr,
                    _("\n%s: installation failed\nExamine %s/log/install.log for the reason.\nCommand was: %s\n"),
                    progname,
                    outputdir,
                    buf);
                exit_nicely(2);
            }
        }
#endif
        if (myinfo.keep_data == false) {
            if (only_install == false) {
#ifndef ENABLE_LLT
                if (directory_exists(temp_install)) {
                    header(_("removing existing temp installation"));
                    (void)rmtree(temp_install, true);
                }

                header(_("creating temporary installation"));

                /* make the temp install top directory */
                make_directory(temp_install);
#endif

                /* and a directory for log files */
                (void)snprintf(buf, sizeof(buf), "%s/log", outputdir);
                if (!directory_exists(buf)) {
                    make_directory(buf);
                }

#ifdef ENABLE_LLT
                if (directory_exists(temp_install)) {
                    snprintf(buf,
                        sizeof(buf),
                        SYSTEMQUOTE "rm -rf %s/coordinator* > %s/log/install.log 2>&1" SYSTEMQUOTE,
                        temp_install,
                        outputdir);
                    header("%s", buf);
                    if (system(buf)) {
                        fprintf(stderr,
                            _("\n%s: installation failed\nExamine %s/log/install.log for the reason.\nCommand was: "
                              "%s\n"),
                            progname,
                            outputdir,
                            buf);
                        exit_nicely(2);
                    }

                    snprintf(buf,
                        sizeof(buf),
                        SYSTEMQUOTE "rm -rf %s/data* > %s/log/install.log 2>&1" SYSTEMQUOTE,
                        temp_install,
                        outputdir);
                    header("%s", buf);
                    if (system(buf)) {
                        fprintf(stderr,
                            _("\n%s: installation failed\nExamine %s/log/install.log for the reason.\nCommand was: "
                              "%s\n"),
                            progname,
                            outputdir,
                            buf);
                        exit_nicely(2);
                    }
                }
#endif
            }
#ifndef ENABLE_LLT
            /* "make install" */
#ifdef BUILD_BY_CMAKE
    (void)snprintf_s(buf, sizeof(buf), sizeof(buf) - 1,
        SYSTEMQUOTE "cd %s && \"%s\" DESTDIR=\"%s/install\" install -j >> \"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
        current_exe_dir, makeprog, temp_install, outputdir);
    printf("cd %s && \"%s\" DESTDIR=\"%s/install\" install -j >> \"%s/log/install.log\" 2>&1\n", \
           current_exe_dir, makeprog, temp_install, outputdir);
#elif !defined(WIN32_ONLY_COMPILER)
            (void)snprintf(buf,
                sizeof(buf),
                SYSTEMQUOTE
                "\"%s\" -C \"%s\" DESTDIR=\"%s/install\" install -sj > \"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
                makeprog,
                top_builddir,
                temp_install,
                outputdir);
#else
            (void)snprintf(buf,
                sizeof(buf),
                SYSTEMQUOTE
                "perl \"%s/src/tools/msvc/install.pl\" \"%s/install\" >\"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
                top_builddir,
                temp_install,
                outputdir);
#endif
            if (system(buf)) {
                fprintf(stderr,
                    _("\n%s: installation failed\nExamine %s/log/install.log for the reason.\nCommand was: %s\n"),
                    progname,
                    outputdir,
                    buf);
                exit_nicely(2);
            } else if (only_install)
                exit_nicely(2);

            for (ssl = extra_install; ssl != NULL; ssl = ssl->next) {
#ifndef WIN32_ONLY_COMPILER
                (void)snprintf(buf,
                    sizeof(buf),
                    SYSTEMQUOTE
                    "\"%s\" -C \"%s/%s\" DESTDIR=\"%s/install\" install >> \"%s/log/install.log\" 2>&1" SYSTEMQUOTE,
                    makeprog,
                    top_builddir,
                    ssl->str,
                    temp_install,
                    outputdir);
#else
                fprintf(stderr, _("\n%s: --extra-install option not supported on this platform\n"), progname);
                exit_nicely(2);
#endif
                if (system(buf)) {
                    fprintf(stderr,
                        _("\n%s: installation failed\nExamine %s/log/install.log for the reason.\nCommand was: %s\n"),
                        progname,
                        outputdir,
                        buf);
                    exit_nicely(2);
                }
            }
#endif
            if ((!inplace_upgrade && grayscale_upgrade == -1) || init_database) {
                /* initdb */
                header(_("initializing database system"));
                init_gtm();
                /* Initialize nodes */
                if (parallel_initdb)
                    initdb_node_info_parallel(standby_defined);
                else
                    initdb_node_info(standby_defined);
            } else {
                /* copy base data and binary for upgrade */
                (void)snprintf(buf,
                    sizeof(buf),
                    SYSTEMQUOTE "rm -rf %s/data* && rm -rf %s/coordinator* &&"
                                "rm -rf %s/data_base && rm -rf %s/coordinator* &&"
                                " rm -rf %s/bin/ && rm -rf %s/etc/ && rm -rf %s/include/ &&"
                                " rm -rf %s/jdk/ && rm -rf %s/lib/ && rm -rf %s/share/ &&"
                                " tar -xpf %s/%s/data_base.tar.gz -C %s &&"
                                " tar -xpf %s/%s/bin_base.tar.gz -C %s " SYSTEMQUOTE,
                    temp_install,
                    temp_install,
                    data_base_dir,
                    data_base_dir,
                    temp_install,
                    temp_install,
                    temp_install,
                    temp_install,
                    temp_install,
                    temp_install,
                    data_base_dir,
                    platform,
                    temp_install,
                    data_base_dir,
                    platform,
                    temp_install);
                header("%s", buf);
                if (system(buf)) {
                    fprintf(stderr, _("Failed to copy base data for upgrade.\nCommand was: %s\n"), buf);
                    exit_nicely(2);
                }
                // The old binary installation is complete, switch to the old binary environment variable
                setBinAndLibPath(true);
            }

            /* If only init database for inplace upgrade, we are done. */
            if (init_database) {
                cleanup_environment();
                return 0;
            }

            /*
             * Adjust the default postgresql.conf as needed for regression
             * testing. The user can specify a file to be appended; in any case we
             * set max_prepared_transactions to enable testing of prepared xacts.
             * (Note: to reduce the probability of unexpected shmmax failures,
             * don't set max_prepared_transactions any higher than actually needed
             * by the prepared_xacts regression test.)
             * Update configuration file of each node with user-defined options
             * and 2PC related information.
             * PGXCTODO: calculate port of GTM before setting configuration files
             */
            if (!(inplace_upgrade || grayscale_upgrade != -1)) {
                initdb_node_config_file(standby_defined);
            }
        }
        /*Execute shell cmds in source files*/
        exec_cmds_from_inputfiles();

        /*
         * Start the temp postmaster: 3C12D
         */
        (void)start_postmaster();

        if (inplace_upgrade || grayscale_upgrade != -1) {
            checkProcInsert();
            setup_super_user();

            super_user_altered = true;
            // Execute the upgrade script in the old bin
            (void)snprintf_s(buf,
                             sizeof(buf),
                             sizeof(buf) - 1,
                             SYSTEMQUOTE "python %s/upgradeCheck.py -u -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                    upgrade_script_dir,
                    get_port_number(0, DATANODE),
                    upgrade_from,
                    psqldir ? psqldir : "",
                    psqldir ? "/" : "");
            header("%s", buf);
            (void)gettimeofday(&end_time, NULL);
            if (regr_system(buf)) {
                fprintf(stderr, _("Failed to exec upgrade.\nCommand was: %s\n"), buf);
                exit_nicely(2);
            }

            // start with new bin
            (void)gettimeofday(&start_time, NULL);
            header(_("shutting down postmaster"));
            (void)stop_postmaster();
            setBinAndLibPath(false);

            if (grayscale_upgrade == 1) {
                (void)start_postmaster();
                header(_("sleeping"));
                pg_usleep(10000000L);
                (void)gettimeofday(&end_time, NULL);
                total_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 0.000001;
                printf("It takes %fs to restart cluster.\n", total_time);
                /* Execute the post upgrade script */
                (void)snprintf_s(buf,
                                 sizeof(buf),
                                 sizeof(buf) - 1,
                                 SYSTEMQUOTE "python %s/upgradeCheck.py --post -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                        upgrade_script_dir,
                        get_port_number(0, DATANODE),
                        upgrade_from,
                        psqldir ? psqldir : "",
                        psqldir ? "/" : "");
                header("%s", buf);
                (void)gettimeofday(&end_time, NULL);
                if (regr_system(buf)) {
                    fprintf(stderr, _("Failed to exec post-upgrade.\nCommand was: %s\n"), buf);
                    exit_nicely(2);
                }

                /* Execute the post rollback script */
                (void)snprintf_s(buf,
                                 sizeof(buf),
                                 sizeof(buf) - 1,
                                 SYSTEMQUOTE "python %s/upgradeCheck.py --rollback -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                        upgrade_script_dir,
                        get_port_number(0, DATANODE),
                        upgrade_from,
                        psqldir ? psqldir : "",
                        psqldir ? "/" : "");
                header("%s", buf);
                (void)gettimeofday(&end_time, NULL);
                if (regr_system(buf)) {
                    fprintf(stderr, _("Failed to exec post-rollback.\nCommand was: %s\n"), buf);
                    exit_nicely(2);
                }

                // start with old bin
                restartPostmaster(true);

                /* Execute the rollback script */
                (void)snprintf_s(buf,
                                 sizeof(buf),
                                 sizeof(buf) - 1,
                                 SYSTEMQUOTE "python %s/upgradeCheck.py -o -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                        upgrade_script_dir,
                        get_port_number(0, DATANODE),
                        upgrade_from,
                        psqldir ? psqldir : "",
                        psqldir ? "/" : "");
                header("%s", buf);
                (void)gettimeofday(&end_time, NULL);
                if (regr_system(buf)) {
                    fprintf(stderr, _("Failed to exec post-rollback.\nCommand was: %s\n"), buf);
                    exit_nicely(2);
                }

                // Execute the upgrade script in the old bin
                (void)snprintf_s(buf,
                                 sizeof(buf),
                                 sizeof(buf) - 1,
                                 SYSTEMQUOTE "python %s/upgradeCheck.py -u -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                        upgrade_script_dir,
                        get_port_number(0, DATANODE),
                        upgrade_from,
                        psqldir ? psqldir : "",
                        psqldir ? "/" : "");
                header("%s", buf);
                (void)gettimeofday(&end_time, NULL);
                if (regr_system(buf)) {
                    fprintf(stderr, _("Failed to exec upgrade.\nCommand was: %s\n"), buf);
                    exit_nicely(2);
                }

                // start with new bin
                (void)gettimeofday(&start_time, NULL);
                header(_("shutting down postmaster"));
                (void)stop_postmaster();
                setBinAndLibPath(false);

            }


            initdb_node_config_file(standby_defined);
            (void)start_postmaster();
            header(_("sleeping"));
            pg_usleep(10000000L);
            (void)gettimeofday(&end_time, NULL);
            total_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 0.000001;
            printf("It takes %fs to restart cluster.\n", total_time);
            /* Execute the post upgrade script */
            (void)snprintf_s(buf,
                             sizeof(buf),
                             sizeof(buf) - 1,
                             SYSTEMQUOTE "python %s/upgradeCheck.py --post -p %d -f %d -s %s%sgsql" SYSTEMQUOTE,
                    upgrade_script_dir,
                    get_port_number(0, DATANODE),
                    upgrade_from,
                    psqldir ? psqldir : "",
                    psqldir ? "/" : "");
            header("%s", buf);
            (void)gettimeofday(&end_time, NULL);
            if (regr_system(buf)) {
                fprintf(stderr, _("Failed to exec post-upgrade.\nCommand was: %s\n"), buf);
                exit_nicely(2);
            }

            header(_("shutting down postmaster"));
            (void)stop_postmaster();
            upgrade_from = 0;
            // switch to the new binary
            setBinAndLibPath(false);
            (void)start_postmaster();
            header(_("sleeping"));
            pg_usleep(10000000L);
        }

        if (!test_single_node) {
            /* Postmaster is finally running, so set up connection information on Coordinators */
            if (myinfo.keep_data == false) {
                setup_connection_information(standby_defined);
            } else {
                pg_usleep(2000000L);
                rebuild_node_group();

                for (ssl = dblist; ssl; ssl = ssl->next)
                    drop_database_if_exists(ssl->str);
                for (ssl = extraroles; ssl; ssl = ssl->next)
                    drop_role_if_exists(ssl->str);
                _stringlist* tmpdblist = NULL;
                _stringlist* tmprolelist = NULL;
                add_stringlist_item(&tmpdblist, "cstore_vacuum_full_db");
                add_stringlist_item(&tmpdblist, "test_sort");
                add_stringlist_item(&tmpdblist, "td_db");
                add_stringlist_item(&tmpdblist, "my_db1");
                add_stringlist_item(&tmpdblist, "td_db_char");
                add_stringlist_item(&tmpdblist, "td_db_char_cast");
                add_stringlist_item(&tmpdblist, "tdtest");
                add_stringlist_item(&tmpdblist, "td_db_char_bulkload");
                add_stringlist_item(&tmpdblist, "testaes1");
                add_stringlist_item(&tmpdblist, "db_gin_utf8");
                add_stringlist_item(&tmpdblist, "music");
                add_stringlist_item(&tmpdblist, "db_ascii_bulkload_compatibility_test");
                add_stringlist_item(&tmpdblist, "db_latin1_bulkload_compatibility_test");
                add_stringlist_item(&tmpdblist, "db_gbk_bulkload_compatibility_test");
                add_stringlist_item(&tmpdblist, "db_eucjis2004_bulkload_compatibility_test");
                add_stringlist_item(&tmpdblist, "td_format_db");
                add_stringlist_item(&tmpdblist, "test_parallel_db");
                for (ssl = tmpdblist; ssl; ssl = ssl->next)
                    drop_database_if_exists(ssl->str);

                add_stringlist_item(&tmprolelist, "temp_reset_user");
                add_stringlist_item(&tmprolelist, "cstore_role");
                add_stringlist_item(&tmprolelist, "test_llt");
                add_stringlist_item(&tmprolelist, "alter_llt2");
                add_stringlist_item(&tmprolelist, "llt_1");
                add_stringlist_item(&tmprolelist, "llt_3");
                add_stringlist_item(&tmprolelist, "llt_5");
                add_stringlist_item(&tmprolelist, "role_pwd_complex");
                add_stringlist_item(&tmprolelist, "dfm");
                add_stringlist_item(&tmprolelist, "ad1");
                add_stringlist_item(&tmprolelist, "ad2");
                add_stringlist_item(&tmprolelist, "hs");
                add_stringlist_item(&tmprolelist, "testdb_new");
                for (ssl = tmprolelist; ssl; ssl = ssl->next)
                    drop_role_if_exists(ssl->str);
            }
        }
        gen_startstop_script();
    } else {
        /*
         * Using an existing installation, so may need to get rid of
         * pre-existing database(s) and role(s)
         */
        if (myinfo.run_check == true && !use_existing) {
            for (ssl = dblist; ssl; ssl = ssl->next)
                drop_database_if_exists(ssl->str);
            for (ssl = extraroles; ssl; ssl = ssl->next)
                drop_role_if_exists(ssl->str);
        }
    }

    if (myinfo.run_check == true) {
        (void)gettimeofday(&end_time_total, NULL);
        total_time = (end_time_total.tv_sec - start_time_total.tv_sec) + (end_time_total.tv_usec - start_time_total.tv_usec) * 0.000001;
        printf("Preparation before the test case execution takes %fs..\n", total_time);
        if (!use_existing) {
            for (ssl = dblist; ssl; ssl = ssl->next) {
                create_database(ssl->str);
            }
            if (to_create_jdbc_user) {
                create_jdbc_user(dblist);
            }
            for (ssl = extraroles; ssl; ssl = ssl->next) {
                create_role(ssl->str, dblist);
            }
        }

        /*
         * Ready to run the tests
         */
        header(_("running regression test queries"));

        char* old_gausshome = NULL;
        char env_path[MAXPGPATH + sizeof("GAUSSHOME=")];

        // Query retry need env 'GAUSSHOME'.
        old_gausshome = gs_getenv_r("GAUSSHOME");
        /* keep the old shell GAUSSHOME evn */
        (void)snprintf(env_path, sizeof(env_path), "OLDGAUSSHOME=%s", old_gausshome);
        char* old_gausshome_env = strdup(env_path);
        gs_putenv_r(old_gausshome_env);
        /* pg regress env */
        (void)snprintf(env_path, sizeof(env_path), "GAUSSHOME=%s/../", psqldir);
        gs_putenv_r(env_path);

        (void)gettimeofday(&start_time, NULL);

        if (!g_bEnableDiagCollection)
            dfunc = NULL;
        for (ssl = schedulelist; ssl != NULL; ssl = ssl->next) {
            run_schedule(ssl->str, tfunc, dfunc);
        }

        for (ssl = extra_tests; ssl != NULL; ssl = ssl->next) {
            run_single_test(ssl->str, tfunc, dfunc);
        }

        (void)gettimeofday(&end_time, NULL);

        // Restore the env 'GAUSSHOME'
        //
        if (old_gausshome) {
            (void)snprintf(env_path, sizeof(env_path), "GAUSSHOME=%s", old_gausshome);
            gs_putenv_r(env_path);
        }

        total_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 0.000001;

        /*
         * Shut down temp installation's postmaster
         */
        if (temp_install && clean) {
            header(_("shutting down postmaster"));
            stop_postmaster();
        }

        fclose(logfile);

        /*
         * Emit nice-looking summary message
         */
        if (fail_count == 0 && fail_ignore_count == 0)
            (void)snprintf(buf, sizeof(buf), _(" All %d tests passed. "), success_count);
        else if (fail_count == 0) /* fail_count=0, fail_ignore_count>0 */
            (void)snprintf(buf,
                sizeof(buf),
                _(" %d of %d tests passed, %d failed test(s) ignored. "),
                success_count,
                success_count + fail_ignore_count,
                fail_ignore_count);
        else if (fail_ignore_count == 0) /* fail_count>0 && fail_ignore_count=0 */
            (void)snprintf(buf, sizeof(buf), _(" %d of %d tests failed. "), fail_count, success_count + fail_count);
        else
            /* fail_count>0 && fail_ignore_count>0 */
            (void)snprintf(buf,
                sizeof(buf),
                _(" %d of %d tests failed, %d of these failures ignored. "),
                fail_count + fail_ignore_count,
                success_count + fail_count + fail_ignore_count,
                fail_ignore_count);

        (void)putchar('\n');
        for (i = strlen(buf); i > 0; i--) {
            (void)putchar('=');
        }
        printf("\n%s\n", buf);
        printf(" Total Time: %fs\n", total_time);
        for (i = strlen(buf); i > 0; i--) {
            (void)putchar('=');
        }
        (void)putchar('\n');
        (void)putchar('\n');

        if (file_size(difffilename) > 0) {
            printf(_("The differences that caused some tests to fail can be viewed in the\n"
                     "file \"%s\".  A copy of the test summary that you see\n"
                     "above is saved in the file \"%s\".\n\n"),
                difffilename,
                logfilename);
        } else {
            unlink(difffilename);
            unlink(logfilename);
        }

        if (!keep_run && fail_count != 0) {
            exit_nicely(1);
        }
    }

    if (run_qunit == true) {
        char buf[MAXPGPATH * 2];
        int r;

        /* On Windows, system() seems not to force fflush, so... */
        fflush(stdout);
        fflush(stderr);

        int ret = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf),
            "../QUnit/src/qunit  -logdir ../QUnit/test/log -cp %d -dp %d -m %s -l %s",
            init_port,
            myinfo.dn_port[0],
            qunit_module,
            qunit_level);
        securec_check_ss_c(ret, "", "");
        fprintf(stderr, _("\n%s\n"), buf);
        r = regr_system(buf);
        if (r != 0) {
            fprintf(stderr, _("\n%s: Failed to Run QUnit : exit code was %d\n"), progname, r);
            exit(2);
        }

        if (temp_install && clean) {
            header(_("shutting down postmaster"));
            stop_postmaster();
        }
    }
    cleanup_environment();
    return 0;
}

void checkProcInsert()
{
    int result = 0;
    char cmd[MAXPGPATH] = {'\0'};
#ifdef BUILD_BY_CMAKE
    (void)snprintf(
        cmd, sizeof(cmd), "grep -E \"DATA.*\\(.*insert\" %s/include/catalog/pg_proc.h | wc -l", code_base_src);
#else
    (void)snprintf(
        cmd, sizeof(cmd), "grep -E \"DATA.*\\(.*insert\" %s/../../../include/catalog/pg_proc.h | wc -l", temp_install);
#endif
    FILE* fstream = NULL;
    char buf[50];
    memset(buf, 0, sizeof(buf));
    fstream = popen(cmd, "r");
    if (NULL != fgets(buf, sizeof(buf), fstream)) {
        result = atoi(buf);
    }
    if (result != 0) {
        fprintf(stderr,
            _("\n%s: can not add buildin function in the pg_proc.h;\n"
              "please add it in the Code/src/backend/catalog/pg_builtin_proc.cpp\n"),
            progname);
        exit(2);
    }
    if (NULL != fstream) {
        pclose(fstream);
    }
}

// only for restart during grayscale upgrade
void restartPostmaster(bool isOld)
{
    struct timeval start_time;
    struct timeval end_time;
    double total_time;
    (void)gettimeofday(&start_time, NULL);
    header(_("shutting down postmaster"));
    (void)stop_postmaster();
    setBinAndLibPath(isOld);
    (void)start_postmaster();
    header(_("sleeping"));
    pg_usleep(10000000L);
    (void)gettimeofday(&end_time, NULL);
    total_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 0.000001;
    printf("It takes %fs to restart cluster.\n", total_time);
}
