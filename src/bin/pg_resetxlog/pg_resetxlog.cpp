/* -------------------------------------------------------------------------
 *
 * pg_resetxlog.c
 *	  A utility to "zero out" the xlog when it's corrupt beyond recovery.
 *	  Can also rebuild pg_control if needed.
 *
 * The theory of operation is fairly simple:
 *	  1. Read the existing pg_control (which will include the last
 *		 checkpoint record).  If it is an old format then update to
 *		 current format.
 *	  2. If pg_control is corrupt, attempt to intuit reasonable values,
 *		 by scanning the old xlog if necessary.
 *	  3. Modify pg_control to reflect a "shutdown" state with a checkpoint
 *		 record at the start of xlog.
 *	  4. Flush the existing xlog files and write a new segment with
 *		 just a checkpoint record in it.  The new segment is positioned
 *		 just past the end of the old xlog, so that existing LSNs in
 *		 data pages will appear to be "in the past".
 * This is all pretty straightforward except for the intuition part of
 * step 2 ...
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_resetxlog/pg_resetxlog.c
 *
 * -------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1

#include "postgres.h"
#include "knl/knl_variable.h"

#include <dirent.h>
#include <fcntl.h>
#include <locale.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "tool_common.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/multixact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "getopt_long.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/file/fio_device.h"

extern int optind;
extern char* optarg;

static ControlFileData ControlFile; /* pg_control values */
static XLogSegNo newXlogSegNo;      /* XLogSegNo of new XLOG segment */
static bool guessed = false;        /* T if we had to guess at any values */
static const char* progname;

static void DssParaInit(void);
static void SetGlobalDssParam(void);
static bool TryReadControlFile(void);
static void GuessControlValues(void);
static bool GetGucValue(const char *key, char *value);
static bool CheckConfigFileStatus(void);
static void PrintControlValues(bool guessed);
static void RewriteControlFile(void);
static void FindEndOfXLOG(void);
static void KillExistingXLOG(void);
static void KillExistingArchiveStatus(void);
static void WriteEmptyXLOG(void);
static void usage(void);
static void CheckPrimaryInDssMode(void);
static int GetPrimaryIdInDssMode(void);

#define XLOG_NAME_LENGTH 24
#define MAX_STRING_LENGTH 1024
const uint64 FREEZE_MAX_AGE = 2000000000;

/* DSS connect parameters */
static DssOptions dss;

static inline bool is_negative_num(char *str)
{
    char *s = str;
    while (*s != '\0' && isspace(*s)) {
        s++;
    }
    return *s == '-' ? true : false;
}

static void checkDssInput()
{
    if (!dss.enable_dss && dss.vgname != NULL) {
        dss.enable_dss = true;
    }

    if (dss.enable_dss && dss.socketpath == NULL) {
        dss.socketpath = getSocketpathFromEnv();
    }
}

int main(int argc, char* argv[])
{
    int c;
    bool force = false;
    bool noupdate = false;
    TransactionId set_xid = 0;
    Oid set_oid = 0;
    MultiXactId set_mxid = 0;
    MultiXactOffset set_mxoff = (MultiXactOffset)-1;
    uint32 minXlogTli = 0;
    uint32 log_temp = 0;
    uint32 seg_temp = 0;
    XLogSegNo minXlogSegNo = 0;
    char* endptr = NULL;
    char* DataDir = NULL;
    int fd = -1;
    uint64 tmpValue;
    int option_index;
    bool setMinXlogSegNo = false;

    static struct option long_options[] = {{"enable-dss", no_argument, NULL, 1},
        {"socketpath", required_argument, NULL, 2},
        {"vgname", required_argument, NULL, 3},
        {NULL, 0, NULL, 0}};

    /* init DSS parameters */
    DssParaInit();

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_resetxlog"));

    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            puts("pg_resetxlog (PostgreSQL) " PG_VERSION);
#else
            puts("pg_resetxlog " DEF_GS_VERSION);
#endif
            exit(0);
        }
    }

    while ((c = getopt_long(argc, argv, "fl:m:no:O:x:e:D:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'f':
                force = true;
                break;

            case 'n':
                noupdate = true;
                break;

            case 'e':
                tmpValue = strtoul(optarg, &endptr, 0);
                if (endptr == optarg || *endptr != '\0' || tmpValue >= PG_UINT32_MAX) {
                    fprintf(stderr, _("%s: invalid argument for option -e\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if (is_negative_num(optarg)) {
                    fprintf(stderr, _("%s: transaction ID epoch (-e) can't be negative.\n"), progname);
                    exit(1);
                }
                break;

            case 'x':
                set_xid = strtoul(optarg, &endptr, 0);
                if (endptr == optarg || *endptr != '\0') {
                    fprintf(stderr, _("%s: invalid argument for option -x\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if (is_negative_num(optarg)) {
                    fprintf(stderr, _("%s: transaction ID (-x) can't be negative.\n"), progname);
                    exit(1);
                }

                if (!TransactionIdIsNormal(set_xid)) {
                    fprintf(stderr, _("%s: transaction ID (-x) must be greater than or equal to %lu.\n"),
                        progname, FirstNormalTransactionId);
                    exit(1);
                }
                break;

            case 'o':
                tmpValue = strtoul(optarg, &endptr, 0);
                if (endptr == optarg || *endptr != '\0' || tmpValue > PG_UINT32_MAX) {
                    fprintf(stderr, _("%s: invalid argument for option -o\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if (is_negative_num(optarg)) {
                    fprintf(stderr, _("%s: OID (-o) can't be negative.\n"), progname);
                    exit(1);
                }
                set_oid = (Oid)tmpValue;
                if (set_oid == 0) {
                    fprintf(stderr, _("%s: OID (-o) must not be 0\n"), progname);
                    exit(1);
                }
                break;

            case 'm':
                set_mxid = strtoul(optarg, &endptr, 0);
                if (endptr == optarg || *endptr != '\0') {
                    fprintf(stderr, _("%s: invalid argument for option -m\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if (is_negative_num(optarg)) {
                    fprintf(stderr, _("%s: multitransaction ID (-m) can't be negative.\n"), progname);
                    exit(1);
                }
                if (set_mxid == 0) {
                    fprintf(stderr, _("%s: multitransaction ID (-m) must not be 0\n"), progname);
                    exit(1);
                }
                break;

            case 'O':
                set_mxoff = strtoul(optarg, &endptr, 0);
                if (endptr == optarg || *endptr != '\0') {
                    fprintf(stderr, _("%s: invalid argument for option -O\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if ((int32)set_mxoff == -1) {
                    fprintf(stderr, _("%s: the low 32bit of multitransaction offset (-O) must not be ffffffff\n"), progname);
                    exit(1);
                }
                break;

            case 'l':
                if (strspn(optarg, "01234567890ABCDEFabcdef") != XLOG_NAME_LENGTH) {
                    fprintf(stderr, _("%s: invalid argument for option -l\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit(1);
                }
                if (sscanf_s(optarg, "%08X%08X%08X", &minXlogTli, &log_temp, &seg_temp) != 3) {
                    fprintf(stderr, _("%s: invalid segment file"), progname);
                    exit(1);
                }
                setMinXlogSegNo = true;
                break;

#ifndef ENABLE_LITE_MODE
            case 1:
                dss.enable_dss = true;
                break;

            case 2:
                dss.socketpath = strdup(optarg);
                break;

            case 3:
                dss.vgname = strdup(optarg);
                break;
#endif

            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
        }
    }

    checkDssInput();

    /* the data directory is not necessary in dss mode */
    if (optind >= argc) {
        fprintf(stderr, _("%s: no data directory specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (dss.enable_dss) {
        if (dss.socketpath == NULL || strlen(dss.socketpath) == 0 || strncmp("UDS:", dss.socketpath, 4) != 0) {
            fprintf(stderr, _("%s: socketpath must be specific correctly when enable dss, "
                "format is: '--socketpath=\"UDS:xxx\"'.\n"), progname);
            exit(1);
        }
        if (dss.vgname == NULL) {
            fprintf(stderr, _("%s: vgname cannot be NULL when enable dss\n"), progname);
            exit(1);
        }
    } else {
        if (dss.socketpath != NULL) {
            fprintf(stderr, _("%s: socketpath cannot be set when disable dss\n"), progname);
            exit(1);
        }
        if (dss.vgname != NULL) {
            fprintf(stderr, _("%s: vgname cannot be set when disable dss\n"), progname);
            exit(1);
        }
    }

    /*
     * Don't allow pg_resetxlog to be run as root, to avoid overwriting the
     * ownership of files in the data directory. We need only check for root
     * -- any other user won't have sufficient permissions to modify files in
     * the data directory.
     */
#ifndef WIN32
    if (geteuid() == 0) {
        fprintf(stderr, _("%s: cannot be executed by \"root\"\n"), progname);
        fprintf(stderr, _("You must run %s as the openGauss system admin.\n"), progname);
        exit(1);
    }
#endif

    DataDir = argv[optind];
    if (DataDir == NULL || strlen(DataDir) == 0) {
        fprintf(stderr, _("%s: the data directory is \"<NULL>\""), progname);
        exit(1);
    }

    if (chdir(DataDir) < 0) {
        fprintf(stderr, _("%s: could not change directory to \"%s\": %s\n"), 
            progname, DataDir, strerror(errno));
        exit(1);
    }

    /* register for dssapi */
    if (dss_device_init(dss.socketpath, dss.enable_dss) != DSS_SUCCESS) {
        fprintf(stderr, _("%s: fail to init dss device\n"), progname);
        exit(1);
    }

    /* set DSS connect parameters */
    if (dss.enable_dss)
        SetGlobalDssParam();

    initDataPathStruct(dss.enable_dss);

    /*
     * Check for a postmaster lock file --- if there is one, refuse to
     * proceed, on grounds we might be interfering with a live installation.
     */
    if ((fd = open("postmaster.pid", O_RDONLY, 0)) < 0) {
        if (errno != ENOENT) {
            fprintf(stderr,
                _("%s: could not open file \"%s\" for reading: %s\n"),
                progname,
                "postmaster.pid",
                strerror(errno));
            exit(1);
        }
    } else {
        fprintf(stderr,
            _("%s: lock file \"%s\" exists\n"
              "Is a server running?  If not, delete the lock file and try again.\n"),
            progname,
            "postmaster.pid");
        close(fd);
        fd = -1;
        exit(1);
    }

    /*
     * We require cluster shutdown when executing this tool, so a better
     * solution is to execute the tool on the primary node.
     */
    CheckPrimaryInDssMode();

    /*
     * Attempt to read the existing pg_control file
     */
    if (!TryReadControlFile()) {
        GuessControlValues();
    }

    /*
     * Also look at existing segment files to set up newXlogSegNo
     */
    FindEndOfXLOG();

    /*
     * Adjust fields if required by switches.  (Do this now so that printout,
     * if any, includes these values.)
     */
    if (set_xid != 0) {
        ControlFile.checkPointCopy.nextXid = set_xid;

        /*
         * For the moment, just set oldestXid to a value that will force
         * immediate autovacuum-for-wraparound.  It's not clear whether adding
         * user control of this is useful, so let's just do something that's
         * reasonably safe.  The magic constant here corresponds to the
         * maximum allowed value of autovacuum_freeze_max_age.
         */

        if (set_xid > (FREEZE_MAX_AGE + FirstNormalTransactionId)) {
            ControlFile.checkPointCopy.oldestXid = set_xid - FREEZE_MAX_AGE;
        } else { 
            ControlFile.checkPointCopy.oldestXid = FirstNormalTransactionId;
        }
        ControlFile.checkPointCopy.oldestXidDB = InvalidOid;
    }

    if (set_oid != 0)
        ControlFile.checkPointCopy.nextOid = set_oid;

    if (set_mxid != 0)
        ControlFile.checkPointCopy.nextMulti = set_mxid;

    if ((int32)set_mxoff != -1)
        ControlFile.checkPointCopy.nextMultiOffset = set_mxoff;

    if (minXlogTli > ControlFile.checkPointCopy.ThisTimeLineID)
        ControlFile.checkPointCopy.ThisTimeLineID = minXlogTli;

    if (setMinXlogSegNo)
        minXlogSegNo = (uint64)log_temp * XLogSegmentsPerXLogId + seg_temp;

    if (minXlogSegNo > newXlogSegNo) {
        newXlogSegNo = minXlogSegNo;
    }

    /*
     * If we had to guess anything, and -f was not given, just print the
     * guessed values and exit.  Also print if -n is given.
     */
    if ((guessed && !force) || noupdate) {
        PrintControlValues(guessed);
        if (!noupdate) {
            printf(_("\nIf these values seem acceptable, use -f to force reset.\n"));
            exit(1);
        } else {
            exit(0);
        }
    }

    /*
     * Don't reset from a dirty pg_control without -f, either.
     */
    if (ControlFile.state != DB_SHUTDOWNED && !force) {
        printf(_("The database server was not shut down cleanly.\n"
                 "Resetting the transaction log might cause data to be lost.\n"
                 "If you want to proceed anyway, use -f to force reset.\n"));
        exit(1);
    }

    /*
     * Else, do the dirty deed.
     */
    RewriteControlFile();
    KillExistingXLOG();
    KillExistingArchiveStatus();
    WriteEmptyXLOG();

    printf(_("Transaction log reset\n"));
    return 0;
}

static void DssParaInit(void)
{
    dss.enable_dss = false;
    dss.socketpath = NULL;
    dss.vgname = NULL;
    dss.primaryInstId = INVALID_INSTANCEID;
}

static void SetGlobalDssParam(void)
{
    errno_t rc = strcpy_s(g_datadir.dss_data, strlen(dss.vgname) + 1, dss.vgname);
    securec_check_c(rc, "\0", "\0");
    XLogSegmentSize = DSS_XLOG_SEG_SIZE;
    /* Check dss connect */
    struct stat st;
    if (stat(g_datadir.dss_data, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, _("Could not connect dssserver, vgname: \"%s\", socketpath: \"%s\", \n"
            "please check that whether the dssserver is manually started and retry later.\n"),
            dss.vgname, dss.socketpath);
        exit(1);
    }
    rc = strcpy_s(g_datadir.dss_log, strlen(dss.vgname) + 1, dss.vgname);
    securec_check_c(rc, "\0", "\0");
}

/*
 * Try to read the existing pg_control file.
 *
 * This routine is also responsible for updating old pg_control versions
 * to the current format.  (Currently we don't do anything of the sort.)
 */
static bool TryReadControlFile(void)
{
    int fd = -1;
    int len = 0;
    char* buffer = NULL;
    pg_crc32 crc;
    errno_t rc = 0;

    if ((fd = open(T_XLOG_CONTROL_FILE, O_RDONLY | PG_BINARY, 0)) < 0) {
        /*
         * If pg_control is not there at all, or we can't read it, the odds
         * are we've been handed a bad DataDir path, so give up. User can do
         * "touch pg_control" to force us to proceed.
         */
        fprintf(stderr,
            _("%s: could not open file \"%s\" for reading: %s\n"),
            progname,
            T_XLOG_CONTROL_FILE,
            strerror(errno));
        if (!dss.enable_dss && errno == ENOENT)
            fprintf(stderr,
                _("If you are sure the data directory path is correct, execute\n"
                "  touch %s\n"
                "and try again.\n"),
                T_XLOG_CONTROL_FILE);
        exit(1);
    }

    /* Use malloc to ensure we have a maxaligned buffer */
    buffer = (char*)malloc(PG_CONTROL_SIZE);
    if (buffer == NULL) {
        fprintf(stderr, _("%s: out of memory\n"), progname);
        close(fd);
        fd = -1;
        exit(1);
    }

    len = read(fd, buffer, PG_CONTROL_SIZE);
    if (len < 0) {
        fprintf(stderr, _("%s: could not read file \"%s\": %s\n"),
                progname, T_XLOG_CONTROL_FILE, strerror(errno));
        free(buffer);
        buffer = NULL;
        close(fd);
        fd = -1;
        exit(1);
    }
    close(fd);
    fd = -1;

    if ((unsigned int)(len) >= sizeof(ControlFileData) &&
        ((ControlFileData*)buffer)->pg_control_version == PG_CONTROL_VERSION) {
        /* Check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, buffer, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);

        if (EQ_CRC32C(crc, ((ControlFileData*)buffer)->crc)) {
            /* Valid data... */
            rc = memcpy_s(&ControlFile, sizeof(ControlFile), buffer, sizeof(ControlFile));
            securec_check_c(rc, "", "");
            free(buffer);
            buffer = NULL;
            return true;
        }

        fprintf(stderr, _("%s: pg_control exists but has invalid CRC; proceed with caution\n"), progname);
        /* We will use the data anyway, but treat it as guessed. */
        rc = memcpy_s(&ControlFile, sizeof(ControlFile), buffer, sizeof(ControlFile));
        securec_check_c(rc, "", "");
        guessed = true;
        free(buffer);
        buffer = NULL;
        return true;
    }

    /* Looks like it's a mess. */
    fprintf(stderr, _("%s: pg_control exists but is broken or unknown version; ignoring it\n"), progname);
    free(buffer);
    buffer = NULL;
    return false;
}

/*
 * Guess at pg_control values when we can't read the old ones.
 */
static void GuessControlValues(void)
{
    uint64 sysidentifier;
    struct timeval tv;
    errno_t rc = 0;

    /*
     * Set up a completely default set of pg_control values.
     */
    guessed = true;
    rc = memset_s(&ControlFile, sizeof(ControlFile), 0, sizeof(ControlFile));
    securec_check_c(rc, "\0", "\0");

    ControlFile.pg_control_version = PG_CONTROL_VERSION;
    ControlFile.catalog_version_no = CATALOG_VERSION_NO;

    /*
     * Create a new unique installation identifier, since we can no longer use
     * any old XLOG records.  See notes in xlog.c about the algorithm.
     */
    gettimeofday(&tv, NULL);
    sysidentifier = ((uint64)tv.tv_sec) << 32;
    sysidentifier |= (uint32)((uint64)tv.tv_sec | (uint64)tv.tv_usec);

    ControlFile.system_identifier = sysidentifier;

    ControlFile.checkPointCopy.redo = SizeOfXLogLongPHD;
    ControlFile.checkPointCopy.ThisTimeLineID = 1;
    ControlFile.checkPointCopy.fullPageWrites = false;
    ControlFile.checkPointCopy.nextXid = FirstNormalTransactionId;
    ControlFile.checkPointCopy.nextOid = FirstBootstrapObjectId;
    ControlFile.checkPointCopy.nextMulti = FirstMultiXactId;
    ControlFile.checkPointCopy.nextMultiOffset = 0;
    ControlFile.checkPointCopy.oldestXid = FirstNormalTransactionId;
    ControlFile.checkPointCopy.oldestXidDB = InvalidOid;
    ControlFile.checkPointCopy.time = (pg_time_t)time(NULL);
    ControlFile.checkPointCopy.oldestActiveXid = InvalidTransactionId;
    ControlFile.checkPointCopy.remove_seg = InvalidXLogSegPtr;
    ControlFile.state = DB_SHUTDOWNED;
    ControlFile.time = (pg_time_t)time(NULL);
    ControlFile.checkPoint = ControlFile.checkPointCopy.redo;

    /* minRecoveryPoint, backupStartPoint and backupEndPoint can be left zero */

    ControlFile.wal_level = WAL_LEVEL_MINIMAL;
    ControlFile.MaxConnections = 100;
    ControlFile.max_prepared_xacts = 0;
    ControlFile.max_locks_per_xact = 64;

    ControlFile.maxAlign = MAXIMUM_ALIGNOF;
    ControlFile.floatFormat = FLOATFORMAT_VALUE;
    ControlFile.blcksz = BLCKSZ;
    ControlFile.relseg_size = RELSEG_SIZE;
    ControlFile.xlog_blcksz = XLOG_BLCKSZ;
    ControlFile.xlog_seg_size = XLogSegSize;
    ControlFile.nameDataLen = NAMEDATALEN;
    ControlFile.indexMaxKeys = INDEX_MAX_KEYS;
    ControlFile.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
#ifdef HAVE_INT64_TIMESTAMP
    ControlFile.enableIntTimes = true;
#else
    ControlFile.enableIntTimes = false;
#endif
    ControlFile.float4ByVal = FLOAT4PASSBYVAL;
    ControlFile.float8ByVal = FLOAT8PASSBYVAL;

    /*
     * eventually, should try to grovel through old XLOG to develop more
     * accurate values for TimeLineID, nextXID, etc.
     */
}

/*
 * Get guc the value of key from postgresql.conf.
 * e.g.
 * 1. get the last guc value line
 *    result: "port   =  ' 5432  '  #  database port"
 * 2. ensure that the last guc value is valid
 *      key = value     key = value #...
 *      key = 'value'   key = 'value' #... 
 *    result: "port   =  ' 5432  '  #  database port"
 * 3. get the string between the first '=' and the first '#'
 *    result: "  ' 5432  '  "
 * 4. cut the head and tail spaces
 *    result: "' 5432  '"
 * 5. cut the head and tail char "'"
 *    result: " 5432  "
 * 6. cut the head and tail spaces
 *    result: "5432"
 */
static bool GetGucValue(const char *key, char *value)
{
    char cmd[MAX_STRING_LENGTH];
    FILE* fp = NULL;

    if (!CheckConfigFileStatus()) {
        fprintf(stderr, _("%s: postgresql.conf does not exist!\n"), progname);
        exit(1);
    }

    /* generate the command for get guc value */
    int sret = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1,
        "grep \"^[[:space:]]*%s[[:space:]]*=\" postgresql.conf | tail -1 | "
        "grep \"^[[:space:]]*%s[[:space:]*=[[:space:]]*[^']*$\\|"
        "^[[:space:]]*%s[[:space:]]*=[[:space:]]*[^'#]*#\\|"
        "^[[:space:]]*%s[[:space:]]*=[[:space:]]*'[^']*'[[:space:]]*$\\|"
        "^[[:space:]]*%s[[:space:]]*=[[:space:]]*'[^']*'[[:space:]]*#\" | "
        "sed 's/=/\\n/' | sed 's/#/\\n/' | sed -n 2p | sed 's/^[ \\t]*//g' | "
        "sed 's/[ \\t]$//g' | sed $'s/^\\'//g' | sed $'s/\\'$//g' | "
        "sed 's/^[ \\t]*//g' | sed 's/[ \\t]*$//g'", key, key, key, key, key);
    securec_check_ss_c(sret, "", "");

    if ((fp = popen(cmd, "r")) != NULL) {
        if (fgets(value, MAX_STRING_LENGTH - 1, fp) != NULL) {
            uint value_len = strlen(value);
            value[value_len - 1] = '\0';
            pclose(fp);
            return true;
        }
    }
    pclose(fp);
    return false;
}

/* check the status of postgresql.conf */
bool CheckConfigFileStatus()
{
    struct stat statbuf;

    if (lstat("postgresql.conf", &statbuf) != 0)
        return false;
    return true;
}

/*
 * Print the guessed pg_control values when we had to guess.
 *
 * NB: this display should be just those fields that will not be
 * reset by RewriteControlFile().
 */
void CheckGuessed(bool guessed)
{
    if (guessed)
        printf(_("Guessed pg_control values:\n\n"));
    else
        printf(_("pg_control values:\n\n"));
}

static void PrintControlValues(bool guessed)
{
    char sysident_str[32];
    int nRet = 0;
    char fname[MAXFNAMELEN];
    
    CheckGuessed(guessed);
    /*
     * Format system_identifier separately to keep platform-dependent format
     * code out of the translatable message string.
     */
    nRet = snprintf_s(
        sysident_str, sizeof(sysident_str), sizeof(sysident_str) - 1, UINT64_FORMAT, ControlFile.system_identifier);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = snprintf_s(fname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X",
                      ControlFile.checkPointCopy.ThisTimeLineID,
                      (uint32)((newXlogSegNo) / XLogSegmentsPerXLogId),
                      (uint32)((newXlogSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss_c(nRet, "", "");

    printf(_("First log segment after reset:		%s\n"), fname);
    printf(_("pg_control version number:            %u\n"), ControlFile.pg_control_version);
    printf(_("Catalog version number:               %u\n"), ControlFile.catalog_version_no);
    printf(_("Database system identifier:           %s\n"), sysident_str);
    printf(_("Latest checkpoint's TimeLineID:       %u\n"), ControlFile.checkPointCopy.ThisTimeLineID);
    printf(_("Latest checkpoint's full_page_writes: %s\n"),
        ControlFile.checkPointCopy.fullPageWrites ? _("on") : _("off"));
    printf(_("Latest checkpoint's NextXID:          " XID_FMT "\n"), ControlFile.checkPointCopy.nextXid);
    printf(_("Latest checkpoint's NextOID:          %u\n"), ControlFile.checkPointCopy.nextOid);
    printf(_("Latest checkpoint's NextMultiXactId:  %lu\n"), ControlFile.checkPointCopy.nextMulti);
    printf(_("Latest checkpoint's NextMultiOffset:  %lu\n"), ControlFile.checkPointCopy.nextMultiOffset);
    printf(_("Latest checkpoint's oldestXID:        %lu\n"), ControlFile.checkPointCopy.oldestXid);
    printf(_("Latest checkpoint's oldestXID's DB:   %u\n"), ControlFile.checkPointCopy.oldestXidDB);
    printf(_("Latest checkpoint's oldestActiveXID:  %lu\n"), ControlFile.checkPointCopy.oldestActiveXid);
    printf(_("Latest checkpoint's remove lsn:          %X/%X\n"),
        (uint32)(ControlFile.checkPointCopy.remove_seg >> 32),
        (uint32)ControlFile.checkPointCopy.remove_seg);
    printf(_("Maximum data alignment:               %u\n"), ControlFile.maxAlign);
    /* we don't print floatFormat since can't say much useful about it */
    printf(_("Database block size:                  %u\n"), ControlFile.blcksz);
    printf(_("Blocks per segment of large relation: %u\n"), ControlFile.relseg_size);
    printf(_("WAL block size:                       %u\n"), ControlFile.xlog_blcksz);
    printf(_("Bytes per WAL segment:                %u\n"), ControlFile.xlog_seg_size);
    printf(_("Maximum length of identifiers:        %u\n"), ControlFile.nameDataLen);
    printf(_("Maximum columns in an index:          %u\n"), ControlFile.indexMaxKeys);
    printf(_("Maximum size of a TOAST chunk:        %u\n"), ControlFile.toast_max_chunk_size);
    printf(_("Date/time type storage:               %s\n"),
        (ControlFile.enableIntTimes ? _("64-bit integers") : _("floating-point numbers")));
    printf(
        _("Float4 argument passing:              %s\n"), (ControlFile.float4ByVal ? _("by value") : _("by reference")));
    printf(
        _("Float8 argument passing:              %s\n"), (ControlFile.float8ByVal ? _("by value") : _("by reference")));
}

/*
 * Write out the new pg_control file.
 */
static void RewriteControlFile(void)
{
    int fd = -1;
    /* need to be aligned */
    char buffer[PG_CONTROL_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    errno_t rc = 0;
    /*
     * Adjust fields as needed to force an empty XLOG starting at
     * newXlogSegNo.
     */
    XLogSegNoOffsetToRecPtr(newXlogSegNo, SizeOfXLogLongPHD, ControlFile.checkPointCopy.redo);
    ControlFile.checkPointCopy.time = (pg_time_t)time(NULL);

    ControlFile.state = DB_SHUTDOWNED;
    ControlFile.time = (pg_time_t)time(NULL);
    ControlFile.checkPoint = ControlFile.checkPointCopy.redo;
    ControlFile.prevCheckPoint = 0;
    ControlFile.minRecoveryPoint = 0;
    ControlFile.backupStartPoint = 0;
    ControlFile.backupEndPoint = 0;
    ControlFile.backupEndRequired = false;
    ControlFile.timeline = 0;

    /*
     * Force the defaults for max_* settings. The values don't really matter
     * as long as wal_level='minimal'; the postmaster will reset these fields
     * anyway at startup.
     * Now we can force the recorded xlog seg size to the right thing. */
    ControlFile.xlog_seg_size = XLogSegSize;

    /* Contents are protected with a CRC */
    INIT_CRC32C(ControlFile.crc);
    COMP_CRC32C(ControlFile.crc, (char*)&ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(ControlFile.crc);

    /*
     * We write out PG_CONTROL_SIZE bytes into pg_control, zero-padding the
     * excess over sizeof(ControlFileData).  This reduces the odds of
     * premature-EOF errors when reading pg_control.  We'll still fail when we
     * check the contents of the file, but hopefully with a more specific
     * error than "couldn't read pg_control".
     */
    if (sizeof(ControlFileData) > PG_CONTROL_SIZE) {
        fprintf(stderr,
            _("%s: internal error -- sizeof(ControlFileData) is too large ... fix PG_CONTROL_SIZE\n"),
            progname);
        exit(1);
    }

    rc = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check_c(rc, "", "");
    rc = memcpy_s(buffer, PG_CONTROL_SIZE, &ControlFile, sizeof(ControlFileData));
    securec_check_c(rc, "", "");

    if (!dss.enable_dss)
        unlink(T_XLOG_CONTROL_FILE);

    fd = open(T_XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        fprintf(stderr, _("%s: could not create pg_control file: %s\n"), progname, strerror(errno));
        exit(1);
    }

    errno = 0;
    if (write(fd, buffer, PG_CONTROL_SIZE) != PG_CONTROL_SIZE) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;
        fprintf(stderr, _("%s: could not write pg_control file: %s\n"), progname, strerror(errno));
        close(fd);
        fd = -1;
        exit(1);
    }

    if (fsync(fd) != 0) {
        fprintf(stderr, _("%s: fsync error: %s\n"), progname, strerror(errno));
        close(fd);
        fd = -1;
        exit(1);
    }

    close(fd);
    fd = -1;
}

/*
 * Scan existing XLOG files and determine the highest existing WAL address
 *
 * On entry, ControlFile.checkPointCopy.redo and ControlFile.xlog_seg_size
 * are assumed valid (note that we allow the old xlog seg size to differ
 * from what we're using).  On exit, newXlogSegNo is set to
 * suitable values for the beginning of replacement WAL (in our seg size).
 */
static void FindEndOfXLOG(void)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    int ret = 0;
    uint64 segs_per_xlogid = 0;
    uint64 xlogbytepos = 0;

    if (ControlFile.xlog_seg_size == 0) {
        fprintf(stderr, _("%s: xlog segment size(%u) is invalid "), progname, ControlFile.xlog_seg_size);
        exit(1);
    }
    /*
     * Initialize the max() computation using the last checkpoint address from
     * old pg_control.	Note that for the moment we are working with segment
     * numbering according to the old xlog seg size.
     */
    segs_per_xlogid = (UINT64CONST(0x100000000) / ControlFile.xlog_seg_size);
    newXlogSegNo = ControlFile.checkPointCopy.redo / ControlFile.xlog_seg_size;

    /*
     * Scan the pg_xlog directory to find existing WAL segment files. We
     * assume any present have been used; in most scenarios this should be
     * conservative, because of xlog.c's attempts to pre-create files.
     */
    xldir = opendir(T_SS_XLOGDIR);
    if (xldir == NULL) {
        fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"), progname, T_SS_XLOGDIR, strerror(errno));
        exit(1);
    }

    errno = 0;
    while ((xlde = readdir(xldir)) != NULL) {
        if (strlen(xlde->d_name) == 24 && strspn(xlde->d_name, "0123456789ABCDEF") == 24) {
            unsigned int tli, log, seg;
            XLogSegNo segno = 0;

            ret = sscanf_s(xlde->d_name, "%08X%08X%08X", &tli, &log, &seg);
            if (ret != 3) {
                printf("ERROR at %s : %d : The destination buffer or format is a NULL pointer or the invalid parameter "
                       "handle is invoked..\n",
                    __FILE__,
                    __LINE__);
                closedir(xldir);
                exit(1);
            }
            segno = ((uint64)log) * segs_per_xlogid + seg;

            /*
             * Note: we take the max of all files found, regardless of their
             * timelines.  Another possibility would be to ignore files of
             * timelines other than the target TLI, but this seems safer.
             * Better too large a result than too small...
             */
            if (segno > newXlogSegNo) {
                newXlogSegNo = segno;
            }
        }
        errno = 0;
    }
#ifdef WIN32

    /*
     * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
     * released version
     */
    if (GetLastError() == ERROR_NO_MORE_FILES)
        errno = 0;
#endif

    if (errno) {
        fprintf(stderr, _("%s: could not read from directory \"%s\": %s\n"), progname, T_SS_XLOGDIR, strerror(errno));
        closedir(xldir);
        exit(1);
    }
    closedir(xldir);

    /*
     * Finally, convert to new xlog seg size, and advance by one to ensure we
     * are in virgin territory.
     */
    xlogbytepos = newXlogSegNo * ControlFile.xlog_seg_size;

    if (xlogbytepos / ControlFile.xlog_seg_size != newXlogSegNo) {
        fprintf(stderr, _("%s: newXlogSegNo(%lu) * ControlFile.xlog_seg_size(%u) is overflow"), progname, newXlogSegNo, 
            ControlFile.xlog_seg_size);
        exit(1);
    }
    
    newXlogSegNo = (xlogbytepos + XLogSegSize - 1) / XLogSegSize;
    newXlogSegNo++;
}

/*
 * Remove existing XLOG files
 */
static void KillExistingXLOG(void)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    char path[MAXPGPATH] = {0};
    int nRet = 0;

    xldir = opendir(T_SS_XLOGDIR);
    if (xldir == NULL) {
        fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"), progname, T_SS_XLOGDIR, strerror(errno));
        exit(1);
    }

    errno = 0;
    while ((xlde = readdir(xldir)) != NULL) {
        if (strlen(xlde->d_name) == 24 && strspn(xlde->d_name, "0123456789ABCDEF") == 24) {
            nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", T_SS_XLOGDIR, xlde->d_name);
            securec_check_ss_c(nRet, "\0", "\0");
            if (unlink(path) < 0) {
                fprintf(stderr, _("%s: could not delete file \"%s\": %s\n"), progname, path, strerror(errno));
                closedir(xldir);
                exit(1);
            }
        }
        errno = 0;
    }
#ifdef WIN32

    /*
     * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
     * released version
     */
    if (GetLastError() == ERROR_NO_MORE_FILES)
        errno = 0;
#endif

    if (errno) {
        fprintf(stderr, _("%s: could not read from directory \"%s\": %s\n"), progname, T_SS_XLOGDIR, strerror(errno));
        closedir(xldir);
        exit(1);
    }
    closedir(xldir);
}

/*
 * Remove existing archive status files
 */
static void KillExistingArchiveStatus(void)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    char path[MAXPGPATH] = {0};
    char archStatDir[MAXPGPATH] = {0};
    int nRet = 0;

    nRet = snprintf_s(archStatDir, MAXPGPATH, MAXPGPATH - 1, "%s/archive_status", T_SS_XLOGDIR);
    securec_check_ss_c(nRet, "\0", "\0");

    xldir = opendir(archStatDir);
    if (xldir == NULL) {
        fprintf(stderr, _("%s: could not open directory \"%s\": %s\n"), progname, archStatDir, strerror(errno));
        exit(1);
    }

    errno = 0;
    while ((xlde = readdir(xldir)) != NULL) {
        if (strspn(xlde->d_name, "0123456789ABCDEF") == 24 &&
            (strcmp(xlde->d_name + 24, ".ready") == 0 || strcmp(xlde->d_name + 24, ".done") == 0)) {
            nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", archStatDir, xlde->d_name);
            securec_check_ss_c(nRet, "\0", "\0");
            if (unlink(path) < 0) {
                fprintf(stderr, _("%s: could not delete file \"%s\": %s\n"), progname, path, strerror(errno));
                closedir(xldir);
                exit(1);
            }
        }
        errno = 0;
    }
#ifdef WIN32

    /*
     * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
     * released version
     */
    if (GetLastError() == ERROR_NO_MORE_FILES)
        errno = 0;
#endif

    if (errno) {
        fprintf(stderr, _("%s: could not read from directory \"%s\": %s\n"), progname, archStatDir, strerror(errno));
        closedir(xldir);
        exit(1);
    }
    closedir(xldir);
}

/*
 * Write an empty XLOG file, containing only the checkpoint record
 * already set up in ControlFile.
 */
static void WriteEmptyXLOG(void)
{
    XLogPageHeader page;
    XLogLongPageHeader longpage;
    XLogRecord* record = NULL;
    pg_crc32 crc;
    char path[MAXPGPATH] = {0};
    int fd = -1;
    uint64 nbytes = 0;
    char* recptr = NULL;
    errno_t rc = EOK;
    /* need to be aligned */
    char buffer[PG_CONTROL_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER)));

    page = (XLogPageHeader)buffer;
    rc = memset_s(buffer, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
    securec_check_c(rc, "\0", "\0");
    /* Set up the XLOG page header */
    page->xlp_magic = XLOG_PAGE_MAGIC;
    page->xlp_info = XLP_LONG_HEADER;
    page->xlp_tli = ControlFile.checkPointCopy.ThisTimeLineID;
    page->xlp_pageaddr = ControlFile.checkPointCopy.redo - SizeOfXLogLongPHD;
    longpage = (XLogLongPageHeader)page;
    longpage->xlp_sysid = ControlFile.system_identifier;
    longpage->xlp_seg_size = XLogSegSize;
    longpage->xlp_xlog_blcksz = XLOG_BLCKSZ;

    /* Insert the initial checkpoint record */
    recptr = (char*)page + SizeOfXLogLongPHD;
    record = (XLogRecord*)recptr;
    record->xl_prev = 0;
    record->xl_xid = InvalidTransactionId;
    record->xl_tot_len = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint);
    record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    record->xl_rmid = RM_XLOG_ID;
    recptr += SizeOfXLogRecord;
    *(recptr++) = XLR_BLOCK_ID_DATA_SHORT;
    *(recptr++) = sizeof(CheckPoint);
    rc = memcpy_s(recptr, sizeof(CheckPoint), &ControlFile.checkPointCopy, sizeof(CheckPoint));
    securec_check_c(rc, "", "");

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char*)record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    COMP_CRC32C(crc, (char*)record, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);
    record->xl_crc = crc;

    /* Write the first page */
    rc = snprintf_s(path,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%08X%08X%08X",
        T_SS_XLOGDIR,
        ControlFile.checkPointCopy.ThisTimeLineID,
        (uint32)((newXlogSegNo) / XLogSegmentsPerXLogId),
        (uint32)((newXlogSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss_c(rc, "", "");

    unlink(path);

    fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        fprintf(stderr, _("%s: could not open file \"%s\": %s\n"), progname, path, strerror(errno));
        exit(1);
    }

    errno = 0;
    if (write(fd, buffer, XLOG_BLCKSZ) != XLOG_BLCKSZ) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;
        fprintf(stderr, _("%s: could not write file \"%s\": %s\n"), progname, path, strerror(errno));
        close(fd);
        fd = -1;
        exit(1);
    }

    if (dss.enable_dss) {
        /* extend file and fill space at once to avoid performance issue */
        errno = 0;
        if (ftruncate(fd, XLogSegSize) != 0) {
            int save_errno = errno;
            /* if write didn't set errno, assume problem is no disk space */
            errno = save_errno ? save_errno : ENOSPC;
            fprintf(stderr, _("%s: could not write file \"%s\": %s\n"), progname, path, strerror(errno));
            close(fd);
            fd = -1;
            exit(1);
        }
    } else {
        /* Fill the rest of the file with zeroes */
        rc = memset_s(buffer, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
        securec_check_c(rc, "", "");
        for (nbytes = XLOG_BLCKSZ; nbytes < XLogSegSize; nbytes += XLOG_BLCKSZ) {
            errno = 0;
            if (write(fd, buffer, XLOG_BLCKSZ) != XLOG_BLCKSZ) {
                if (errno == 0)
                    errno = ENOSPC;
                fprintf(stderr, _("%s: could not write file \"%s\": %s\n"), progname, path, strerror(errno));
                close(fd);
                fd = -1;
                exit(1);
            }
        }
    }

    if (fsync(fd) != 0) {
        fprintf(stderr, _("%s: fsync error: %s\n"), progname, strerror(errno));
        close(fd);
        fd = -1;
        exit(1);
    }

    close(fd);
    fd = -1;
}

static void CheckPrimaryInDssMode(void)
{
    if (!dss.enable_dss) {
        return;
    }

    char guc_value[MAX_STRING_LENGTH] = {0};
    int curr_inst_id;

    if (!GetGucValue("ss_instance_id", guc_value)) {
        fprintf(stderr,
            _("%s: get the guc value of \"ss_instance_id\" failed, "
            "please check the file \"postgresql.conf\".\n"), progname);
        exit(1);
    }

    curr_inst_id = atoi(guc_value);
    if (curr_inst_id < MIN_INSTANCEID || curr_inst_id > MAX_INSTANCEID) {
        fprintf(stderr, _("%s: unexpected node id specified, valid range is %d - %d\n"),
            progname, MIN_INSTANCEID, MAX_INSTANCEID);
        exit(1);
    }

    dss.primaryInstId = GetPrimaryIdInDssMode();
    if (curr_inst_id != dss.primaryInstId) {
        fprintf(stderr, _("%s: you must execute this command in primary node: %d, current node id is %d.\n"),
            progname, dss.primaryInstId, curr_inst_id);
        exit(1);
    }
}

static int GetPrimaryIdInDssMode(void)
{
    char* buffer = NULL;
    int fd = -1;
    int len = 0;
    int primaryId = -1;

    if ((fd = open(T_XLOG_CONTROL_FILE, O_RDONLY | PG_BINARY, 0)) < 0) {
        /*
         * If pg_control is not there at all, or we can't read it, the odds
         * are we've been handed a bad DataDir path, so give up. User can do
         * "touch pg_control" to force us to proceed.
         */
        fprintf(stderr,
            _("%s: could not open file \"%s\" for reading: %s\n"),
            progname,
            T_XLOG_CONTROL_FILE,
            strerror(errno));
        exit(1);
    }

    /* Use malloc to ensure we have a maxaligned buffer */
    buffer = (char*)malloc(PG_CONTROL_SIZE);
    if (buffer == NULL) {
        fprintf(stderr, _("%s: out of memory\n"), progname);
        close(fd);
        fd = -1;
        exit(1);
    }

    len = pread(fd, buffer, PG_CONTROL_SIZE, REFORMER_CTL_INSTANCEID * PG_CONTROL_SIZE);
    if (len < 0) {
        fprintf(stderr, _("%s: could not read file \"%s\": %s\n"),
                progname, T_XLOG_CONTROL_FILE, strerror(errno));
        free(buffer);
        buffer = NULL;
        close(fd);
        fd = -1;
        exit(1);
    }

    ss_reformer_ctrl_t *reformerCtrl = (ss_reformer_ctrl_t *)buffer;
    primaryId = reformerCtrl->primaryInstId;
    if (primaryId < MIN_INSTANCEID || primaryId > MAX_INSTANCEID) {
        fprintf(stderr, _("%s: unexpected primary id %d in reform control, valid range is %d - %d\n"),
            progname, primaryId, MIN_INSTANCEID, MAX_INSTANCEID);
        exit(1);
    }

    free(buffer);
    buffer = NULL;
    close(fd);
    fd = -1;
    return primaryId;
}

static void usage(void)
{
    printf(_("%s resets the openGauss transaction log.\n\n"), progname);
    printf(_("Usage:\n  %s [OPTION]... DATADIR\n\n"), progname);
    printf(_("Options:\n"));
    printf(_("  -e XIDEPOCH      set next transaction ID epoch\n"));
    printf(_("  -f               force update to be done\n"));
    printf(_("  -l xlogfile      force minimum WAL starting location for new transaction log\n"));
    printf(_("  -m XID           set next multitransaction ID\n"));
    printf(_("  -n               no update, just show extracted control values (for testing)\n"));
    printf(_("  -o OID           set next OID\n"));
    printf(_("  -O OFFSET        set next multitransaction offset\n"));
    printf(_("  -V, --version    output version information, then exit\n"));
    printf(_("  -x XID           set next transaction ID\n"));
    printf(_("  -?, --help       show this help, then exit\n"));
#ifndef ENABLE_LITE_MODE
    printf(_("  --vgname\n"));
    printf(_("                   the dss data on dss mode\n"));
    printf(_("  --enable-dss     enable shared storage mode\n"));
    printf(_("  --socketpath=SOCKETPATH\n"));
    printf(_("                   dss connect socket file path\n"));
#endif
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf("\nReport bugs to GaussDB support.\n");
#else
    printf("\nReport bugs to openGauss community by raising an issue.\n");
#endif
}

