/*
 * pg_controldata
 *
 * reads the data from $PGDATA/global/pg_control
 *
 * copyright (c) Oliver Elphick <olly@lfix.co.uk>, 2001;
 * licence: BSD
 *
 * src/bin/pg_controldata/pg_controldata.c
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1

#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "bin/elog.h"
#include "getopt_long.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/file/fio_device.h"
#include "tool_common.h"

#define FirstNormalTransactionId ((TransactionId)3)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)

static const char *progname;
static bool enable_dss = false;

static void usage(const char* prog_name)
{
    printf(_("%s displays control information of an openGauss database cluster.\n\n"), prog_name);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION] [DATADIR]\n"), prog_name);
    printf(_("\nOptions:\n"));
#ifndef ENABLE_LITE_MODE
    printf(_("  -I, --instance-id=INSTANCE_ID\n"));
    printf(_("                    display information of specified instance (default all)\n"));
    printf(_("      --enable-dss  enable shared storage mode\n"));
    printf(_("      --socketpath=SOCKETPATH\n"));
    printf(_("                    dss connect socket file path\n"));
#endif
    printf(_("  -V, --version     output version information, then exit\n"));
    printf(_("  -?, --help        show this help, then exit\n"));
    printf(_("\nIf no data directory (DATADIR) is specified, "
             "the environment variable PGDATA\nis used.\n"));
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to community@opengauss.org> or join opengauss community <https://opengauss.org>.\n"));
#endif
}

static const char* dbState(DBState state)
{
    switch (state) {
        case DB_STARTUP:
            return _("starting up");
        case DB_SHUTDOWNED:
            return _("shut down");
        case DB_SHUTDOWNED_IN_RECOVERY:
            return _("shut down in recovery");
        case DB_SHUTDOWNING:
            return _("shutting down");
        case DB_IN_CRASH_RECOVERY:
            return _("in crash recovery");
        case DB_IN_ARCHIVE_RECOVERY:
            return _("in archive recovery");
        case DB_IN_PRODUCTION:
            return _("in production");
        default:
            break;
    }
    return _("unrecognized status code");
}

static const char* SSClusterState(SSGlobalClusterState state) {
    switch (state) {
        case CLUSTER_IN_ONDEMAND_BUILD:
            return _("in on-demand build");
        case CLUSTER_IN_ONDEMAND_REDO:
            return _("in on-demand redo");
        case CLUSTER_NORMAL:
            return _("normal");
        default:
            break;
    }
    return _("unrecognized status code");
}

static const char* SSClusterRunMode(ClusterRunMode run_mode) {
    switch (run_mode) {
        case RUN_MODE_PRIMARY:
            return _("primary cluster");
        case RUN_MODE_STANDBY:
            return _("standby cluster");
        default:
            break;
    }
    return _("unrecognized cluster run mode");
}

static const char* wal_level_str(WalLevel wal_level)
{
    switch (wal_level) {
        case WAL_LEVEL_MINIMAL:
            return "minimal";
        case WAL_LEVEL_ARCHIVE:
            return "archive";
        case WAL_LEVEL_HOT_STANDBY:
            return "hot_standby";
        case WAL_LEVEL_LOGICAL:
            return "logical";
        default:
            break;
    }
    return _("unrecognized wal_level");
}

static void exit_safely(int returnCode)
{
    if (progname != NULL) {
        free((char*)progname);
        progname = NULL;
    }
    exit(returnCode);
}

static void display_control_page(ControlFileData ControlFile, int instance_id, bool display_all)
{
    pg_crc32c crc; /* pg_crc32c as same as pg_crc32 */
    time_t time_tmp;
    char pgctime_str[128];
    char ckpttime_str[128];
    char sysident_str[32];
    const char* strftime_fmt = "%c";
    int sret = 0;

    /* skip invalid node in shared storage mode */
    if (enable_dss && ControlFile.system_identifier == 0 && display_all)
        return;

    /* display instance id in shared storage mode */
    if (enable_dss) {
        printf(_("\npg_control data (instance id %d)\n\n"), instance_id);
    }

    /* Check the CRC. */
    /* using CRC32C since 923 */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)&ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, ControlFile.crc)) {
        printf(_("WARNING: Calculated CRC checksum does not match value stored in file.\n"
                 "Either the file is corrupt, or it has a different layout than this program\n"
                 "is expecting.  The results below are untrustworthy.\n\n"));
    }

    /*
     * This slightly-chintzy coding will work as long as the control file
     * timestamps are within the range of time_t; that should be the case in
     * all foreseeable circumstances, so we don't bother importing the
     * backend's timezone library into pg_controldata.
     *
     * Use variable for format to suppress overly-anal-retentive gcc warning
     * about %c
     */
    time_tmp = (time_t)ControlFile.time;
    strftime(pgctime_str, sizeof(pgctime_str), strftime_fmt, localtime(&time_tmp));
    time_tmp = (time_t)ControlFile.checkPointCopy.time;
    strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt, localtime(&time_tmp));

    /*
     * Format system_identifier separately to keep platform-dependent format
     * code out of the translatable message string.
     */
    sret = snprintf_s(
        sysident_str, sizeof(sysident_str), sizeof(sysident_str) - 1, UINT64_FORMAT, ControlFile.system_identifier);
    securec_check_ss_c(sret, "\0", "\0");

    printf(_("pg_control version number:            %u\n"), ControlFile.pg_control_version);
    if (ControlFile.pg_control_version % 65536 == 0 && ControlFile.pg_control_version / 65536 != 0)
        printf(_("WARNING: possible byte ordering mismatch\n"
                 "The byte ordering used to store the pg_control file might not match the one\n"
                 "used by this program.  In that case the results below would be incorrect, and\n"
                 "the openGauss installation would be incompatible with this data directory.\n"));
    printf(_("Catalog version number:               %u\n"), ControlFile.catalog_version_no);
    printf(_("Database system identifier:           %s\n"), sysident_str);
    printf(_("Database cluster state:               %s\n"), dbState(ControlFile.state));
    printf(_("pg_control last modified:             %s\n"), pgctime_str);
    printf(_("Latest checkpoint location:           %X/%X\n"),
        (uint32)(ControlFile.checkPoint >> 32),
        (uint32)ControlFile.checkPoint);
    printf(_("Prior checkpoint location:            %X/%X\n"),
        (uint32)(ControlFile.prevCheckPoint >> 32),
        (uint32)ControlFile.prevCheckPoint);
    printf(_("Latest checkpoint's REDO location:    %X/%X\n"),
        (uint32)(ControlFile.checkPointCopy.redo >> 32),
        (uint32)ControlFile.checkPointCopy.redo);
    printf(_("Latest checkpoint's TimeLineID:       %u\n"), ControlFile.checkPointCopy.ThisTimeLineID);
    printf(_("Latest checkpoint's full_page_writes: %s\n"),
        ControlFile.checkPointCopy.fullPageWrites ? _("on") : _("off"));
    printf(_("Latest checkpoint's NextXID:          " XID_FMT "\n"), ControlFile.checkPointCopy.nextXid);
    printf(_("Latest checkpoint's NextOID:          %u\n"), ControlFile.checkPointCopy.nextOid);
    printf(_("Latest checkpoint's NextMultiXactId:  " XID_FMT "\n"), ControlFile.checkPointCopy.nextMulti);
    printf(_("Latest checkpoint's NextMultiOffset:  " XID_FMT "\n"), ControlFile.checkPointCopy.nextMultiOffset);
    printf(_("Latest checkpoint's oldestXID:        " XID_FMT "\n"), ControlFile.checkPointCopy.oldestXid);
    printf(_("Latest checkpoint's oldestXID's DB:   %u\n"), ControlFile.checkPointCopy.oldestXidDB);
    printf(_("Latest checkpoint's oldestActiveXID:  " XID_FMT "\n"), ControlFile.checkPointCopy.oldestActiveXid);
    printf(_("Latest checkpoint's remove lsn:       %X/%X\n"),
        (uint32)(ControlFile.checkPointCopy.remove_seg >> 32),
        (uint32)ControlFile.checkPointCopy.remove_seg);
    printf(_("Time of latest checkpoint:            %s\n"), ckpttime_str);
    printf(_("Minimum recovery ending location:     %X/%X\n"),
        (uint32)(ControlFile.minRecoveryPoint >> 32),
        (uint32)ControlFile.minRecoveryPoint);
    printf(_("Backup start location:                %X/%X\n"),
        (uint32)(ControlFile.backupStartPoint >> 32),
        (uint32)ControlFile.backupStartPoint);
    printf(_("Backup end location:                  %X/%X\n"),
        (uint32)(ControlFile.backupEndPoint >> 32),
        (uint32)ControlFile.backupEndPoint);
    printf(_("End-of-backup record required:        %s\n"), ControlFile.backupEndRequired ? _("yes") : _("no"));
    printf(_("Current wal_level setting:            %s\n"), wal_level_str((WalLevel)ControlFile.wal_level));
    printf(_("Current max_connections setting:      %d\n"), ControlFile.MaxConnections);
    printf(_("Current max_prepared_xacts setting:   %d\n"), ControlFile.max_prepared_xacts);
    printf(_("Current max_locks_per_xact setting:   %d\n"), ControlFile.max_locks_per_xact);
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
    printf(_("Database system TimeLine:             %u\n"), ControlFile.timeline);
}

static void display_last_page(ss_reformer_ctrl_t reformerCtrl, int last_page_id)
{
    pg_crc32c crc;

    /* Check the CRC. */
    /* using CRC32C since 923 */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)&reformerCtrl, offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, reformerCtrl.crc)) {
        printf(_("WARNING: Calculated CRC checksum does not match value stored in file.\n"
                 "Either the file is corrupt, or it has a different layout than this program\n"
                 "is expecting.  The results below are untrustworthy.\n\n"));
    }
    printf(_("\nreformer data (last page id %d)\n\n"), last_page_id);
    printf(_("Reform control version number:        %u\n"), reformerCtrl.version);
    printf(_("Stable instances list:                %lu\n"), reformerCtrl.list_stable);
    printf(_("Primary instance ID:                  %d\n"), reformerCtrl.primaryInstId);
    printf(_("Recovery instance ID:                 %d\n"), reformerCtrl.recoveryInstId);
    printf(_("Cluster status:                       %s\n"), SSClusterState(reformerCtrl.clusterStatus));
    printf(_("Cluster run mode:                     %s\n"), SSClusterRunMode(reformerCtrl.clusterRunMode));
}

static void checkDssInput(const char* file, char** socketpath)
{
    if (file[0] == '+') {
        enable_dss = true;
    }

    /* set socketpath if not existed when enable dss */
    if (enable_dss && *socketpath == NULL) {
        *socketpath = getSocketpathFromEnv();
    }
}

int main(int argc, char* argv[])
{
    ControlFileData ControlFile;
    int fd = -1;
    bool display_all = true;
    char ControlFilePath[MAXPGPATH];
    char* DataDir = NULL;
    char* socketpath = NULL;
    int sret = 0;
    int seekpos;
    int option_value;
    int option_index;
    int display_id;
    int ss_nodeid = MIN_INSTANCEID;
    off_t ControlFileSize;
    char* endstr = nullptr;

    static struct option long_options[] = {{"enable-dss", no_argument, NULL, 1},
        {"socketpath", required_argument, NULL, 2},
        {NULL, 0, NULL, 0}};

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_controldata"));

    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage(progname);
            exit_safely(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            puts("pg_controldata (PostgreSQL) " PG_VERSION);
#else
            puts("pg_controldata (openGauss) " PG_VERSION);
#endif
            exit_safely(0);
        }
    }

    while ((option_value = getopt_long(argc, argv, "I:V", long_options, &option_index)) != -1) {
        switch (option_value) {
#ifndef ENABLE_LITE_MODE
            case 'I':
                ss_nodeid = strtol(optarg, &endstr, 10);
                if ((endstr != nullptr && endstr[0] != '\0') || ss_nodeid < MIN_INSTANCEID ||
                     ss_nodeid > REFORMER_CTL_INSTANCEID) {
                    fprintf(stderr, _("%s: unexpected node id specified, "
                        "the instance-id should be an integer in the range of %d - %d\n"),
                        progname, MIN_INSTANCEID, REFORMER_CTL_INSTANCEID);
                    exit_safely(1);
                }
                display_all = false;
                break;
            case 1:
                enable_dss = true;
                break;
            case 2:
                enable_dss = true;
                socketpath = strdup(optarg);
                break;
#endif
            default:
                /* getopt_long already emitted a complaint */
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                exit_safely(1);
        }
    }

    if (optind < argc) {
        DataDir = argv[optind];
    } else {
        DataDir = getenv("PGDATA");
    }
    if (DataDir == NULL) {
        fprintf(stderr, _("%s: no data directory specified\n"), progname);
        fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
        exit_safely(1);
    }
    check_env_value_c(DataDir);

    checkDssInput(DataDir, &socketpath);

    if (enable_dss) {
        if (socketpath == NULL || strlen(socketpath) == 0 || strncmp("UDS:", socketpath, 4) != 0) {
            fprintf(stderr, _("%s: socketpath must be specific correctly when enable dss, "
                "format is: '--socketpath=\"UDS:xxx\"'.\n"), progname);
            exit_safely(1);
        }

        if (DataDir[0] != '+') {
            fprintf(stderr, _("%s: the DATADIR is not correct with enable dss."), progname);
            exit_safely(1);
        }
    }

    // dss device init
    if (dss_device_init(socketpath, enable_dss) != DSS_SUCCESS) {
        fprintf(stderr, _("failed to init dss device\n"));
        exit_safely(1);
    }

    if (enable_dss) {
        // in shared storage mode, the cluster contains only one pg_control file
        sret = snprintf_s(ControlFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control", DataDir);
    } else {
        sret = snprintf_s(ControlFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control", DataDir);
    }
    securec_check_ss_c(sret, "\0", "\0");

    fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        fprintf(
            stderr, _("%s: could not open file \"%s\" for reading: %s\n"), progname, ControlFilePath, strerror(errno));
        exit_safely(2);
    }

    if ((ControlFileSize = lseek(fd, 0, SEEK_END)) < 0) {
        fprintf(stderr, _("%s: could not get \"%s\" size: %s\n"), progname, ControlFilePath, strerror(errno));
        close(fd);
        exit_safely(2);
    }

    display_id = ss_nodeid;
    seekpos = (int)BLCKSZ * ss_nodeid;
    if (seekpos >= ControlFileSize) {
        fprintf(stderr, _("%s: cound not read beyond end of file \"%s\", file_size: %ld, instance_id: %d\n"),
                progname, ControlFilePath, ControlFileSize, ss_nodeid);
        close(fd);
        exit_safely(2);
    }

    do {
        if (lseek(fd, (off_t)seekpos, SEEK_SET) < 0) {
            fprintf(stderr, _("%s: could not seek in \"%s\" to offset %d: %s\n"),
                    progname, ControlFilePath, seekpos, strerror(errno));
            close(fd);
            exit_safely(2);
        }

        if (display_id <= MAX_INSTANCEID) {
            if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData)) {
                fprintf(stderr, _("%s: could not read file \"%s\": %s\n"), progname, ControlFilePath, strerror(errno));
                close(fd);
                exit_safely(2);
            }
            display_control_page(ControlFile, display_id, display_all);
        }

        /* get the last page from the the pg_control in shared storage mode */
        if (enable_dss && display_id > MAX_INSTANCEID) {
            ss_reformer_ctrl_t reformerCtrl;
            if (read(fd, &reformerCtrl, sizeof(ss_reformer_ctrl_t)) != sizeof(ss_reformer_ctrl_t)) {
                fprintf(stderr, _("%s: could not read file \"%s\": %s\n"), progname, ControlFilePath, strerror(errno));
                close(fd);
                exit_safely(2);
            }
            display_last_page(reformerCtrl, display_id);
        }

        seekpos += BLCKSZ;
        display_id = display_id + 1;
    } while (display_all && seekpos < ControlFileSize);

    close(fd);
    if (progname != NULL) {
        free((char*)progname);
        progname = NULL;
    }
    return 0;
}
