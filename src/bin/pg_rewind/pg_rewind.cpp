/* -------------------------------------------------------------------------
 *
 * pg_rewind.c
 *	  Synchronizes an old master server to a new timeline
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#define FRONTEND 1
#include "streamutil.h"
#include "postgres_fe.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/wait.h>
#include "backup.h"
#include "pg_rewind.h"
#include "fetch.h"
#include "file_ops.h"
#include "logging.h"

#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "replication/slot.h"
#include "storage/buf/bufpage.h"
#include "utils/pg_crc.h"
#include "common/build_query/build_query.h"
#include "bin/elog.h"
#include "pg_build.h"

#define FORMATTED_TS_LEN 128
#define BUILD_PID "gs_build.pid"

static BuildErrorCode createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli, XLogRecPtr checkpointloc);
static void digestControlFile(ControlFileData* ControlFile, const char* source);
static BuildErrorCode updateControlFile(ControlFileData* ControlFile);
static BuildErrorCode sanityChecks(void);
static void rewind_dw_file();
static BuildErrorCode MoveOldXlogFiles(uint32 checkSeg, const char* newPath);

static ControlFileData ControlFile_target;
static ControlFileData ControlFile_source;

/* Configuration options */
char* datadir_target = NULL;
char* connstr_source = NULL;
uint32 term = 0;
bool debug = false;
bool dry_run = false;
int replication_type = RT_WITH_DUMMY_STANDBY;
bool ws_replication = false;
char divergeXlogFileName[MAXFNAMELEN] = {0};

BuildErrorCode increment_return_code = BUILD_SUCCESS;

BuildErrorCode gs_increment_build(const char* pgdata, const char* connstr, char* sysidentifier, uint32 timeline, uint32 term)
{
    TimeLineID lastcommontli;
    XLogRecPtr chkptrec = InvalidXLogRecPtr;
    TimeLineID chkpttli;
    XLogRecPtr chkptredo = InvalidXLogRecPtr;
    uint32 checkSeg;
    size_t size = 0;
    char* buffer = NULL;
    XLogRecPtr startrec;
    XLogRecPtr endrec;
    ControlFileData ControlFile_new;
    int fd = -1;
    FILE* file = NULL;
    char start_file[MAXPGPATH] = {0};
    char done_file[MAXPGPATH] = {0};
    char bkup_file[MAXPGPATH] = {0};
    char bkup_filemap[MAXPGPATH] = {0};
    char xlog_start[MAXFNAMELEN] = {0};
    char xlog_end[MAXFNAMELEN] = {0};
    char xlog_location[MAXPGPATH] = {0};
    char movedXlogPath[MAXPGPATH] = {0};
    int nRet = 0;
    errno_t errorno = EOK;
    GaussState state;
    BuildErrorCode rv = BUILD_SUCCESS;

    datadir_target = pg_strdup(pgdata);
    /* here we set basedir for last xlog requests purpose. the value is the datadir */
    basedir = datadir_target;
    if (connstr_source == NULL) {
        connstr_source = pg_strdup(connstr);
    }

    if (connstr_source == NULL) {
        pg_log(PG_WARNING, "%s: no source specified (--source-server)\n", progname);
        pg_log(PG_WARNING, "Try \"%s --help\" for more information.\n", progname);
        return BUILD_ERROR;
    }

    if (datadir_target == NULL) {
        pg_log(PG_WARNING, "%s: no target data directory specified (--target-pgdata)\n", progname);
        pg_log(PG_WARNING, "Try \"%s --help\" for more information.\n", progname);
        return BUILD_ERROR;
    }

    if (term > PG_UINT32_MAX) {
        pg_log(PG_PROGRESS, "%s: unexpected term specified\n", progname);
        pg_log(PG_PROGRESS, "Try \"%s --help\" for more information.\n", progname);
        return BUILD_ERROR;
    }

    /*
     * Don't allow pg_rewind to be run as root, to avoid overwriting the
     * ownership of files in the data directory. We need only check for root
     * -- any other user won't have sufficient permissions to modify files in
     * the data directory.
     */
    if (geteuid() == 0) {
        pg_log(PG_PROGRESS, "cannot be executed by \"root\"\n");
        pg_log(PG_PROGRESS, "You must run %s as the PostgreSQL superuser.\n", progname);
        exit(1);
    }

    /*
     * There are so many status file created during pg_rewind.
     */
    /* stat file 1: build_complete.start */
    nRet = snprintf_s(start_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, TAG_START);
    securec_check_ss_c(nRet, "\0", "\0");

    /* Check if last build completed. */
    if (is_file_exist(start_file)) {
        pg_log(PG_FATAL, "last build uncompleted, change to full build. \n");
        return BUILD_FATAL;
    }

    /* Can't start new building until restore process success. */
    if (is_in_restore_process(datadir_target)) {
        pg_log(PG_PROGRESS,
            "%s: last restore process hasn't completed, "
            "can't start new building.\n",
            progname);
        return BUILD_ERROR;
    }

    /* stat file 2: build_complete.done */
    nRet = snprintf_s(done_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, TAG_DONE);
    securec_check_ss_c(nRet, "\0", "\0");

    /* stat file 3: pg_rewind_bak dir */
    nRet = snprintf_s(bkup_file, MAXPGPATH, MAXPGPATH - 1, "%s/pg_rewind_bak", datadir_target);
    securec_check_ss_c(nRet, "", "");

    /* stat file 4: backup_filemap */
    nRet = snprintf_s(bkup_filemap, MAXPGPATH, MAXPGPATH - 1, "%s/pg_rewind_filemap", datadir_target);
    securec_check_ss_c(nRet, "", "");

    /* stat file 5: moved old xlog dir */
    nRet = snprintf_s(movedXlogPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", bkup_file);
    securec_check_ss_c(nRet, "", "");

    errorno = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check_c(errorno, "\0", "\0");
    state.mode = STANDBY_MODE;
    state.conn_num = 2;
    state.state = BUILDING_STATE;
    state.sync_stat = false;
    state.build_info.build_mode = INC_BUILD;
    UpdateDBStateFile(gaussdb_state_file, &state);
    pg_log(PG_PROGRESS,
        "set gaussdb state file when %s build:"
        "db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(INC_BUILD).\n",
        (build_mode == CROSS_CLUSTER_INC_BUILD? "cross_cluster_incremental" : "incremental"));

    /* Connect to remote server */
    rv = libpqConnect(connstr_source);
    PG_CHECKRETURN_AND_RETURN(rv);
    rv = libpqGetParameters();
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "connect to primary success\n");

    if ((replication_type == RT_WITH_DUMMY_STANDBY) && (checkDummyStandbyConnection() == false)) {
        pg_log(PG_PROGRESS,
               "The source DN primary can't connect to dummy standby. Please repairing the dummy standby first.\n");
        libpqDisconnect();
        pg_free(sysidentifier);
        sysidentifier = NULL;
        exit(1);
    }

    /*
     * Ok, we have all the options and we're ready to start. Read in all the
     * information we need from both clusters.
     */
    buffer = slurpFile(datadir_target, "global/pg_control", &size);
    PG_CHECKBUILD_AND_RETURN();
    digestControlFile(&ControlFile_target, (const char*)buffer);
    pg_free(buffer);
    buffer = NULL;
    PG_CHECKBUILD_AND_RETURN();

    buffer = fetchFile("global/pg_control", &size);
    PG_CHECKBUILD_AND_RETURN();
    digestControlFile(&ControlFile_source, buffer);
    pg_free(buffer);
    buffer = NULL;
    PG_CHECKBUILD_AND_RETURN();
    pg_log(PG_PROGRESS, "get pg_control success\n");

    /* Check if rewind can be performed */
    rv = sanityChecks();
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "sanityChecks success\n");

    lastcommontli = ControlFile_target.checkPointCopy.ThisTimeLineID;

    pg_log(PG_PROGRESS,
        "find last checkpoint at %X/%X and checkpoint redo at %X/%X from source control file\n",
        (uint32)(ControlFile_source.checkPoint >> 32),
        (uint32)(ControlFile_source.checkPoint),
        (uint32)(ControlFile_source.checkPointCopy.redo >> 32),
        (uint32)(ControlFile_source.checkPointCopy.redo));

    pg_log(PG_PROGRESS,
        "find last checkpoint at %X/%X and checkpoint redo at %X/%X from target control file\n",
        (uint32)(ControlFile_target.checkPoint >> 32),
        (uint32)(ControlFile_target.checkPoint),
        (uint32)(ControlFile_target.checkPointCopy.redo >> 32),
        (uint32)(ControlFile_target.checkPointCopy.redo));

    /* Find the common checkpoint locaiton */
    startrec = ControlFile_source.checkPoint <= ControlFile_target.checkPoint ?
        ControlFile_source.checkPoint : ControlFile_target.checkPoint;
    rv = findCommonCheckpoint(datadir_target, lastcommontli, startrec, &chkptrec, &chkpttli, &chkptredo, term);
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "find diverge point success\n");

    /* Checkpoint redo should exist. Otherwise, fatal and change to full build. */
    (void)readOneRecord(datadir_target, chkptredo, chkpttli);
    pg_log(PG_PROGRESS,
        "read checkpoint redo (%X/%X) success before rewinding.\n",
        (uint32)(chkptredo >> 32),
        (uint32)chkptredo);

    pg_log(PG_PROGRESS,
        "rewinding from checkpoint redo point at %X/%X on timeline %u\n",
        (uint32)(chkptredo >> 32),
        (uint32)chkptredo,
        chkpttli);

    XLByteToSeg(chkptredo, checkSeg);
    XLogFileName(divergeXlogFileName, MAXFNAMELEN, chkpttli, checkSeg);
    pg_log(PG_PROGRESS, "diverge xlogfile is %s, older ones will not be copied or removed.\n", divergeXlogFileName);

    if (libpqRotateCbmFile(conn, chkptredo) != true) {
        pg_log(PG_ERROR, "error when libpqRotateCbmFile, inc build failed");
        return BUILD_ERROR;
    }

    /*
     * Build the filemap, by comparing the source and target data directories.
     */
    filemapInit();
    rv = targetFileStatThread();
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "reading source file list\n");
    rv = fetchSourceFileList();
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "reading target file list\n");
    rv = targetFilemapProcess();
    PG_CHECKRETURN_AND_RETURN(rv);

    pg_log(PG_PROGRESS, "traverse target datadir success\n");

    /*
     * Read the target WAL from last checkpoint before the point of fork, to
     * extract all the pages that were modified on the target cluster after
     * the fork. We can stop reading after reaching the final shutdown record.
     * XXX: If we supported rewinding a server that was not shut down cleanly,
     * we would need to replay until the end of WAL here.
     */
    pg_log(PG_PROGRESS, "reading WAL in target\n");
    extractPageMap(datadir_target, chkptredo, lastcommontli);
    PG_CHECKBUILD_AND_RETURN();
    filemap_finalize();
    calculate_totals();
    pg_log(PG_PROGRESS, "calculate totals rewind success\n");

    /* this is too verbose even for verbose mode */
    if (debug)
        print_filemap();

    /*
     * Ok, we're ready to start copying things over.
     */
    fetch_size = filemap->fetch_size;
    fetch_done = 0;

    pg_log(PG_PROGRESS,
        "need to copy %luMB (total source directory size is %luMB)\n",
        (unsigned long)(filemap->fetch_size / (1024 * 1024)),
        (unsigned long)(filemap->total_size / (1024 * 1024)));

    /* Clean old backup dir if exists */
    if (access(bkup_file, F_OK) == 0) {
        delete_all_file(bkup_file, true);
        PG_CHECKBUILD_AND_RETURN();
    }
    if (replication_type == RT_WITH_DUMMY_STANDBY) {
        /* Backup local data into pg_rewind_bak dir */
        rv = backupFileMap(filemap);
        PG_CHECKRETURN_AND_RETURN(rv);
        pg_log(PG_PROGRESS, "backup target files success\n");

        MoveOldXlogFiles(checkSeg, movedXlogPath);
        PG_CHECKBUILD_AND_RETURN();
    }
    pg_log(PG_WARNING, _("starting background WAL receiver\n"));
    nRet = snprintf_s(xlog_start, MAXFNAMELEN, MAXFNAMELEN - 1, "%X/%X", (uint32)(chkptredo >> 32), (uint32)chkptredo);
    securec_check_ss_c(nRet, "", "");
    get_xlog_location(xlog_location);
    pg_log(PG_PROGRESS, "Starting copy xlog, start point: %s\n", xlog_start);
    bool startSuccess = StartLogStreamer(xlog_start, timeline, sysidentifier, (const char*)xlog_location, term);
    if (!startSuccess) {
        pg_fatal("start log streamer failed: %s\n", strerror(errno));
        return BUILD_FATAL;
    }
    
    /* Create build_complete.start file first */
    canonicalize_path(start_file);
    if ((fd = open(start_file, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) {
        pg_fatal("could not create file \"%s\": %s\n", TAG_START, strerror(errno));
        return BUILD_FATAL;
    }
    close(fd);
    fd = -1;

    /* Create pg_rewind_filemap, print filemap and other rewind info for debug purpose */
    canonicalize_path(bkup_filemap);
    if ((file = fopen(bkup_filemap, "w")) == NULL) {
        pg_fatal("could not create file \"%s\": %s\n", "pg_rewind_filemap", strerror(errno));
        return BUILD_FATAL;
    }
    print_filemap_to_file(file);

    /*
     * This is the point of no return. Once we start copying things, we have
     * modified the target directory and there is no turning back!
     */
    executeFileMap(filemap, file);
    fclose(file);
    file = NULL;
    PG_CHECKBUILD_AND_RETURN();
    pg_log(PG_PROGRESS, "execute file map success\n");

    progress_report(true);

    /*
     * Update control file of target. Make it ready to perform archive
     * recovery when restarting.
     *
     * minRecoveryPoint is set to the current WAL insert location in the
     * source server. Like in an online backup, it's important that we recover
     * all the WAL that was generated while we copied the files over.
     * 
     * But for primary-standby-dummystandby deployment, max lsn of local xlogs
     * is set so as to prevent from unavailable circumstances when primary is
     * down at this moment.
     */
    errorno = memcpy_s(&ControlFile_new, sizeof(ControlFileData), &ControlFile_source, sizeof(ControlFileData));
    securec_check_c(errorno, "\0", "\0");

    if (connstr_source != NULL) {
        endrec = libpqGetCurrentXlogInsertLocation();
        PG_CHECKBUILD_AND_RETURN();
        pg_log(PG_PROGRESS, "find minRecoveryPoint success from xlog insert location %X/%X\n",
            (uint32) (endrec >> 32), (uint32) endrec);
    } else {
        endrec = ControlFile_source.checkPoint;
        pg_log(PG_PROGRESS, "find minRecoveryPoint success from checkpoint location %X/%X\n",
            (uint32) (endrec >> 32), (uint32) endrec);
    }

    ControlFile_new.minRecoveryPoint = endrec;
    ControlFile_new.state = DB_IN_ARCHIVE_RECOVERY;
    rv = updateControlFile(&ControlFile_new);
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "update pg_control file success, minRecoveryPoint: %X/%X, "
        "ckpLoc:%X/%X, ckpRedo:%X/%X, preCkp:%X/%X\n", (uint32) (endrec >> 32), (uint32) endrec,
        (uint32) (ControlFile_new.checkPoint >> 32), (uint32) ControlFile_new.checkPoint,
        (uint32) (ControlFile_new.checkPointCopy.redo >> 32), (uint32) ControlFile_new.checkPointCopy.redo,
        (uint32) (ControlFile_new.prevCheckPoint >> 32), (uint32) ControlFile_new.prevCheckPoint);

    /* Update pg_dw file */
    rewind_dw_file();
    pg_log(PG_PROGRESS, "update pg_dw file success\n");

    /* Disconnect from remote server */
    if (connstr_source != NULL) {
        libpqDisconnect();
    }

    nRet = snprintf_s(xlog_end, MAXFNAMELEN, MAXFNAMELEN - 1, "%X/%X", (uint32)(endrec >> 32), (uint32)endrec);
    securec_check_ss_c(nRet, "", "");

    /*
     * Run IDENTIFY_MAXLSN to get end lsn
     * This lsn is latest since last full backup
     */
    pg_log(PG_WARNING, "xlog end point: %s\n", xlog_end);
    if (bgchild > 0) {
        int status;
        int r;

        pg_log(PG_WARNING, _("waiting for background process to finish streaming...\n"));
        if ((unsigned int)write(bgpipe[1], xlog_end, strlen(xlog_end)) != strlen(xlog_end)) {
            pg_log(PG_WARNING, _("could not send command to background pipe: %s\n"), strerror(errno));
            (void)kill(bgchild, SIGTERM);
            return BUILD_FATAL;
        }

        /* Just wait for the background process to exit */
        r = waitpid(bgchild, &status, 0);
        if (r == -1) {
            pg_log(PG_WARNING, _("could not wait for child process: %s\n"), strerror(errno));
            (void)kill(bgchild, SIGTERM);
            return BUILD_FATAL;
        }
        if (r != bgchild) {
            pg_log(PG_WARNING, _("child %d died, expected %d\n"), r, (int)bgchild);
            (void)kill(bgchild, SIGTERM);
            return BUILD_FATAL;
        }
        if (!WIFEXITED(status)) {
            pg_log(PG_WARNING, _("child process did not exit normally\n"));
            (void)kill(bgchild, SIGTERM);
            return BUILD_FATAL;
        }
        if (WEXITSTATUS(status) != 0) {
            pg_log(PG_WARNING, _("child process exited with error %d\n"), WEXITSTATUS(status));
            (void)kill(bgchild, SIGTERM);
            return BUILD_FATAL;
        }
        /* Exited normally, we're happy! */
    }

    /* Create backup lable file */
    pg_log(PG_PROGRESS, "creating backup label and updating control file\n");
    rv = createBackupLabel(chkptredo, chkpttli, chkptrec);
    PG_CHECKRETURN_AND_RETURN(rv);
    pg_log(PG_PROGRESS, "create backup label success\n");

    /* Check if recoreds at chkptredo and chkptrec are valid. */
    (void)readOneRecord(datadir_target, chkptredo, chkpttli);
    PG_CHECKBUILD_AND_RETURN();
    pg_log(PG_PROGRESS, "read checkpoint redo (%X/%X) success.\n", (uint32)(chkptredo >> 32), (uint32)chkptredo);

    (void)readOneRecord(datadir_target, chkptrec, chkpttli);
    PG_CHECKBUILD_AND_RETURN();
    pg_log(PG_PROGRESS, "read checkpoint rec (%X/%X) success.\n", (uint32)(chkptrec >> 32), (uint32)chkptrec);

    /* Rename build_complete.start file to build_complete.done file */
    if (rename(start_file, done_file) < 0) {
        pg_fatal("failed to rename \"%s\" to \"%s\": %s\n", TAG_START, TAG_DONE, strerror(errno));
        return BUILD_FATAL;
    }

    /* Remove pg_rewind_bak dir */
    if (access(bkup_file, F_OK) == 0) {
        delete_all_file(bkup_file, true);
        PG_CHECKBUILD_AND_RETURN();
    }

    if (datadir_target != NULL) {
        free(datadir_target);
        datadir_target = NULL;
    }

    pg_log(PG_PROGRESS, "dn %s build completed.\n", 
        (build_mode == CROSS_CLUSTER_INC_BUILD? "cross_cluster_incremental" : "incremental"));
    return BUILD_SUCCESS;
}

static BuildErrorCode sanityChecks(void)
{
    char labelfile[] = "backup_label";
    char path[MAXPGPATH] = {0};
    int fd = -1;
    int ret = 0;

    /* check system_id match */
    if (ControlFile_target.system_identifier != ControlFile_source.system_identifier) {
        pg_fatal("source and target clusters are from different systems\n");
        return BUILD_FATAL;
    }

    /* check version */
    if (ControlFile_target.pg_control_version != PG_CONTROL_VERSION ||
        ControlFile_source.pg_control_version != PG_CONTROL_VERSION ||
        ControlFile_target.catalog_version_no != CATALOG_VERSION_NO ||
        ControlFile_source.catalog_version_no != CATALOG_VERSION_NO) {
        pg_fatal("clusters are not compatible with this version of gs_rewind\n");
        return BUILD_FATAL;
    }

    /*
     * If both clusters are not on the same timeline, there's nothing to
     * do.
     */
    if (ControlFile_target.checkPointCopy.ThisTimeLineID != ControlFile_source.checkPointCopy.ThisTimeLineID) {
        pg_fatal("source and target cluster should be on the same timeline.\n");
        return BUILD_FATAL;
    }

    /*
     * Target cluster better not be running. This doesn't guard against
     * someone starting the cluster concurrently. Also, this is probably more
     * strict than necessary; it's OK if the master was not shut down cleanly,
     * as long as it isn't running at the moment.
     * Notes: Before we call the gs_rewind, gs_ctl will ensure that this process
     * has exit.
     */
    pg_log(PG_WARNING, "target server was interrupted in mode %d.\n", ControlFile_target.state);

    /* check backup_label */
    ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, labelfile);
    securec_check_ss_c(ret, "\0", "\0");

    if ((fd = open(path, O_RDONLY | PG_BINARY, 0)) >= 0) {
        close(fd);
        fd = -1;

        int ss_c = 0;
        char tmpfilename[MAXPGPATH];

        ss_c = snprintf_s(tmpfilename, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, BUILD_PID);
        securec_check_ss_c(ss_c, "", "");

        /* remove the gs_build.pid, tell cm don't send build command. */
        if (is_file_exist(tmpfilename) && unlink(tmpfilename) < 0) {
            pg_log(PG_WARNING, "failed to remove \"%s\"\n", BUILD_PID);
        }
        pg_fatal("the cluster needs to recover from the latest backup first.\n");
        return BUILD_FATAL;
    }
    return BUILD_SUCCESS;
}

/*
 * Create a backup_label file that forces recovery to begin at the last common
 * checkpoint.
 */
static BuildErrorCode createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli, XLogRecPtr checkpointloc)
{
    XLogSegNo startpointsegno;
    time_t stamp_time;
    char strfbuf[128];
    char xlogfilename[MAXFNAMELEN];
    struct tm* tmp = NULL;
    char buf[1000];
    int len;
    errno_t errorno = EOK;

    XLByteToSeg(startpoint, startpointsegno);
    errorno = snprintf_s(xlogfilename,
        MAXFNAMELEN,
        MAXFNAMELEN - 1,
        "%08X%08X%08X",
        starttli,
        (uint32)((startpointsegno) / XLogSegmentsPerXLogId),
        (uint32)((startpointsegno) % XLogSegmentsPerXLogId));
    securec_check_ss_c(errorno, "", "");

    /*
     * Construct backup label file
     */
    stamp_time = time(NULL);
    tmp = localtime(&stamp_time);
    if (tmp == NULL) {
        pg_fatal("localtime return NULL\n");
        return BUILD_FATAL;
    }

    strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", tmp);

    len = snprintf_s(buf,
        sizeof(buf),
        sizeof(buf) - 1,
        "START WAL LOCATION: %X/%X (file %s)\n"
        "CHECKPOINT LOCATION: %X/%X\n"
        "BACKUP METHOD: gs_rewind\n"
        "BACKUP FROM: standby\n"
        "START TIME: %s\n",
        /* omit LABEL: line */
        (uint32)(startpoint >> 32),
        (uint32)startpoint,
        xlogfilename,
        (uint32)(checkpointloc >> 32),
        (uint32)checkpointloc,
        strfbuf);
    securec_check_ss_c(len, "", "");
    if (len >= (int)sizeof(buf)) {
        pg_fatal("backup label buffer too small\n"); /* shouldn't happen */
        return BUILD_FATAL;
    }

    open_target_file("backup_label", true); /* BACKUP_LABEL_FILE */
    PG_CHECKBUILD_AND_RETURN();
    write_target_range(buf, 0, len, 1000);
    PG_CHECKBUILD_AND_RETURN();
    close_target_file();
    PG_CHECKBUILD_AND_RETURN();
    return BUILD_SUCCESS;
}

/*
 * Check CRC of control file
 */
static void checkControlFile(ControlFileData* ControlFile)
{
    pg_crc32c crc;

    /* Calculate CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    /* And simply compare it */
    if (!EQ_CRC32C(crc, ControlFile->crc))
        pg_fatal("unexpected control file CRC\n");
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void digestControlFile(ControlFileData* ControlFile, const char* src)
{
    errno_t errorno = EOK;

    errorno = memcpy_s(ControlFile, sizeof(ControlFileData), src, sizeof(ControlFileData));
    securec_check_c(errorno, "\0", "\0");
    /* Additional checks on control file */
    checkControlFile(ControlFile);
}

/*
 * Update the target's control file.
 */
static BuildErrorCode updateControlFile(ControlFileData* ControlFile)
{
    char buffer[PG_CONTROL_SIZE];
    errno_t errorno = EOK;

    /* Recalculate CRC of control file */
    INIT_CRC32C(ControlFile->crc);
    COMP_CRC32C(ControlFile->crc, (char*)ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(ControlFile->crc);
    /*
     * Write out PG_CONTROL_SIZE bytes into pg_control by zero-padding the
     * excess over sizeof(ControlFileData) to avoid premature EOF related
     * errors when reading it.
     */
    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check_c(errorno, "", "");
    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, ControlFile, sizeof(ControlFileData));
    securec_check_c(errorno, "", "");
    open_target_file("global/pg_control", false);
    PG_CHECKBUILD_AND_RETURN();
    write_target_range(buffer, 0, PG_CONTROL_SIZE, PG_CONTROL_SIZE);
    PG_CHECKBUILD_AND_RETURN();
    close_target_file();
    PG_CHECKBUILD_AND_RETURN();
    return BUILD_SUCCESS;
}

void openDebugLog(void)
{
    debug = true;
    return;
}

/**
 * delete existing double write file if existed, recreate it and write one page of zero
 */
static void rewind_dw_file()
{
#define MAX_REALPATH_LEN 4096
    int rc;
    int fd = -1;
    char dw_file_path[MAXPGPATH];
    char real_file_path[MAX_REALPATH_LEN] = {0};
    char* buf = NULL;
    char* unaligned_buf = NULL;

    /* Delete the dw file, if it exists. */
    rc = snprintf_s(dw_file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, OLD_DW_FILE_NAME);
    securec_check_ss_c(rc, "\0", "\0");
    if (realpath(dw_file_path, real_file_path) == NULL) {
        if (real_file_path[0] == '\0') {
            pg_fatal("could not get canonical path for file \"%s\": %s in backup\n", dw_file_path, gs_strerror(errno));
        }
    }

    if (is_file_exist(real_file_path)) {
        delete_all_file(real_file_path, true);
    }

    rc = memset_s(real_file_path, MAX_REALPATH_LEN, 0, MAX_REALPATH_LEN);
    securec_check_c(rc, "\0", "\0");

    /* Delete the dw build file, if it exists. */
    rc = snprintf_s(dw_file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, DW_BUILD_FILE_NAME);
    securec_check_ss_c(rc, "\0", "\0");
    if (realpath(dw_file_path, real_file_path) == NULL) {
        if (real_file_path[0] == '\0') {
            pg_fatal("could not get canonical path for file \"%s\": %s in backup\n", dw_file_path, gs_strerror(errno));
        }
    }

    if (is_file_exist(real_file_path)) {
        delete_all_file(real_file_path, true);
    }

    /* Create the dw build file. */
    if ((fd = open(real_file_path, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM)) < 0) {
        pg_fatal("could not create file \"%s\": %s\n", real_file_path, gs_strerror(errno));
    }

    unaligned_buf = (char*)malloc(BLCKSZ + BLCKSZ);
    if (unaligned_buf == NULL) {
        close(fd);
        pg_fatal("could not write data to file \"%s\": %s in backup\n", real_file_path, gs_strerror(errno));
    }

    buf = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);
    rc = memset_s(buf, BLCKSZ, 0, BLCKSZ);
    securec_check_c(rc, "\0", "\0");
    if (write(fd, buf, BLCKSZ) != BLCKSZ) {
        close(fd);
        pg_fatal("could not write data to file \"%s\": %s in backup\n", real_file_path, gs_strerror(errno));
    }

    free(unaligned_buf);
    unaligned_buf = NULL;
    close(fd);
}

/* move xlog files which are after CommonCheckpoint to another dir for finding rewind block */
static BuildErrorCode MoveOldXlogFiles(uint32 checkSeg, const char* newPath)
{
    char xlogLocation[MAXPGPATH] = {0};
    char movedXlogFile[MAXPGPATH] = {0};
    char movedXlogLocation[MAXPGPATH] = {0};
    char realMovedPath[PATH_MAX] = {0};
    char realXlogPath[PATH_MAX] = {0};
    DIR *xlogDir = NULL;
    struct dirent *dirEnt = NULL;
    TimeLineID tli = 0;
    uint32 xlogReadLogid = -1;
    uint32 xlogReadLogSeg = -1;
    int rc;
    int ret = 0;

    rc = snprintf_s(xlogLocation, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, "pg_xlog");
    securec_check_ss_c(rc, "\0", "\0");
    if (realpath(xlogLocation, realXlogPath) == NULL && realXlogPath[0] == '\0') {
        pg_log(PG_FATAL, "could not get canonical path for file \"%s\": %s in backup\n", xlogLocation,
            gs_strerror(errno));
        return BUILD_FATAL;
    }

    xlogDir = opendir(realXlogPath);
    if (!xlogDir) {
        pg_log(PG_ERROR, "open xlog dir %s failed when move old xlog files.\n", realXlogPath);
        return BUILD_ERROR;
    }
    while ((dirEnt = readdir(xlogDir)) != NULL) {
        if (strlen(dirEnt->d_name) == 24 && strspn(dirEnt->d_name, "0123456789ABCDEF") == 24) {
            if (sscanf_s(dirEnt->d_name, "%08X%08X%08X", &tli, &xlogReadLogid, &xlogReadLogSeg) != 3) {
                pg_log(PG_DEBUG, "failed to translate name to xlog: %s\n", dirEnt->d_name);
                continue;
            }
            if ((checkSeg / XLogSegmentsPerXLogId) < xlogReadLogid ||
                ((checkSeg / XLogSegmentsPerXLogId) == xlogReadLogid &&
                (checkSeg % XLogSegmentsPerXLogId) <= xlogReadLogSeg)) {
                rc = snprintf_s(movedXlogFile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", realXlogPath, dirEnt->d_name);
                securec_check_ss_c(rc, "\0", "\0");
                rc = snprintf_s(movedXlogLocation, MAXPGPATH, MAXPGPATH - 1, "%s/%s", newPath, dirEnt->d_name);
                securec_check_ss_c(rc, "\0", "\0");
                if (!is_file_exist(newPath)) {
                    if (mkdir(newPath, S_IRWXU) != 0) {
                        pg_log(PG_ERROR, "could not create directory \"%s\"\n", newPath);
                        (void)closedir(xlogDir);
                        return BUILD_ERROR;
                    }
                }
                if (realpath(movedXlogLocation, realMovedPath) == NULL && realMovedPath[0] == '\0') {
                    pg_log(PG_FATAL, "could not get canonical path for file \"%s\": %s in backup\n",
                        movedXlogLocation, gs_strerror(errno));
                    (void)closedir(xlogDir);
                    return BUILD_FATAL;
                }
                ret = rename(movedXlogFile, realMovedPath);
                if (ret != 0) {
                    pg_log(PG_ERROR, "failed to rename xlog file: %s\n", movedXlogFile);
                    (void)closedir(xlogDir);
                    return BUILD_ERROR;
                }
            }
        }
    }
    (void)closedir(xlogDir);
    return BUILD_SUCCESS;
}

