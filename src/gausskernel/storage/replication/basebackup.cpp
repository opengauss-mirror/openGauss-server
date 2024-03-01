/* -------------------------------------------------------------------------
 *
 * basebackup.cpp
 *	  code for taking a base backup and streaming it to a standby
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/basebackup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/types.h>
#include <sys/stat.h>

#include "access/xlog_internal.h" /* for pg_start/stop_backup */
#include "access/cbmparsexlog.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "gs_thread.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/pg_list.h"
#include "replication/basebackup.h"
#include "replication/dcf_data.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "replication/ss_disaster_cluster.h"
#include "replication/slot.h"
#include "access/xlog.h"
#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_buffers.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/page_compression.h"
#include "storage/pmsignal.h"
#include "storage/checksum.h"
#include "storage/file/fio_device.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "storage/cfs/cfs_tools.h"
#include "postmaster/syslogger.h"
#include "pgxc/pgxc.h"

/* t_thrd.proc_cxt.DataDir */
#include "miscadmin.h"
#include "replication/dcf_replication.h"

typedef struct {
    const char *label;
    bool progress;
    bool fastcheckpoint;
    bool nowait;
    bool includewal;
    bool sendtblspcmapfile;
    bool isBuildFromStandby;
    bool isCopySecureFiles;
    bool isCopyUpgradeFile;
    bool isObsmode;
} basebackup_options;

#define BUILD_PATH_LEN 2560 /* (MAXPGPATH*2 + 512) */
const int FILE_NAME_MAX_LEN = 1024;
const int MATCH_ONE = 1;
const int MATCH_TWO = 2;
const int MATCH_THREE = 3;
const int MATCH_FOUR = 4;
const int MATCH_FIVE = 5;
const int MATCH_SIX = 6;
const int MATCH_SEVEN = 7;

/*
 * Size of each block sent into the tar stream for larger files.
 */
#define TAR_SEND_SIZE (32 * 1024) /* data send unit 32KB */
#define EREPORT_WAL_NOT_FOUND(segno)                                              \
    do {                                                                          \
        char walErrorName[MAXFNAMELEN];                                           \
        XLogFileName(walErrorName, MAXFNAMELEN, t_thrd.xlog_cxt.ThisTimeLineID, segno);        \
        ereport(ERROR, (errmsg("could not find WAL file \"%s\"", walErrorName))); \
    } while (0)

XLogRecPtr XlogCopyStartPtr = InvalidXLogRecPtr;

#ifdef ENABLE_MOT
static int64 sendDir(
        const char *path, int basepathlen, bool sizeonly, List *tablespaces, bool sendtblspclinks, bool skipmot = true);
#else
static int64 sendDir(const char *path, int basepathlen, bool sizeonly, List *tablespaces, bool sendtblspclinks);
#endif
int64 sendTablespace(const char *path, bool sizeonly);
static void SendTableSpaceForBackup(basebackup_options* opt, List* tablespaces, char* labelfile,
    char* tblspc_map_file);
static bool sendFile(char *readfilename, char *tarfilename, struct stat *statbuf, bool missing_ok);
static void sendFileWithContent(const char *filename, const char *content);
static void _tarWriteHeader(const char *filename, const char *linktarget, struct stat *statbuf);
static void send_int8_string(StringInfoData *buf, int64 intval);
static void SendBackupHeader(List *tablespaces);
#ifdef ENABLE_MOT
static void SendMotCheckpointHeader(const char* path);
#endif
static void base_backup_cleanup(int code, Datum arg);
static void SendSecureFileToDisasterCluster(basebackup_options *opt);
static void perform_base_backup(basebackup_options *opt, DIR *tblspcdir);
static void parse_basebackup_options(List *options, basebackup_options *opt);
static int CompareWalFileNames(const void* a, const void* b);
static void SendXlogRecPtrResult(XLogRecPtr ptr, unsigned long long consensusPaxosIdx = 0);

static void send_xlog_location();
static void send_xlog_header(const char *linkpath);
static void save_xlogloc(const char *xloglocation);
static XLogRecPtr GetMinArchiveSlotLSN(void);
static XLogRecPtr GetMinLogicalSlotLSN(void);
static XLogRecPtr UpdateStartPtr(XLogRecPtr minLsn, XLogRecPtr curStartPtr);

/* compressed Function */
static void SendCompressedFile(char* readFileName, int basePathLen, struct stat& statbuf, bool missingOk, int64* size);

/*
 * save xlog location
 */
static void save_xlogloc(const char *xloglocation)
{
    errno_t rc = 0;

    if (0 == strncmp(xloglocation, t_thrd.proc_cxt.DataDir, strlen(t_thrd.proc_cxt.DataDir))) {
        rc = strncpy_s(t_thrd.basebackup_cxt.g_xlog_location, MAXPGPATH,
                       xloglocation + strlen(t_thrd.proc_cxt.DataDir) + 1, MAXPGPATH - 1);
        securec_check(rc, "", "");
        t_thrd.basebackup_cxt.g_xlog_location[MAXPGPATH - 1] = '\0';
    }
}

/*
 * send xlog location header
 */
static void send_xlog_header(const char *linkpath)
{
    StringInfoData buf;
    char pg_xlog[] = "data/pg_xlog";

    /* Construct and send the directory information */
    pq_beginmessage(&buf, 'T'); /* RowDescription */
    pq_sendint16(&buf, 1);      /* 1 fields */

    /* Second field - xloglink */
    pq_sendstring(&buf, "xloglink");
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, TEXTOID);
    pq_sendint16(&buf, UINT16_MAX);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_endmessage_noblock(&buf);

    /* Data row */
    pq_beginmessage(&buf, 'D');
    pq_sendint16(&buf, 1); /* number of columns */
    if (linkpath == NULL) {
        /* default data ,here we just send a fixed str */
        pq_sendint32(&buf, strlen(pg_xlog));
        pq_sendbytes(&buf, pg_xlog, strlen(pg_xlog));
    } else {
        pq_sendint32(&buf, strlen(linkpath));
        pq_sendbytes(&buf, linkpath, strlen(linkpath));
    }
    pq_endmessage_noblock(&buf);

    /* Send a CommandComplete message */
    pq_puttextmessage_noblock('C', "SELECT");
}

/*
 *  if xlog location is a link ,send it to standby
 */
static void send_xlog_location()
{
    char fullpath[MAXPGPATH] = {0};
    struct stat statbuf;
    int rc = 0;
    
    if (ENABLE_DSS) {
        char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;
        rc = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, "%s/pg_xlog%d", dssdir,
             g_instance.attr.attr_storage.dms_attr.instance_id);
    } else {
        rc = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, "%s/pg_xlog", t_thrd.proc_cxt.DataDir);
    }
    securec_check_ss(rc, "", "");

    if (lstat(fullpath, &statbuf) != 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat control file \"%s\": %m", fullpath)));
    }

#ifndef WIN32
    if (S_ISLNK(statbuf.st_mode)) {
#else
    if (pgwin32_is_junction(fullpath)) {
#endif
#if defined(HAVE_READLINK) || defined(WIN32)
        char linkpath[MAXPGPATH] = {0};
        int rllen;

        rllen = readlink(fullpath, linkpath, sizeof(linkpath));
        if (rllen < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read symbolic link \"%s\": %m", fullpath)));
        if (rllen >= (int)sizeof(linkpath))
            ereport(ERROR,
                    (errcode(ERRCODE_NAME_TOO_LONG), errmsg("symbolic link \"%s\" target is too long", fullpath)));
        linkpath[MAXPGPATH - 1] = '\0';

        /* save xlog location to varible */
        save_xlogloc(linkpath);

        send_xlog_header(linkpath);

#else

        /*
         * If the platform does not have symbolic links, it should not be
         * possible to have tablespaces - clearly somebody else created
         * them. Warn about it and ignore.
         */
        ereport(WARNING,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
        continue;
#endif /* HAVE_READLINK */
    } else if (S_ISDIR(statbuf.st_mode)) {
        statbuf.st_mode = S_IFDIR | S_IRWXU;
        send_xlog_header(NULL);
    }
}

/*
 * Called when ERROR or FATAL happens in perform_base_backup() after
 * we have started the backup - make sure we end it!
 */
static void base_backup_cleanup(int code, Datum arg)
{
    do_pg_abort_backup();
}

#ifndef ENABLE_MULTIPLE_NODES
static void SetPaxosIndex(unsigned long long *consensusPaxosIdx)
{
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        unsigned long long minAppliedIdx = 0;
        /*
         * Get the DCF min applied index from DCF in case that dcfData.realMinAppliedIdx was stale.
         * Save min applied index in dcfData.realMinAppliedIdx in case that switchover happened and
         * get min applied index 0 from DCF leader if the leader can't get response from some nodes
         * at the beginning. It because these nodes have had exceptions before switchover.
         */
        if (dcf_get_cluster_min_applied_idx(1, &minAppliedIdx) == 0) {
            minAppliedIdx = (minAppliedIdx > t_thrd.shemem_ptr_cxt.dcfData->realMinAppliedIdx) ?
                minAppliedIdx : t_thrd.shemem_ptr_cxt.dcfData->realMinAppliedIdx;
            /*
             * Let the leader replicates DCF log from min applied index (include min applied index)
             */
            *consensusPaxosIdx = minAppliedIdx;
            ereport(LOG, (errmsg("Enable DCF and sending paxos index is %llu", *consensusPaxosIdx)));
        } else {
            ereport(ERROR, (errmsg("Enable DCF and get min applied index from dcf failed!")));
        }
    }
}
#endif

static void ProcessSecureFilesForDisasterCluster(const char *path)
{
#define BASE_PATH_LEN 2
    struct dirent *de = NULL;
    char pathbuf[MAXPGPATH];
    struct stat statbuf;
    int64 size = 0;
    int rc = 0;


    DIR *dir = AllocateDir(path);

    while ((de = ReadDir(dir, path)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }
        if (!PostmasterIsAlive()) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("Postmaster exited, aborting active base backup")));
        }

        if (t_thrd.walsender_cxt.walsender_shutdown_requested || t_thrd.walsender_cxt.walsender_ready_to_stop) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("shutdown requested, aborting active base backup")));
        }
        if (t_thrd.postmaster_cxt.HaShmData &&
            (t_thrd.walsender_cxt.server_run_mode != t_thrd.postmaster_cxt.HaShmData->current_mode)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("server run mode changed, aborting active base backup")));
        }
        rc = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, de->d_name);
        securec_check_ss(rc, "", "");

        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", pathbuf)));
            }
            /* If the file went away while scanning, it's no error. */
            continue;
        }
        if (S_ISDIR(statbuf.st_mode)) {
            _tarWriteHeader(pathbuf + BASE_PATH_LEN, NULL, &statbuf);
            size += BUILD_PATH_LEN;
            ProcessSecureFilesForDisasterCluster(pathbuf);
        } else if (S_ISREG(statbuf.st_mode)) {
            bool sent = sendFile(pathbuf, pathbuf + BASE_PATH_LEN, &statbuf, true);
            size = size + ((statbuf.st_size + 511) & ~511) + BUILD_PATH_LEN;
            if (!sent) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not send file \"%s\"", pathbuf)));
            }
        }
    }
    FreeDir(dir);
}

static void SendSecureFileToDisasterCluster(basebackup_options *opt)
{
    char pathbuf[MAXPGPATH];
    struct stat statbuf;
    StringInfoData buf;
    tablespaceinfo *ti = (tablespaceinfo *)palloc0(sizeof(tablespaceinfo));
    List *tablespaces = NIL;
    int64 size = 0;
    int rc = 0;

    ti->size = size;
    tablespaces = (List *)lappend(tablespaces, ti);
    SendBackupHeader(tablespaces);
    pq_beginmessage(&buf, 'H');
    pq_sendbyte(&buf, 0);  /* overall format */
    pq_sendint16(&buf, 0); /* natts */
    pq_endmessage_noblock(&buf);
    if (opt->isCopyUpgradeFile) {
        rc = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "./upgrade_phase_info");
        securec_check_ss(rc, "", "");
        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", pathbuf)));
            } else {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("upgrade file \"%s\" does not exist.", pathbuf)));
            }
        }
        bool sent = sendFile(pathbuf, pathbuf + BASE_PATH_LEN, &statbuf, true);
        size = size + ((statbuf.st_size + 511) & ~511) + BUILD_PATH_LEN;
        if (!sent) {
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not send file \"%s\"", pathbuf)));
        }
        pq_putemptymessage_noblock('c');
        pfree(ti);
        return;
    }
    ProcessSecureFilesForDisasterCluster("./gs_secure_files");
    pq_putemptymessage_noblock('c');
    pfree(ti);
}

static XLogRecPtr UpdateStartPtr(XLogRecPtr minLsn, XLogRecPtr curStartPtr)
{
    XLogRecPtr resStartPtr = curStartPtr;
    if (!XLByteEQ(minLsn, InvalidXLogRecPtr) && (minLsn < resStartPtr)) {
        /* If xlog file has been recycled, don't use this minlsn */
        if (XlogFileIsExisted(t_thrd.proc_cxt.DataDir, minLsn, DEFAULT_TIMELINE_ID) == true) {
            resStartPtr = minLsn;
        }
    }
    return resStartPtr;
}

/*
 * Actually do a base backup for the specified tablespaces.
 *
 * This is split out mainly to avoid complaints about "variable might be
 * clobbered by longjmp" from stupider versions of gcc.
 */
static void perform_base_backup(basebackup_options *opt, DIR *tblspcdir)
{
    XLogRecPtr startptr;
    XLogRecPtr endptr;
    XLogRecPtr disasterSlotRestartPtr = InvalidXLogRecPtr;
    char *labelfile = NULL;
    char* tblspc_map_file = NULL;
    List* tablespaces = NIL;
    XLogSegNo startSegNo;

    if (opt->isBuildFromStandby) {
        startptr = StandbyDoStartBackup(opt->label, &labelfile, &tblspc_map_file, &tablespaces,
            tblspcdir, opt->progress);
    } else {
        startptr =
            do_pg_start_backup(opt->label, opt->fastcheckpoint, &labelfile, tblspcdir, &tblspc_map_file, &tablespaces,
            opt->progress, opt->sendtblspcmapfile);
    }
    if (opt->isObsmode) {
        t_thrd.walsender_cxt.is_obsmode = true;
    }
    /* Get the slot minimum LSN */
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);

    /*
     * If force recycle has been triggered, archive slot min lsn may be the smallest one, but its xlog is gone.
     * In result, we fail to use minlsn to update startptr. But we need keep some needed xlogs which are smaller
     * than startptr but bigger than archive slot min lsn. So calculate the specific restart lsn one by one.
     */
    /* consider min lsn in all slots, but we should fail to keep minlsn if force recycle happen, see detail above. */
    XLogRecPtr minlsn = XLogGetReplicationSlotMinimumLSNByOther();
    startptr = UpdateStartPtr(minlsn, startptr);
    /* only consider min lsn in archive slots */
    disasterSlotRestartPtr = GetMinArchiveSlotLSN();
    startptr = UpdateStartPtr(disasterSlotRestartPtr, startptr);
    /* only consider min lsn in logical slots */
    XLogRecPtr logicalMinLsn = GetMinLogicalSlotLSN();
    startptr = UpdateStartPtr(logicalMinLsn, startptr);

    LWLockAcquire(FullBuildXlogCopyStartPtrLock, LW_EXCLUSIVE);
    XlogCopyStartPtr = startptr;
    LWLockRelease(FullBuildXlogCopyStartPtrLock);
    ereport(INFO,
        (errmsg("The starting position of the xlog copy of the full build is: %X/%X. The slot minimum LSN is: %X/%X."
        " The disaster slot minimum LSN is: %X/%X. The logical slot minimum LSN is: %X/%X.",
        (uint32)(startptr >> 32), (uint32)startptr, (uint32)(minlsn >> 32), (uint32)minlsn,
        (uint32)(disasterSlotRestartPtr >> 32), (uint32)disasterSlotRestartPtr,
        (uint32)(logicalMinLsn >> 32), (uint32)logicalMinLsn)));

#ifdef ENABLE_MULTIPLE_NODES
    cbm_rotate_file(startptr);
#endif
    XLByteToSeg(startptr, startSegNo);
    XLogSegNo lastRemovedSegno = XLogGetLastRemovedSegno();
    if (startSegNo <= lastRemovedSegno) {
        startptr = (lastRemovedSegno + 1) * XLogSegSize;
    }
    SendXlogRecPtrResult(startptr);

    PG_ENSURE_ERROR_CLEANUP(base_backup_cleanup, (Datum)0);
    SendTableSpaceForBackup(opt, tablespaces, labelfile, tblspc_map_file);
    PG_END_ENSURE_ERROR_CLEANUP(base_backup_cleanup, (Datum)0);

    if (opt->isBuildFromStandby) {
        endptr = StandbyDoStopBackup(labelfile);
    } else {
        endptr = do_pg_stop_backup(labelfile, !opt->nowait);
    }

    if (opt->includewal) {
        /*
         * We've left the last tar file "open", so we can now append the
         * required WAL files to it.
         */
        char pathbuf[MAXPGPATH];
        XLogSegNo segno;
        XLogSegNo startsegno;
        XLogSegNo endsegno;
        struct stat statbuf;
        List* historyFileList = NIL;
        List* walFileList = NIL;
        char** walFiles;
        int nWalFiles;
        char firstoff[MAXFNAMELEN];
        char lastoff[MAXFNAMELEN];
        struct dirent* de;
        int i;
        ListCell* lc;
        TimeLineID tli;

        /*
         * I'd rather not worry about timelines here, so scan pg_xlog and
         * include all WAL files in the range between 'startptr' and 'endptr',
         * regardless of the timeline the file is stamped with. If there are
         * some spurious WAL files belonging to timelines that don't belong in
         * this server's history, they will be included too. Normally there
         * shouldn't be such files, but if there are, there's little harm in
         * including them.
         */
        XLByteToSeg(startptr, startsegno);
        XLogFileName(firstoff, MAXFNAMELEN, t_thrd.xlog_cxt.ThisTimeLineID, startsegno);
        XLByteToPrevSeg(endptr, endsegno);
        XLogFileName(lastoff, MAXFNAMELEN, t_thrd.xlog_cxt.ThisTimeLineID, endsegno);

        DIR* dir = AllocateDir("pg_xlog");
        if (!dir) {
            ereport(ERROR, (errmsg("could not open directory \"%s\": %m", "pg_xlog")));
        }
        while ((de = ReadDir(dir, "pg_xlog")) != NULL) {
            /* Does it look like a WAL segment, and is it in the range? */
            if (strlen(de->d_name) == 24 && strspn(de->d_name, "0123456789ABCDEF") == 24 &&
                strcmp(de->d_name + 8, firstoff + 8) >= 0 && strcmp(de->d_name + 8, lastoff + 8) <= 0) {
                walFileList = lappend(walFileList, pstrdup(de->d_name));
            } else if (strlen(de->d_name) == 8 + strlen(".history") && strspn(de->d_name, "0123456789ABCDEF") == 8 &&
                       strcmp(de->d_name + 8, ".history") == 0) {
                /* Does it look like a timeline history file? */
                historyFileList = lappend(historyFileList, pstrdup(de->d_name));
            }
        }
        FreeDir(dir);

        /*
         * Before we go any further, check that none of the WAL segments we
         * need were removed.
         */
        CheckXLogRemoved(startsegno, t_thrd.xlog_cxt.ThisTimeLineID);

        /*
         * Put the WAL filenames into an array, and sort. We send the files in
         * order from oldest to newest, to reduce the chance that a file is
         * recycled before we get a chance to send it over.
         */
        nWalFiles = list_length(walFileList);
        /*
         * There must be at least one xlog file in the pg_xlog directory,
         * since we are doing backup-including-xlog.
         */
        if (nWalFiles < 1) {
            ereport(ERROR, (errmsg("could not find any WAL files")));
        }

        walFiles = (char**)palloc0(nWalFiles * sizeof(char*));
        i = 0;
        foreach (lc, walFileList) {
            walFiles[i++] = (char*)lfirst(lc);
        }
        qsort(walFiles, nWalFiles, sizeof(char*), CompareWalFileNames);

        /*
         * Sanity check: the first and last segment should cover startptr and
         * endptr, with no gaps in between.
         */
        XLogFromFileName(walFiles[0], &tli, &segno);
        if (segno != startsegno) {
            EREPORT_WAL_NOT_FOUND(startsegno);
        }
        for (i = 0; i < nWalFiles; i++) {
            XLogSegNo currsegno = segno;
            XLogSegNo nextsegno = segno + 1;

            XLogFromFileName(walFiles[i], &tli, &segno);
            if (!(nextsegno == segno || currsegno == segno)) {
                EREPORT_WAL_NOT_FOUND(nextsegno);
            }
        }
        if (segno != endsegno) {
            EREPORT_WAL_NOT_FOUND(endsegno);
        }

        /* Ok, we have everything we need. Send the WAL files. */
        for (i = 0; i < nWalFiles; i++) {
            FILE* fp;
            char buf[TAR_SEND_SIZE];
            size_t cnt;
            pgoff_t len = 0;

            int rt = snprintf_s(pathbuf, MAXPGPATH,MAXPGPATH -1, XLOGDIR "/%s", walFiles[i]);
            securec_check_ss_c(rt, "\0", "\0");
            XLogFromFileName(walFiles[i], &tli, &segno);

            fp = AllocateFile(pathbuf, "rb");
            if (fp == NULL) {
                int save_errno = errno;

                /*
                 * Most likely reason for this is that the file was already
                 * removed by a checkpoint, so check for that to get a better
                 * error message.
                 */
                CheckXLogRemoved(segno, tli);

                errno = save_errno;
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", pathbuf)));
            }

            if (fstat(fileno(fp), &statbuf) != 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathbuf)));
            }
            if (statbuf.st_size != (off_t)XLogSegSize) {
                CheckXLogRemoved(segno, tli);
                ereport(ERROR, (errcode_for_file_access(), errmsg("unexpected WAL file size \"%s\"", walFiles[i])));
            }

            /* send the WAL file itself */
            _tarWriteHeader(pathbuf, NULL, &statbuf);

            while ((cnt = fread(buf, 1, Min((uint32)sizeof(buf), XLogSegSize - len), fp)) > 0) {
                CheckXLogRemoved(segno, tli);
                /* Send the chunk as a CopyData message */
                if (pq_putmessage('d', buf, cnt)) {
                    ereport(ERROR, (errmsg("base backup could not send data, aborting backup")));
                }

                len += cnt;

                if (len == (off_t)XLogSegSize)
                    break;
            }

            if (len != (off_t)XLogSegSize) {
                CheckXLogRemoved(segno, tli);
                ereport(ERROR, (errcode_for_file_access(), errmsg("unexpected WAL file size \"%s\"", walFiles[i])));
            }

            /* XLogSegSize is a multiple of 512, so no need for padding */
            FreeFile(fp);

            /*
             * Mark file as archived, otherwise files can get archived again
             * after promotion of a new node. This is in line with
             * walreceiver.c always doing a XLogArchiveForceDone() after a
             * complete segment.
             */
            StatusFilePath(pathbuf, MAXPGPATH, walFiles[i], ".done");
            sendFileWithContent(pathbuf, "");
        }

        /*
         * Send timeline history files too. Only the latest timeline history
         * file is required for recovery, and even that only if there happens
         * to be a timeline switch in the first WAL segment that contains the
         * checkpoint record, or if we're taking a base backup from a standby
         * server and the target timeline changes while the backup is taken.
         * But they are small and highly useful for debugging purposes, so
         * better include them all, always.
         */
        foreach (lc, historyFileList) {
            char* fname = (char*)lfirst(lc);

            int rt = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH-1, "/%s", fname);
            securec_check_ss_c(rt, "\0", "\0");
            if (lstat(pathbuf, &statbuf) != 0)
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathbuf)));

            sendFile(pathbuf, pathbuf, &statbuf, false);

            /* unconditionally mark file as archived */
            StatusFilePath(pathbuf, MAXPGPATH, fname, ".done");
            sendFileWithContent(pathbuf, "");
        }

        /* Send CopyDone message for the last tar file */
        pq_putemptymessage('c');
    }
    unsigned long long consensusPaxosIdx = 0;
#ifndef ENABLE_MULTIPLE_NODES
    SetPaxosIndex(&consensusPaxosIdx);
#endif
    SendXlogRecPtrResult(endptr, consensusPaxosIdx);
    LWLockAcquire(FullBuildXlogCopyStartPtrLock, LW_EXCLUSIVE);
    XlogCopyStartPtr = InvalidXLogRecPtr;
    LWLockRelease(FullBuildXlogCopyStartPtrLock);
}

/*
 * list_sort comparison function, to compare log/seg portion of WAL segment
 * filenames, ignoring the timeline portion.
 */
static int CompareWalFileNames(const void* a, const void* b)
{
    char* fna = *((char**)a);
    char* fnb = *((char**)b);

    return strcmp(fna + 8, fnb + 8);
}

#ifdef ENABLE_MOT
/*
 * Called when ERROR or FATAL happens in PerformMotCheckpointFetch() after
 * we have started the operation - make sure we end it!
 */
static void mot_checkpoint_fetch_cleanup(int code, Datum arg)
{
    MOTCheckpointFetchUnlock();
}

/*
 * Sends the current checkpoint to the client.
 */
void PerformMotCheckpointFetch()
{
    char fullChkptDir[MAXPGPATH] = {0};
    char ctrlFilePath[MAXPGPATH] = {0};
    size_t basePathLen = 0;

    MOTCheckpointFetchLock();
    PG_ENSURE_ERROR_CLEANUP(mot_checkpoint_fetch_cleanup, (Datum)0);
    {
        /* Nothing to do, if no MOT checkpoint exists. */
        if (MOTCheckpointExists(ctrlFilePath, MAXPGPATH, fullChkptDir, MAXPGPATH, basePathLen)) {
            /* send mot header */
            SendMotCheckpointHeader(fullChkptDir);
            StringInfoData buf;

            /* send mot.ctrl file */
            pq_beginmessage(&buf, 'H');
            pq_sendbyte(&buf, 0);  /* overall format */
            pq_sendint16(&buf, 0); /* natts */
            pq_endmessage_noblock(&buf);

            struct stat statbuf;
            if (lstat(ctrlFilePath, &statbuf) != 0) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not stat mot ctrl file \"%s\": %m", ctrlFilePath)));
            }

            /* send the MOT control file */
            sendFile(ctrlFilePath, "mot.ctrl", &statbuf, false);

            /* send the checkpoint dir */
            sendDir(fullChkptDir, (int)basePathLen, false, NIL, false);

            /* CopyDone */
            pq_putemptymessage_noblock('c');
        }
    }
    PG_END_ENSURE_ERROR_CLEANUP(mot_checkpoint_fetch_cleanup, (Datum)0);
    MOTCheckpointFetchUnlock();
}
#endif

/*
 * Parse the base backup options passed down by the parser
 */
static void parse_basebackup_options(List *options, basebackup_options *opt)
{
    ListCell *lopt = NULL;
    bool o_label = false;
    bool o_progress = false;
    bool o_fast = false;
    bool o_nowait = false;
    bool o_wal = false;
    bool o_buildstandby = false;
    bool o_copysecurefiles = false;
    bool o_iscopyupgradefile = false;
    bool o_tablespace_map = false;
    bool o_isobsmode = false;
    errno_t rc = memset_s(opt, sizeof(*opt), 0, sizeof(*opt));
    securec_check(rc, "", "");
    foreach (lopt, options) {
        DefElem *defel = (DefElem *)lfirst(lopt);
        if (strcmp(defel->defname, "label") == 0) {
            if (o_label)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            opt->label = strVal(defel->arg);
            o_label = true;
        } else if (strcmp(defel->defname, "progress") == 0) {
            if (o_progress)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            opt->progress = true;
            o_progress = true;
        } else if (strcmp(defel->defname, "fast") == 0) {
            if (o_fast)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            opt->fastcheckpoint = true;
            o_fast = true;
        } else if (strcmp(defel->defname, "nowait") == 0) {
            if (o_nowait)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            opt->nowait = true;
            o_nowait = true;
        } else if (strcmp(defel->defname, "wal") == 0) {
            if (o_wal)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            opt->includewal = true;
            o_wal = true;
        } else if (strcmp(defel->defname, "buildstandby") == 0) {
            if (o_buildstandby) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            }
            opt->isBuildFromStandby = true;
            o_buildstandby = true;
        } else if (strcmp(defel->defname, "tablespace_map") == 0) {
            if (o_tablespace_map) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            }
            opt->sendtblspcmapfile = true;
            o_tablespace_map = true;
        } else if (strcmp(defel->defname, "obsmode") == 0) {
            if (o_isobsmode) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            }
            opt->isObsmode = true;
            o_isobsmode = true;
        } else if (strcmp(defel->defname, "copysecurefile") == 0) {
            if (o_copysecurefiles) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            }
            opt->isCopySecureFiles = true;
            o_copysecurefiles = true;
        } else if (strcmp(defel->defname, "needupgradefile") == 0) {
            if (o_iscopyupgradefile) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicate option \"%s\"", defel->defname)));
            }
            opt->isCopyUpgradeFile = true;
            o_iscopyupgradefile = true;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }
    if (opt->label == NULL)
        opt->label = "base backup";
}

/*
 * SendBaseBackup() - send a complete base backup.
 *
 * The function will put the system into backup mode like pg_start_backup()
 * does, so that the backup is consistent even though we read directly from
 * the filesystem, bypassing the buffer cache.
 */
void SendBaseBackup(BaseBackupCmd *cmd)
{
    DIR *dir = NULL;
    MemoryContext backup_context;
    MemoryContext old_context;
    basebackup_options opt;

    parse_basebackup_options(cmd->options, &opt);

    backup_context = AllocSetContextCreate(CurrentMemoryContext, "Streaming base backup context",
                                           ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE);
    old_context = MemoryContextSwitchTo(backup_context);

    WalSndSetState(WALSNDSTATE_BACKUP);

    if (opt.isCopySecureFiles) {
        SendSecureFileToDisasterCluster(&opt);
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(backup_context);
        return;
    }

    if (u_sess->attr.attr_common.update_process_title) {
        char activitymsg[50];
        int rc = 0;

        rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "sending backup \"%s\"", opt.label);
        securec_check_ss(rc, "", "");

        set_ps_display(activitymsg, false);
    }
    
    if (ENABLE_DSS) {
        int rc = 0;
        char fullpath[MAXPGPATH] = {0};
        char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

        rc = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dssdir);
        securec_check_ss(rc, "", "");

        dir = AllocateDir(fullpath);
    } else {
        /* Make sure we can open the directory with tablespaces in it */
        dir = AllocateDir("pg_tblspc");
    }
    if (dir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", "pg_tblspc")));
        return;
    }

    /* read xlog location ,if xlog is a link ,send the link to client */
    send_xlog_location();

    perform_base_backup(&opt, dir);

    FreeDir(dir);

    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(backup_context);
}

static void send_int8_string(StringInfoData *buf, int64 intval)
{
    char is[32];
    int rc = 0;

    rc = snprintf_s(is, sizeof(is), sizeof(is) - 1, INT64_FORMAT, intval);
    securec_check_ss(rc, "", "");

    pq_sendint32(buf, strlen(is));
    pq_sendbytes(buf, is, strlen(is));
}

static void SendBackupHeader(List *tablespaces)
{
    StringInfoData buf;
    ListCell *lc = NULL;

    /* Construct and send the directory information */
    pq_beginmessage(&buf, 'T'); /* RowDescription */
    pq_sendint16(&buf, 4);      /* 4 fields */

    /* First field - spcoid */
    pq_sendstring(&buf, "spcoid");
    pq_sendint32(&buf, 0);      /* table oid */
    pq_sendint16(&buf, 0);      /* attnum */
    pq_sendint32(&buf, OIDOID); /* type oid */
    pq_sendint16(&buf, 4);      /* typlen */
    pq_sendint32(&buf, 0);      /* typmod */
    pq_sendint16(&buf, 0);      /* format code */

    /* Second field - spcpath */
    pq_sendstring(&buf, "spclocation");
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, TEXTOID);
    pq_sendint16(&buf, UINT16_MAX);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);

    /* Third field - size */
    pq_sendstring(&buf, "size");
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, INT8OID);
    pq_sendint16(&buf, 8);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);

    /* Third field - size */
    pq_sendstring(&buf, "relative");
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, INT8OID);
    pq_sendint16(&buf, 8);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);

    pq_endmessage_noblock(&buf);

    foreach (lc, tablespaces) {
        tablespaceinfo *ti = (tablespaceinfo *)lfirst(lc);

        /* Send one datarow message */
        pq_beginmessage(&buf, 'D');
        pq_sendint16(&buf, 4); /* number of columns */
        if (ti->path == NULL) {
            /* Length = -1 ==> NULL */
            pq_sendint32(&buf, UINT32_MAX);
            pq_sendint32(&buf, UINT32_MAX);
        } else {
            pq_sendint32(&buf, strlen(ti->oid)); /* length */
            pq_sendbytes(&buf, ti->oid, strlen(ti->oid));

            char *path = ti->path;
            if (ti->relativePath) {
                path = ti->relativePath;
                Assert(strlen(path) != strlen(ti->path));
            }
            pq_sendint32(&buf, strlen(path)); /* length */
            pq_sendbytes(&buf, path, strlen(path));
        }
        if (ti->size >= 0)
            send_int8_string(&buf, ti->size / 1024);
        else
            pq_sendint32(&buf, UINT32_MAX); /* NULL */

        /* send last column: relativePath flag */
        if (ti->relativePath)
            send_int8_string(&buf, 1);
        else
            pq_sendint32(&buf, UINT32_MAX); /* NULL */

        pq_endmessage_noblock(&buf);
    }

    /* Send a CommandComplete message */
    pq_puttextmessage_noblock('C', "SELECT");
}

#ifdef ENABLE_MOT
static void SendMotCheckpointHeader(const char* path)
{
    StringInfoData buf;

    /* Construct and send the directory information */
    pq_beginmessage(&buf, 'T'); /* RowDescription */
    pq_sendint16(&buf, 1);      /* nfields */

    /* First field - checkpoint path */
    pq_sendstring(&buf, "chkptloc");
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, TEXTOID);
    pq_sendint16(&buf, UINT16_MAX);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_endmessage_noblock(&buf);

    if (path != NULL) {
        /* Send one datarow message */
        pq_beginmessage(&buf, 'D');
        pq_sendint16(&buf, 1);            /* number of columns */
        pq_sendint32(&buf, strlen(path)); /* length */
        pq_sendbytes(&buf, path, strlen(path));
        pq_endmessage_noblock(&buf);
    }

    /* Send a CommandComplete message */
    pq_puttextmessage_noblock('C', "SELECT");
}
#endif

/*
 * Send a single resultset containing just a single
 * XlogRecPtr record (in text format)
 */
static void SendXlogRecPtrResult(XLogRecPtr ptr, unsigned long long consensusPaxosIdx)
{
    StringInfoData buf;
    char str[MAXFNAMELEN];
    int nRet = 0;

    nRet = snprintf_s(str, MAXFNAMELEN, MAXFNAMELEN - 1, "%X/%X", (uint32)(ptr >> 32), (uint32)ptr);
    securec_check_ss(nRet, "", "");


    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        pq_beginmessage(&buf, 'T'); /* RowDescription */
        pq_sendint16(&buf, 2);      /* 1 field */

        /* First field header */
        pq_sendstring(&buf, "recptr");
        pq_sendint32(&buf, 0);       /* table oid */
        pq_sendint16(&buf, 0);       /* attnum */
        pq_sendint32(&buf, TEXTOID); /* type oid */
        pq_sendint16(&buf, UINT16_MAX);
        pq_sendint32(&buf, 0);
        pq_sendint16(&buf, 0);

        /* Second field header */
        pq_sendstring(&buf, "consensusPaxosIndex");
        pq_sendint32(&buf, 0);       /* table oid */
        pq_sendint16(&buf, 0);       /* attnum */
        pq_sendint32(&buf, INT8OID); /* type oid */
        pq_sendint16(&buf, 8);
        pq_sendint32(&buf, 0);
        pq_sendint16(&buf, 0);
        pq_endmessage_noblock(&buf);

        /* Data row */
        pq_beginmessage(&buf, 'D');
        pq_sendint16(&buf, 2);           /* number of columns */
        pq_sendint32(&buf, strlen(str)); /* col1 len */
        pq_sendbytes(&buf, str, strlen(str));
        send_int8_string(&buf, consensusPaxosIdx); /* send col 2 */
    } else {
        pq_beginmessage(&buf, 'T'); /* RowDescription */
        pq_sendint16(&buf, 1);      /* 1 field */

        /* Field header */
        pq_sendstring(&buf, "recptr");
        pq_sendint32(&buf, 0);       /* table oid */
        pq_sendint16(&buf, 0);       /* attnum */
        pq_sendint32(&buf, TEXTOID); /* type oid */
        pq_sendint16(&buf, UINT16_MAX);
        pq_sendint32(&buf, 0);
        pq_sendint16(&buf, 0);
        pq_endmessage_noblock(&buf);

        /* Data row */
        pq_beginmessage(&buf, 'D');
        pq_sendint16(&buf, 1);           /* number of columns */
        pq_sendint32(&buf, strlen(str)); /* length */
        pq_sendbytes(&buf, str, strlen(str));
    }
    pq_endmessage_noblock(&buf);

    /* Send a CommandComplete message */
    pq_puttextmessage_noblock('C', "SELECT");
}

/*
 * Inject a file with given name and content in the output tar stream.
 */
static void sendFileWithContent(const char *filename, const char *content)
{
    struct stat statbuf;
    int pad, len;

    len = strlen(content);

    /*
     * Construct a stat struct for the backup_label file we're injecting in
     * the tar.
     */
    /* Windows doesn't have the concept of uid and gid */
#ifdef WIN32
    statbuf.st_uid = 0;
    statbuf.st_gid = 0;
#else
    statbuf.st_uid = geteuid();
    statbuf.st_gid = getegid();
#endif
    statbuf.st_mtime = time(NULL);
    statbuf.st_mode = S_IRUSR | S_IWUSR;
    statbuf.st_size = len;

    _tarWriteHeader(filename, NULL, &statbuf);
    /* Send the contents as a CopyData message */
    (void)pq_putmessage_noblock('d', content, len);

    /* Pad to 512 byte boundary, per tar format requirements */
    pad = ((len + 511) & ~511) - len;
    if (pad > 0) {
        char buf[512];
        errno_t rc = 0;

        rc = memset_s(buf, sizeof(buf), 0, pad);
        securec_check(rc, "", "");
        (void)pq_putmessage_noblock('d', buf, pad);
    }
}

/*
 * Include the tablespace directory pointed to by 'path' in the output tar
 * stream.  If 'sizeonly' is true, we just calculate a total length and return
 * it, without actually sending anything.
 *
 * Only used to send auxiliary tablespaces, not GAUSSDATA.
 */
int64 sendTablespace(const char *path, bool sizeonly)
{
    int64 size = 0;
    char pathbuf[MAXPGPATH] = {0};
    char relativedirname[MAXPGPATH] = {0};
    struct stat statbuf;
    int rc = 0;

    /*
     * 'path' points to the tablespace location, but we only want to include
     * the version directory in it that belongs to us.
     */
    if (ENABLE_DSS) {
        rc = snprintf_s(relativedirname, sizeof(relativedirname), sizeof(relativedirname) - 1, "%s",
                        TABLESPACE_VERSION_DIRECTORY);
        securec_check_ss(rc, "", "");
    } else {
        rc = snprintf_s(relativedirname, sizeof(relativedirname), sizeof(relativedirname) - 1, "%s_%s",
                        TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(rc, "", "");
    }

    rc = snprintf_s(pathbuf, sizeof(pathbuf), sizeof(pathbuf) - 1, "%s/%s", path, relativedirname);
    securec_check_ss(rc, "", "");

    /*
     * Store a directory entry in the tar file so we get the permissions
     * right.
     */
    if (lstat(pathbuf, &statbuf) != 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", pathbuf)));

        /* If the tablespace went away while scanning, it's no error. */
        return 0;
    }
    if (!sizeonly)
        _tarWriteHeader(relativedirname, NULL, &statbuf);
    size = BUILD_PATH_LEN; /* Size of the header just added */

    /* Send all the files in the tablespace version directory */
    size += sendDir(pathbuf, strlen(path), sizeonly, NIL, true);

    return size;
}

int IsBeginWith(const char *str1, char *str2)
{
    if (str1 == NULL || str2 == NULL)
        return -1;
    int len1 = strlen(str1);
    int len2 = strlen(str2);
    if ((len1 < len2) || (len1 == 0 || len2 == 0)) {
        return -1;
    }

    char *p = str2;
    int i = 0;
    while (*p != '\0') {
        if (*p != str1[i]) {
            return 0;
        }
        p++;
        i++;
    }
    return 1;
}

bool IsSkipDir(const char * dirName)
{
    if (strcmp(dirName, ".") == 0 || strcmp(dirName, "..") == 0)
        return true;
    if (strncmp(dirName, t_thrd.basebackup_cxt.g_xlog_location, strlen(dirName)) == 0)
        return true;
    if (strcmp(dirName, u_sess->attr.attr_common.Log_directory) == 0)
        return true;
    /* Skip temporary files */
    if (strncmp(dirName, PG_TEMP_FILE_PREFIX, strlen(PG_TEMP_FILE_PREFIX)) == 0)
        return true;
    if (strncmp(dirName, EXRTO_FILE_DIR, strlen(EXRTO_FILE_DIR)) == 0) {
        return true;
    }

    /*
     * If there's a backup_label file, it belongs to a backup started by
     * the user with pg_start_backup(). It is *not* correct for this
      * backup, our backup_label is injected into the tar separately.
     */
    if (strcmp(dirName, BACKUP_LABEL_FILE) == 0)
        return true;
    if (strcmp(dirName, DISABLE_CONN_FILE) == 0)
        return true;
    
    /* skip .recycle in dss */
    if (ENABLE_DSS && strcmp(dirName, ".recycle") == 0)
        return true;
    
    /* skip directory which not belong to primary in dss */
    if (ENABLE_DSS) {
        /* skip primary doublewrite and other node doublewrite */
        if (IsBeginWith(dirName, "pg_doublewrite") > 0) {
            return true;
        }
    
        /* skip other node pg_xlog except primary */
        if (IsBeginWith(dirName, "pg_xlog") > 0) { 
            size_t dirNameLen = strlen("pg_xlog");
            char instance_id[MAX_INSTANCEID_LEN];
            errno_t rc = EOK;
            rc = snprintf_s(instance_id, sizeof(instance_id), sizeof(instance_id) - 1, "%d",
                            g_instance.attr.attr_storage.dms_attr.instance_id);
            securec_check_ss_c(rc, "\0", "\0");
            /* not skip pg_xlog directory in file systerm */
            if (strlen(dirName) > dirNameLen && strcmp(dirName + dirNameLen, instance_id) != 0) 
                return true;
        }
    }

    return false;
}

bool IsSkipPath(const char * pathName)
{
    /* Skip pg_control here to back up it last */
    if (strcmp(pathName, "./global/pg_control") == 0)
        return true;
    if (IsBeginWith(pathName, "./global/pg_dw") > 0)
        return true;
    if (strcmp(pathName, "./global/pg_dw") == 0)
        return true;
    if (strcmp(pathName, "./global/pg_dw_single") == 0)
        return true;
    if (strcmp(pathName, "./global/pg_dw.build") == 0)
        return true;
    if (strcmp(pathName, "./global/config_exec_params") == 0)
        return true;

    if (strcmp(pathName, "./gaussdb.state") == 0 || strcmp(pathName, "./gs_build.pid") == 0)
        return true;

    if (strcmp(pathName, "./disc_readonly_test") == 0)
        return true;

    if (strstr(pathName, "./pg_rewind_bak") != NULL || strstr(pathName, "./pg_rewind_filemap") != NULL)
        return true;

    /*
     * 1 major version upgrade mode, following path contains
     * old database backup, need to skip
     * 2 pg_location is for relative tablespace, we need skip it
     */
    if (strcmp(pathName, "./full_upgrade_bak") == 0 || strcmp(pathName, "./pg_location") == 0)
        return true;

    if (strcmp(pathName, "./delay_xlog_recycle") == 0 || strcmp(pathName, "./delay_ddl_recycle") == 0)
        return true;

    if (t_thrd.walsender_cxt.is_obsmode == true && strcmp(pathName, "./pg_replslot") == 0)
        return true;

    /* skip pg_control in dss */
    if (ENABLE_DSS && strcmp(pathName, "+data/pg_control") == 0) {
        return true;
    }

    return false;
}

static bool IsDCFPath(const char *pathname)
{
    char fullpath[MAXPGPATH] = {0};
    errno_t rc = EOK;
    /* Skip paxosindex file in DCF mode */
    if ((strcmp(pathname, "./paxosindex") == 0) || (strcmp(pathname, "./paxosindex.backup") == 0))
        return true;

    if (strlen(pathname) <= 2) {
        return false;
    }
    /* We want to get the absolute path of dirName */
    rc = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/", t_thrd.proc_cxt.DataDir, pathname + 2);
    securec_check_ss(rc, "", "");

    int fullPathLen = strlen(fullpath);
    char comparedPath[MAXPGPATH] = {0};

    rc = snprintf_s(comparedPath, MAXPGPATH, MAXPGPATH - 1, "%s/",
        g_instance.attr.attr_storage.dcf_attr.dcf_log_path);
    securec_check_ss(rc, "", "");

    /* If fullpath is prefix of log path, then skip it. */
    if (strncmp(fullpath, comparedPath, fullPathLen) == 0) {
        return true;
    }
    rc = snprintf_s(comparedPath, MAXPGPATH, MAXPGPATH - 1, "%s/",
        g_instance.attr.attr_storage.dcf_attr.dcf_data_path);
    securec_check_ss(rc, "", "");

    /* If fullpath is prefix of data path, then skip it. */
    if (strncmp(fullpath, comparedPath, fullPathLen) == 0) {
        return true;
    }
    return false;
}

#define SEND_DIR_ADD_SIZE(size, statbuf) ((size) = (size) + (((statbuf).st_size + 511) & ~511) + BUILD_PATH_LEN)

/**
 * send file or compressed file
 * @param sizeOnly send or not
 * @param pathbuf path
 * @param basepathlen subfix of path
 * @param statbuf path stat
 */
static int64 SendRealFile(bool sizeOnly, char* pathbuf, int basepathlen, struct stat* statbuf)
{
    int64 size = 0;
    // we must ensure the page integrity when in IncrementalCheckpoint
    if (!sizeOnly && g_instance.attr.attr_storage.enableIncrementalCheckpoint &&
        IsCompressedFile(pathbuf, strlen(pathbuf))) {
        SendCompressedFile(pathbuf, basepathlen, (*statbuf), true, &size);
    } else {
        bool sent = false;
        if (!sizeOnly) {
            /* dss file send to other node in entire path */
            if (ENABLE_DSS && is_dss_file(pathbuf)) {
                sent = sendFile(pathbuf, pathbuf, statbuf, true);
            } else {
                sent = sendFile(pathbuf, pathbuf + basepathlen + 1, statbuf, true);
            }
        }
        if (sent || sizeOnly) {
            /* Add size, rounded up to 512byte block */
            SEND_DIR_ADD_SIZE(size, (*statbuf));
        }
    }
    return size;
}

/*
 * Include all files from the given directory in the output tar stream. If
 * 'sizeonly' is true, we just calculate a total length and return it, without
 * actually sending anything.
 *
 * Omit any directory in the tablespaces list, to avoid backing up
 * tablespaces twice when they were created inside PGDATA.
 */
#ifdef ENABLE_MOT
static int64 sendDir(
        const char *path, int basepathlen, bool sizeonly, List *tablespaces, bool sendtblspclinks, bool skipmot)
#else
static int64 sendDir(const char *path, int basepathlen, bool sizeonly, List *tablespaces, bool sendtblspclinks)
#endif
{
    struct dirent *de = NULL;
    char pathbuf[MAXPGPATH];
    struct stat statbuf;
    int64 size = 0;
    int rc = 0;

    DIR *dir = AllocateDir(path);
    while ((de = ReadDir(dir, path)) != NULL) {
        /* Skip special stuff */
        if (IsSkipDir(de->d_name)) {
		    continue;
        }

        /*
         * Check if the postmaster has signaled us to exit, and abort with an
         * error in that case. The error handler further up will call
         * do_pg_abort_backup() for us.
         */
        if (!PostmasterIsAlive()) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("Postmaster exited, aborting active base backup")));
        }

        if (t_thrd.walsender_cxt.walsender_shutdown_requested || t_thrd.walsender_cxt.walsender_ready_to_stop)
            ereport(ERROR, (errcode_for_file_access(), errmsg("shutdown requested, aborting active base backup")));

        if (t_thrd.postmaster_cxt.HaShmData &&
            (t_thrd.walsender_cxt.server_run_mode != t_thrd.postmaster_cxt.HaShmData->current_mode))
            ereport(ERROR, (errcode_for_file_access(), errmsg("server run mode changed, aborting active base backup")));

        rc = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, de->d_name);
        securec_check_ss(rc, "", "");
        /* Skip files related to DCF and DCF dir */
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf && IsDCFPath(pathbuf)) {
            continue;
        }

        /* Skip postmaster.pid and postmaster.opts and gs_gazelle.conf in the data directory */
        if (strcmp(pathbuf, "./postmaster.pid") == 0 || strcmp(pathbuf, "./postmaster.opts") == 0 ||
            strcmp(pathbuf, "./gs_gazelle.conf") == 0)
            continue;

        /* For gs_basebackup, we should not skip these files */
        if (strcmp(u_sess->attr.attr_common.application_name, "gs_basebackup") != 0) {
        /* For gs_backup, we should not skip these files */
            if (strcmp(pathbuf, "./pg_ctl.lock") == 0 || strcmp(pathbuf, "./postgresql.conf.lock") == 0 ||
                strcmp(pathbuf, "./postgresql.conf.bak") == 0 || strcmp(pathbuf, "./postgresql.conf") == 0 ||
                strcmp(de->d_name, "postgresql.conf.guc.bak") == 0 ||
                strcmp(pathbuf, "./postgresql.conf.bak.old") == 0) {
                continue;
            }

            if (strcmp(de->d_name, g_instance.attr.attr_security.ssl_cert_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_key_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_ca_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_crl_file) == 0 ||
                strcmp(de->d_name, ssl_cipher_file) == 0 || strcmp(de->d_name, ssl_rand_file) == 0 
 #ifdef USE_TASSL              
                || strcmp(de->d_name, g_instance.attr.attr_security.ssl_enc_cert_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_enc_key_file) == 0 ||
                strcmp(de->d_name, ssl_enc_cipher_file) == 0 || strcmp(de->d_name, ssl_enc_rand_file) == 0
#endif
                ) 
            {
                continue;
            }

            if (strcmp(pathbuf, "./client.crt") == 0 || strcmp(pathbuf, "./client.key") == 0) {
                continue;
            }
        }

        if (IS_PGXC_COORDINATOR && strcmp(pathbuf, "./pg_hba.conf") == 0)
            continue;
        /* Skip cn_drop_backup */
        if (IS_PGXC_COORDINATOR && strcmp(pathbuf, "./cn_drop_backup") == 0)
            continue;
        /* Skip pg_control here to back up it last */
        if (IsSkipPath(pathbuf)) {
            continue;
        }

#ifdef ENABLE_MOT
        /* skip mot files */
        if (skipmot && (strcmp(pathbuf, "./mot.ctrl") == 0 || strncmp(pathbuf, "./chkpt_", strlen("./chkpt_")) == 0)) {
            continue;
        }
#endif

        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT)
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", pathbuf)));

            /* If the file went away while scanning, it's no error. */
            continue;
        }

        /*
         * Skip pg_errorinfo, not useful to copy. But include
         * it as an empty directory anyway, so we get permissions right.
         */
        if (strcmp(de->d_name, "pg_errorinfo") == 0) {
            if (!sizeonly)
                _tarWriteHeader(pathbuf + basepathlen + 1, NULL, &statbuf);
            size += BUILD_PATH_LEN; /* Size of the header just added */
            continue;
        }

        /*
         * Skip physical slot file, not useful to copy, only include logical slot file.
         */
        if (strcmp(path, "./pg_replslot") == 0) {
            bool isphysicalslot = false;
            bool isArchiveSlot = false;
            LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
            for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
                if (s->in_use && s->data.database == InvalidOid && strcmp(de->d_name, NameStr(s->data.name)) == 0 &&
                    GET_SLOT_PERSISTENCY(s->data) != RS_BACKUP && GET_SLOT_EXTRA_DATA_LENGTH(s->data) == 0) {
                    isphysicalslot = true;
                    break;
                }
                if (s->in_use && s->data.database == InvalidOid && strcmp(de->d_name, NameStr(s->data.name)) == 0 &&
                    GET_SLOT_PERSISTENCY(s->data) != RS_BACKUP && s->extra_content != NULL) {
                    isArchiveSlot = true;
                    break;
                }
            }
            LWLockRelease(ReplicationSlotControlLock);

            if (isphysicalslot || (isArchiveSlot && AM_WAL_HADR_DNCN_SENDER))
                continue;
        }

        /*
         * We can skip pg_xlog, the WAL segments need to be fetched from the
         * WAL archive anyway. But include it as an empty directory anyway, so
         * we get permissions right.
         */

        /* when ss dorado replication enabled, "+data/pg_replication/" also need to copy when backup */
        int pathNameLen = strlen("+data/pg_xlog");
        if (strcmp(pathbuf, "./pg_xlog") == 0 || strncmp(pathbuf, "+data/pg_xlog", pathNameLen) == 0 ||
            strcmp(pathbuf, "+data/pg_replication") == 0 || strcmp(pathbuf, "+data/pg_tblspc") == 0) {
            if (!sizeonly) {
                /* If pg_xlog is a symlink, write it as a directory anyway */
#ifndef WIN32
                if (S_ISLNK(statbuf.st_mode)) {
#else
                if (pgwin32_is_junction(pathbuf)) {
#endif
#if defined(HAVE_READLINK) || defined(WIN32)
                    char linkpath[MAXPGPATH] = {0};

                    int rllen = readlink(pathbuf, linkpath, sizeof(linkpath));
                    if (rllen < 0)
                        ereport(ERROR, (errcode_for_file_access(),
                                        errmsg("could not read symbolic link \"%s\": %m", pathbuf)));
                    if (rllen >= (int)sizeof(linkpath))
                        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
                                        errmsg("symbolic link \"%s\" target is too long", pathbuf)));
                    linkpath[MAXPGPATH - 1] = '\0';

                    if (!sizeonly){
                        if (ENABLE_DSS && is_dss_file(pathbuf)) {
                            _tarWriteHeader(pathbuf, linkpath, &statbuf);
                        } else {
                            _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);
                        }
                    }
#else

                    /*
                     * If the platform does not have symbolic links, it should not be
                     * possible to have tablespaces - clearly somebody else created
                     * them. Warn about it and ignore.
                     */
                    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                      errmsg("tablespaces are not supported on this platform")));
                    continue;
#endif /* HAVE_READLINK */
                } else if (S_ISDIR(statbuf.st_mode)) {
                    statbuf.st_mode = S_IFDIR | S_IRWXU;
                    /* dss directory send to other node in entire path */
                    if (ENABLE_DSS && is_dss_file(pathbuf)) {
                        _tarWriteHeader(pathbuf, NULL, &statbuf);
                    } else {
                        _tarWriteHeader(pathbuf + basepathlen + 1, NULL, &statbuf);    
                    }
                }
            }
            size += BUILD_PATH_LEN; /* Size of the header just added */
            if (!sizeonly) {
#ifndef WIN32
                if (S_ISLNK(statbuf.st_mode)) {
#else
                if (pgwin32_is_junction(pathbuf)) {
#endif
#if defined(HAVE_READLINK) || defined(WIN32)
                    char linkpath[MAXPGPATH] = {0};

                    int rllen = readlink(pathbuf, linkpath, sizeof(linkpath));
                    if (rllen < 0)
                        ereport(ERROR,
                            (errcode_for_file_access(), errmsg("could not read symbolic link \"%s\": %m", pathbuf)));
                    if (rllen >= (int)sizeof(linkpath))
                        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
                                errmsg("symbolic link \"%s\" target is too long", pathbuf)));
                    linkpath[MAXPGPATH - 1] = '\0';
                    if (ENABLE_DSS && is_dss_file(pathbuf)) {
                        _tarWriteHeader(pathbuf, linkpath, &statbuf);
                    } else {
                        _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);    
                    }
#else
                    /*
                     * If the platform does not have symbolic links, it should not be
                     * possible to have tablespaces - clearly somebody else created
                     * them. Warn about it and ignore.
                     */
                    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("tablespaces are not supported on this platform")));
                    continue;
#endif /* HAVE_READLINK */

                } else if (S_ISDIR(statbuf.st_mode)) {
                    /*
                     * Also send archive_status directory (by hackishly reusing
                     * statbuf from above ...).
                     */
                    statbuf.st_mode = S_IFDIR | S_IRWXU;
                    if (ENABLE_DSS && is_dss_file(pathbuf)) {
                        _tarWriteHeader(pathbuf, NULL, &statbuf);
                    } else {
                        _tarWriteHeader("pg_xlog/archive_status", NULL, &statbuf);
                    }
                }
            }
            size += BUILD_PATH_LEN; /* Size of the header just added */
            continue;    /* don't recurse into pg_xlog */
        }

        /* Allow symbolic links in pg_tblspc only */
        if (strcmp(path, "./pg_tblspc") == 0 &&
#ifndef WIN32
            S_ISLNK(statbuf.st_mode)
#else
            pgwin32_is_junction(pathbuf)
#endif
        ) {
#if defined(HAVE_READLINK) || defined(WIN32)
            char linkpath[MAXPGPATH];

            int rllen = readlink(pathbuf, linkpath, sizeof(linkpath));
            if (rllen < 0)
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read symbolic link \"%s\": %m", pathbuf)));
            if (rllen >= (int)sizeof(linkpath))
                ereport(ERROR,
                        (errcode(ERRCODE_NAME_TOO_LONG), errmsg("symbolic link \"%s\" target is too long", pathbuf)));
            linkpath[rllen] = '\0';
            if (!sizeonly){
                if (ENABLE_DSS && is_dss_file(pathbuf)) {
                    _tarWriteHeader(pathbuf, linkpath, &statbuf);
                } else {
                    _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);
                }
            }
            size += BUILD_PATH_LEN; /* Size of the header just added */
#else

            /*
             * If the platform does not have symbolic links, it should not be
             * possible to have tablespaces - clearly somebody else created
             * them. Warn about it and ignore.
             */
            ereport(WARNING,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
            continue;
#endif /* HAVE_READLINK */
        } else if (S_ISDIR(statbuf.st_mode)) {
            bool skip_this_dir = false;
            ListCell *lc = NULL;

            /*
             * Store a directory entry in the tar file so we can get the
             * permissions right.
             */
            if (!sizeonly){
                if (ENABLE_DSS && is_dss_file(pathbuf)) {
                    _tarWriteHeader(pathbuf, NULL, &statbuf);
                } else {
                    _tarWriteHeader(pathbuf + basepathlen + 1, NULL, &statbuf);
                }
            }
            size += BUILD_PATH_LEN; /* Size of the header just added */

            /*
             * Call ourselves recursively for a directory, unless it happens
             * to be a separate tablespace located within PGDATA.
             */
            foreach (lc, tablespaces) {
                tablespaceinfo *ti = (tablespaceinfo *)lfirst(lc);

                /*
                 * ti->rpath is the tablespace relative path within PGDATA, or
                 * NULL if the tablespace has been properly located somewhere
                 * else.
                 *
                 * Skip past the leading "./" in pathbuf when comparing.
                 */
                if (ti->relativePath && strcmp(ti->relativePath, pathbuf + 2) == 0) {
                    skip_this_dir = true;
                    break;
                }
            }
            /*
             * skip sending directories inside pg_tblspc, if not required.
             */
            if (strcmp(pathbuf, "./pg_tblspc") == 0 && !sendtblspclinks)
                skip_this_dir = true;
            if (!skip_this_dir)
                size += sendDir(pathbuf, basepathlen, sizeonly, tablespaces, sendtblspclinks);
        } else if (S_ISREG(statbuf.st_mode)) {
            size += SendRealFile(sizeonly, pathbuf, basepathlen, &statbuf);
        } else
            ereport(WARNING, (errmsg("skipping special file \"%s\"", pathbuf)));
    }
    FreeDir(dir);
    return size;
}

/*
 * Functions for handling tar file format
 *
 * Copied from pg_dump, but modified to work with libpq for sending
 *
 * Utility routine to print possibly larger than 32 bit integers in a
 * portable fashion.  Filled with zeros.
 */
static void print_val(char *s, uint64 val, unsigned int base, size_t len)
{
    int i;

    for (i = len; i > 0; i--) {
        int digit = val % base;

        s[i - 1] = '0' + digit;
        val = val / base;
    }
}

/* forkname like "fsm.2" or "vm" */
static ForkNumber forkname_to_number(char *forkName, int *segNo)
{
    ForkNumber forkNum;

    if (forkName == NULL || *forkName == '\0')
        return InvalidForkNumber;

    for (int iforkNum = 0; iforkNum <= MAX_FORKNUM; iforkNum++) {
        forkNum = (ForkNumber)iforkNum;
        if (strstr(forkName, forkNames[forkNum]) != NULL) {
            int nmatch = sscanf_s(forkName + strlen(forkNames[forkNum]), ".%d", segNo);
            if (nmatch == 0) {
                /* Actually, impossible here */
                *segNo = 0;
            }
            return forkNum;
        }
    }

    return MAX_FORKNUM + 1;
}

/* filename should be basename */
static bool check_data_filename(char *filename, int *segNo)
{
    if (basename(filename) != filename) {
        /* not a base name, return directly */
        return false;
    }

    char *token = NULL;
    char *tmptoken = NULL;
    int nmatch = 0;
    unsigned int relNode;

    token = strtok_r(filename, "_", &tmptoken);
    if ('\0' == tmptoken[0]) {
        uint dot_count = 0;
        int filename_idx = static_cast<int>(strlen(filename) - 1);
        // check the last word must be num
        if (isdigit(filename[filename_idx]) == false) {
            *segNo = 0;
            return false;
        }
        while (filename_idx >= 0) {
            if (filename[filename_idx] == '.') {
                dot_count++;
            }
            /* if the char is not num/'.' or dot_count > 1, then break */
            if ((isdigit(filename[filename_idx]) == false && filename[filename_idx] != '.') || dot_count > 1) {
                *segNo = 0;
                return false;
            }
            filename_idx--;
        }
        /* MAIN_FORK */
        nmatch = sscanf_s(filename, "%u.%d", &relNode, segNo);
        return (nmatch == 1 || nmatch == 2);
    } else {
        /* Note that after strtok, filename is broken. So we use tmptoken from here. */
        ForkNumber forkNum = forkname_to_number(tmptoken, segNo);

        if (forkNum == InvalidForkNumber) {
            return false;
        }
        if (forkNum > MAX_FORKNUM) {
            /* column store */
            return false;
        }

        /* Only fsm and vm fork should be copied. */
        if (forkNum == FSM_FORKNUM || forkNum == VISIBILITYMAP_FORKNUM) {
            return true;
        }
        return false;
    }
}

UndoFileType CheckUndoPath(const char* fname, int* segNo)
{
    RelFileNode rnode;
    rnode.spcNode = InvalidOid;
    rnode.dbNode = InvalidOid;
    rnode.relNode = InvalidOid;
    rnode.bucketNode = InvalidBktId;
    rnode.opt = 0;
    /* undo file and undo transaction slot file Checking */
    if (sscanf_s(fname, "undo/permanent/%05X.%07zX", &rnode.relNode, segNo) == MATCH_TWO) {
        return UNDO_RECORD;
    } else if (sscanf_s(fname, "undo/permanent/%05X.meta.%07zX", &rnode.relNode, segNo) == MATCH_TWO) {
        return UNDO_META;
    }
    return UNDO_INVALID;
}

bool is_row_data_file(const char *path, int *segNo, UndoFileType *undoFileType)
{
    struct stat path_st;
    if (stat(path, &path_st) < 0) {
        if (errno != ENOENT) {
            ereport(ERROR, (errmsg("Cannot stat file %s when judge it's a rowdatafile for roach. OS error: %s", path,
                                   strerror(errno))));
        } else {
            ereport(INFO, (errmsg("File %s not exists when judge it's a rowdatafile for roach. OS error: %s", path,
                                  strerror(errno))));
        }
        return false;
    } else if (S_ISDIR(path_st.st_mode)) {
        return false;
    }
    
    /* memcpy path without "_compress" for row data file judge */
    char tablePath[MAXPGPATH] = {0};
    if (IsCompressedFile(path, strlen(path))) {
        auto rc = memcpy_s(tablePath, MAXPGPATH, path, strlen(path) - strlen(COMPRESS_STR));
        securec_check_c(rc, "", "");
        path = tablePath;
    }

    char buf[FILE_NAME_MAX_LEN];
    unsigned int dbNode;
    unsigned int spcNode;
    int nmatch;
    char *fname = NULL;

   /* Skip compressed page files */
    size_t pathLen = strlen(path);
    if (pathLen >= 4) {
        const char* suffix = path + pathLen - 4;
        if (strncmp(suffix, "_pca", 4) == 0 || strncmp(suffix, "_pcd", 4) == 0) {
            return false;
        }
    }

    if ((fname = strstr((char *)path, "pg_tblspc/")) != NULL) {
        nmatch = sscanf_s(fname, "pg_tblspc/%u/%*[^/]/%u/%s", &spcNode, &dbNode, buf, sizeof(buf));
        if (nmatch == 3) {
            return check_data_filename(buf, segNo);
        }
    }

    if ((fname = strstr((char *)path, "global/")) != NULL) {
        nmatch = sscanf_s(fname, "global/%s", buf, sizeof(buf));
        if (nmatch == 1) {
            return check_data_filename(buf, segNo);
        }
    }

    if ((fname = strstr((char *)path, "base/")) != NULL) {
        nmatch = sscanf_s(fname, "base/%u/%s", &dbNode, buf, sizeof(buf));
        if (nmatch == 2) {
            return check_data_filename(buf, segNo);
        }
    }

    if ((fname = strstr((char *)path, "PG_9.2_201611171")) != NULL) {
        nmatch = sscanf_s(fname, "PG_9.2_201611171_%*[^/]/%u/%s", &dbNode, buf, sizeof(buf));
        if (nmatch == 2) {
            return check_data_filename(buf, segNo);
        }
    }

    if ((fname = strstr((char*)path, "undo/")) != NULL) {
            *undoFileType = CheckUndoPath(fname, segNo);
            return (*undoFileType != UNDO_INVALID);
    }

    return false;
}

static void SendTableSpaceForBackup(basebackup_options* opt, List* tablespaces, char* labelfile, char* tblspc_map_file)
{
    ListCell *lc = NULL;
    int64 asize = 0;
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

    if (ENABLE_DSS) {
        /* Add a node for all directory in dss*/
        asize = sendDir(".", 1, true, tablespaces, true) + sendDir(dssdir, 1, true, tablespaces, true);
    } else {
        asize = sendDir(".", 1, true, tablespaces, true);
    }
    
    /* Add a node for the base directory at the end */
    tablespaceinfo *ti = (tablespaceinfo *)palloc0(sizeof(tablespaceinfo));
    ti->size = opt->progress ? asize : -1;
    tablespaces = (List *)lappend(tablespaces, ti);

    /* Send tablespace header */
    SendBackupHeader(tablespaces);

    /* Send off our tablespaces one by one */
    foreach (lc, tablespaces) {
        tablespaceinfo *iterti = (tablespaceinfo *)lfirst(lc);
        StringInfoData buf;

        /* Send CopyOutResponse message */
        pq_beginmessage(&buf, 'H');
        pq_sendbyte(&buf, 0);  /* overall format */
        pq_sendint16(&buf, 0); /* natts */
        pq_endmessage_noblock(&buf);

        /* In the main tar, include the backup_label first. */
        if (iterti->path == NULL)
            sendFileWithContent(BACKUP_LABEL_FILE, labelfile);

        /*
         * if the tblspc created in datadir , the files under tblspc do not send,
         * and send them as normal under datadir,
         * so we just send these tblspcs only once.
         */
        if (iterti->path != NULL) {
            /* Skip the tablespace if it's created in GAUSSDATA */
            sendTablespace(iterti->path, false);
        } else {
            /* Then the tablespace_map file, if required... */
            if (tblspc_map_file && opt->sendtblspcmapfile) {
                sendFileWithContent(TABLESPACE_MAP, tblspc_map_file);
                sendDir(".", 1, false, tablespaces, false);
            } else
                sendDir(".", 1, false, tablespaces, true);
                /* send file in dss*/
                if (ENABLE_DSS) {
                    sendDir(dssdir, 1, false, tablespaces, true);
                }
        }
        
        /* In the main tar, include pg_control last. */
        if (iterti->path == NULL) {
            struct stat statbuf;
            TimeLineID primay_tli = 0;
            char path[MAXPGPATH] = {0};

            if (lstat(XLOG_CONTROL_FILE, &statbuf) != 0) {
                LWLockAcquire(FullBuildXlogCopyStartPtrLock, LW_EXCLUSIVE);
                XlogCopyStartPtr = InvalidXLogRecPtr;
                LWLockRelease(FullBuildXlogCopyStartPtrLock);
                ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not stat control file \"%s\": %m", XLOG_CONTROL_FILE)));
            }

            sendFile(XLOG_CONTROL_FILE, XLOG_CONTROL_FILE, &statbuf, false);
            /* In the main tar, include the last timeline history file at last. */
            primay_tli = t_thrd.xlog_cxt.ThisTimeLineID;
            while (primay_tli > 1) {
                TLHistoryFilePath(path, MAXPGPATH, primay_tli);
                if (lstat(path, &statbuf) == 0)
                    sendFile(path, path, &statbuf, false);
                primay_tli--;
            }
        }

        /*
         * If we're including WAL, and this is the main data directory we
         * don't terminate the tar stream here. Instead, we will append
         * the xlog files below and terminate it then. This is safe since
         * the main data directory is always sent *last*.
         */
        if (opt->includewal && iterti->path == NULL) {
            Assert(lnext(lc) == NULL);
        } else
            pq_putemptymessage_noblock('c'); /* CopyDone */
    }
}

/**
 * init buf_block if not yet; repalloc PqSendBuffer if necessary
 */
static void SendFilePreInit(void)
{
    if (t_thrd.basebackup_cxt.buf_block == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        t_thrd.basebackup_cxt.buf_block = (char *)palloc0(TAR_SEND_SIZE);
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * repalloc to `MaxBuildAllocSize' in one time, to avoid many small step repalloc in `pq_putmessage_noblock'
     * and low performance.
     */
    if (INT2SIZET(t_thrd.libpq_cxt.PqSendBufferSize) < MaxBuildAllocSize) {
        t_thrd.libpq_cxt.PqSendBuffer = (char *)repalloc(t_thrd.libpq_cxt.PqSendBuffer, MaxBuildAllocSize);
        t_thrd.libpq_cxt.PqSendBufferSize = MaxBuildAllocSize;
    }
}

/**
 * check file
 * @param readFileName
 * @param statbuf
 * @param supress error if missingOk is false when file is not found
 * @return return null if file.size > MAX_TAR_MEMBER_FILELEN or file cant found
 */
static FILE *SizeCheckAndAllocate(char *readFileName, const struct stat &statbuf, bool missingOk)
{
    /*
     * Some compilers will throw a warning knowing this test can never be true
     * because pgoff_t can't exceed the compared maximum on their platform.
     */
    if (statbuf.st_size > MAX_TAR_MEMBER_FILELEN) {
        ereport(WARNING, (errcode(ERRCODE_NAME_TOO_LONG),
                          errmsg("archive member \"%s\" too large for tar format", readFileName)));
        return NULL;
    }

    FILE *fp = AllocateFile(readFileName, "rb");
    if (fp == NULL) {
        if (errno == ENOENT && missingOk)
            return NULL;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", readFileName)));
    }
    return fp;

}

static void SendCompressedFile(char* readFileName, int basePathLen, struct stat& statbuf, bool missingOk, int64* size)
{
    char* tarfilename = readFileName + basePathLen + 1;
    SendFilePreInit();
    FILE* compressFd = SizeCheckAndAllocate(readFileName, statbuf, missingOk);
    if (compressFd == NULL) {
        return;
    }

    struct stat fileStat;
    if (fstat(fileno(compressFd), &fileStat) != 0) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not stat file or directory \"%s\": ", readFileName)));
        }
    }

    int segmentNo = 0;
    UndoFileType undoFileType = UNDO_INVALID;
    if (!is_row_data_file(readFileName, &segmentNo, &undoFileType)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s is not a relation file.", readFileName)));
    }
    
    /* send the pkg header containing msg like file size */
    _tarWriteHeader(tarfilename, NULL, (struct stat*)(&statbuf));

    off_t totalLen = 0;
    auto maxExtent = (fileStat.st_size / BLCKSZ) / CFS_EXTENT_SIZE;

    CfsReadStruct cfsReadStruct{compressFd, NULL, 0};
    for (int extentIndex = 0; extentIndex < maxExtent; extentIndex++) {
        cfsReadStruct.extentCount = (BlockNumber)extentIndex;
        size_t extentLen = 0;
        auto mmapHeaderResult = MMapHeader(compressFd, (BlockNumber)extentIndex, true);

        cfsReadStruct.header = mmapHeaderResult.header;
        cfsReadStruct.extentCount = (BlockNumber)extentIndex;
        off_t sendLen = 0;
        size_t bufferSize = (size_t)(TAR_SEND_SIZE - sendLen);
        char extentHeaderPage[BLCKSZ] = {0};
        CfsExtentHeader *header = (CfsExtentHeader*)extentHeaderPage;
        header->chunk_size = cfsReadStruct.header->chunk_size;
        header->algorithm = cfsReadStruct.header->algorithm;

        uint16 chunkIndex = 1;
        BlockNumber blockNumber;
        for (blockNumber = 0; blockNumber < cfsReadStruct.header->nblocks; ++blockNumber) {
            size_t len = CfsReadCompressedPage(t_thrd.basebackup_cxt.buf_block + sendLen, bufferSize, blockNumber,
                                               &cfsReadStruct,
                                               (CFS_LOGIC_BLOCKS_PER_FILE * segmentNo +
                                               extentIndex * CFS_LOGIC_BLOCKS_PER_EXTENT + blockNumber));
            /* valid check */
            if (len == COMPRESS_FSEEK_ERROR) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("fseek error")));
            } else if (len == COMPRESS_FREAD_ERROR) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("fread error")));
            } else if (len == COMPRESS_CHECKSUM_ERROR) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("checksum error")));
            } else if (len == COMPRESS_BLOCK_ERROR) {
                ereport(ERROR, (ERRCODE_INVALID_PARAMETER_VALUE, errmsg("blocknum \"%u\" exceeds max block number",
                    blockNumber)));
            }
            sendLen += (off_t)len;
            if (sendLen > TAR_SEND_SIZE - BLCKSZ) {
                if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, (size_t)sendLen)) {
                    MmapFree(&mmapHeaderResult);
                    ereport(ERROR,
                            (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
                }
                sendLen = 0;
            }
            CfsExtentAddress *cfsExtentAddress = GetExtentAddress(header, (uint16)blockNumber);
            uint8 nchunks = (uint8)(len / header->chunk_size);
            for (size_t i = 0; i  < nchunks; i++) {
                cfsExtentAddress->chunknos[i] = chunkIndex++;
            }
            cfsExtentAddress->nchunks = nchunks;
            cfsExtentAddress->allocated_chunks = nchunks;
            cfsExtentAddress->checksum = AddrChecksum32(cfsExtentAddress, nchunks);
            extentLen += len;
        }
        if (sendLen != 0) {
            if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, (size_t)sendLen)) {
                MmapFree(&mmapHeaderResult);
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
            }
        }
        header->nblocks = blockNumber;
        header->allocated_chunks = (pg_atomic_uint32)(chunkIndex - 1);

        /* send zero chunks to remote */
        size_t extentSize = CFS_LOGIC_BLOCKS_PER_EXTENT * BLCKSZ;
        if (extentLen < extentSize) {
            auto rc = memset_s(t_thrd.basebackup_cxt.buf_block, TAR_SEND_SIZE, 0, TAR_SEND_SIZE);
            securec_check(rc, "\0", "\0");
            int left = extentSize - extentLen;
            while (left > 0) {
                auto currentSendLen = TAR_SEND_SIZE < left ? TAR_SEND_SIZE : left;
                if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, (size_t)currentSendLen)) {
                    MmapFree(&mmapHeaderResult);
                    ereport(ERROR,
                        (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
                }
                left -= TAR_SEND_SIZE;
            }
        }
        /* send CfsHeader Page */
        if (pq_putmessage_noblock('d', extentHeaderPage, BLCKSZ)) {
            MmapFree(&mmapHeaderResult);
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
        }
        totalLen += (off_t)(extentSize + BLCKSZ);
        MmapFree(&mmapHeaderResult);
    }
    size_t pad = (size_t)(((totalLen + 511) & ~511) - totalLen);
    if (pad > 0) {
        securec_check(memset_s(t_thrd.basebackup_cxt.buf_block, pad, 0, pad), "", "");
        (void)pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, pad);
    }
    SEND_DIR_ADD_SIZE(*size, statbuf);
}

/*
 * Given the member, write the TAR header & send the file.
 *
 * If 'missing_ok' is true, will not throw an error if the file is not found.
 *
 * Returns true if the file was successfully sent, false if 'missing_ok',
 * and the file did not exist.
 */
static bool sendFile(char *readfilename, char *tarfilename, struct stat *statbuf, bool missing_ok)
{
    FILE *fp = NULL;
    size_t cnt;
    pgoff_t len = 0;
    size_t pad;
    errno_t rc = 0;
    int check_loc = 0;
    BlockNumber blkno = 0;
    uint16 checksum = 0;
    bool isNeedCheck = false;
    int segNo = 0;
    const int MAX_RETRY_LIMITA = 60;
    int retryCnt = 0;
    UndoFileType undoFileType = UNDO_INVALID;

    SendFilePreInit();
    fp = SizeCheckAndAllocate(readfilename, *statbuf, missing_ok);
    if (fp == NULL) {
        return false;
    }

    isNeedCheck = is_row_data_file(readfilename, &segNo, &undoFileType);
    ereport(DEBUG1, (errmsg("sendFile, filename is %s, isNeedCheck is %d", readfilename, isNeedCheck)));

    /* make sure data file size is integer multiple of BLCKSZ and change statbuf if needed */
    if (isNeedCheck) {
        statbuf->st_size = statbuf->st_size - (statbuf->st_size % BLCKSZ);
    }

    /* send the pkg header containing msg like file size */
    _tarWriteHeader(tarfilename, NULL, statbuf);
    
    /* Because pg_control file is shared in all instance when dss is enabled. Here pg_control of primary id
     * need to send to main standby in standby cluster, so we must seek a postion accoring to primary id.
     * Then content of primary id will be read.
     */
    if (ENABLE_DSS && strcmp(tarfilename, XLOG_CONTROL_FILE) == 0) {
        int read_size = BUFFERALIGN(sizeof(ControlFileData));
        statbuf->st_size = read_size;
        int primary_id = SSGetPrimaryInstId();
        off_t seekpos = (off_t)BLCKSZ * primary_id;
        fseek(fp, seekpos, SEEK_SET);
    }
    
    while ((cnt = fread(t_thrd.basebackup_cxt.buf_block, 1, Min(TAR_SEND_SIZE, statbuf->st_size - len), fp)) > 0) {
        if (t_thrd.walsender_cxt.walsender_ready_to_stop)
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup receive stop message, aborting backup")));
    recheck:
        if (cnt != (size_t)Min(TAR_SEND_SIZE, statbuf->st_size - len)) {
            if (ferror(fp)) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", readfilename)));
            }
        }
        if (g_instance.attr.attr_storage.enableIncrementalCheckpoint && isNeedCheck && !SS_DISASTER_CLUSTER) {
            uint32 segSize;
            GET_SEG_SIZE(undoFileType, segSize);
            /* len and cnt must be integer multiple of BLCKSZ. */
            if (len % BLCKSZ != 0 || cnt % BLCKSZ != 0) {
                ereport(
                    ERROR,
                    (errcode_for_file_access(),
                     errmsg("base backup file length cannot be divisibed by 8k: file %s, len %ld, cnt %ld, aborting "
                            "backup",
                            readfilename, len, cnt)));
            }
            for (check_loc = 0; (unsigned int)(check_loc) < cnt; check_loc += BLCKSZ) {
                blkno = len / BLCKSZ + check_loc / BLCKSZ + (segNo * segSize);
                PageHeader phdr = PageHeader(t_thrd.basebackup_cxt.buf_block + check_loc);
                if (!CheckPageZeroCases(phdr)) {
                    continue;
                }
                checksum = pg_checksum_page(t_thrd.basebackup_cxt.buf_block + check_loc, blkno);

                if (phdr->pd_checksum != checksum) {
                    if (fseeko(fp, (off_t)len, SEEK_SET) != 0) {
                        ereport(ERROR,
                                (errcode_for_file_access(), errmsg("could not seek in file \"%s\": %m", readfilename)));
                    }
                    cnt = fread(t_thrd.basebackup_cxt.buf_block, 1, Min(TAR_SEND_SIZE, statbuf->st_size - len), fp);
                    if (cnt > 0 && retryCnt < MAX_RETRY_LIMITA) {
                        retryCnt++;
                        pg_usleep(1000000);
                        goto recheck;
                    } else if (cnt > 0 && retryCnt == MAX_RETRY_LIMITA) {
                        ereport(
                            ERROR,
                            (errcode_for_file_access(),
                             errmsg("base backup cheksum failed in file \"%s\" block %u (computed: %d, recorded: %d), "
                                    "aborting backup",
                                    readfilename, blkno, checksum, phdr->pd_checksum)));
                    } else {
                        retryCnt = 0;
                        break;
                    }
                }
                retryCnt = 0;
            }
        }

        /* Send the chunk as a CopyData message */
        if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, cnt))
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));

        len += cnt;

        if (len >= statbuf->st_size) {
            /*
             * Reached end of file. The file could be longer, if it was
             * extended while we were sending it, but for a base backup we can
             * ignore such extended data. It will be restored from WAL.
             */
            break;
        }
    }

    /* If the file was truncated while we were sending it, pad it with zeros */
    if (len < statbuf->st_size) {
        rc = memset_s(t_thrd.basebackup_cxt.buf_block, TAR_SEND_SIZE, 0, TAR_SEND_SIZE);
        securec_check(rc, "", "");
        while (len < statbuf->st_size) {
            cnt = Min(TAR_SEND_SIZE, statbuf->st_size - len);
            (void)pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, cnt);
            len += cnt;
        }
    }

    /* Pad to 512 byte boundary, per tar format requirements */
    pad = ((len + 511) & ~511) - len;
    if (pad > 0) {
        rc = memset_s(t_thrd.basebackup_cxt.buf_block, pad, 0, pad);
        securec_check(rc, "", "");
        (void)pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, pad);
    }

    (void)FreeFile(fp);
    return true;
}

static void _tarWriteHeader(const char *filename, const char *linktarget, struct stat *statbuf)
{
    char h[BUILD_PATH_LEN];
    errno_t rc = EOK;
    int nRet = 0;
    /*
     * Note: most of the fields in a tar header are not supposed to be
     * null-terminated.  We use sprintf, which will write a null after the
     * required bytes; that null goes into the first byte of the next field.
     * This is okay as long as we fill the fields in order.
     */
    rc = memset_s(h, sizeof(h), 0, sizeof(h));
    securec_check(rc, "", "");

    /* Name 1024 */
    nRet = sprintf_s(&h[0], BUILD_PATH_LEN, "%.1023s", filename);
    securec_check_ss(nRet, "", "");

    if (linktarget != NULL || S_ISDIR(statbuf->st_mode)) {
        /*
         * We only support symbolic links to directories, and this is
         * indicated in the tar format by adding a slash at the end of the
         * name, the same as for regular directories.
         */
        h[strlen(filename)] = '/';
        h[strlen(filename) + 1] = '\0';
    }

    /* Mode 8 */
    nRet = sprintf_s(&h[1024], BUILD_PATH_LEN - 1024, "%07o ", statbuf->st_mode);
    securec_check_ss(nRet, "", "");

    /* File size 12 - 11 digits, 1 space, no NUL */
    if (linktarget != NULL || S_ISDIR(statbuf->st_mode))
        /* Symbolic link or directory has size zero */
        print_val(&h[1048], 0, 8, 11);
    else
        print_val(&h[1048], statbuf->st_size, 8, 11);

    if (linktarget != NULL) {
        /* Type - Symbolic link */
        if (0 == strncmp(linktarget, t_thrd.proc_cxt.DataDir, strlen(t_thrd.proc_cxt.DataDir))) {
            /* Symbolic link for relative location tablespace. */
            nRet = sprintf_s(&h[1080], BUILD_PATH_LEN - 1080, "3");
            securec_check_ss(nRet, "", "");

            nRet = sprintf_s(&h[1081], BUILD_PATH_LEN - 1081, "%.1023s",
                             linktarget + strlen(t_thrd.proc_cxt.DataDir) + 1);
            securec_check_ss(nRet, "", "");
        } else {
            nRet = sprintf_s(&h[1080], BUILD_PATH_LEN - 1080, "2");
            securec_check_ss(nRet, "", "");

            nRet = sprintf_s(&h[1081], BUILD_PATH_LEN - 1081, "%.1023s", linktarget);
            securec_check_ss(nRet, "", "");
        }
    } else if (S_ISDIR(statbuf->st_mode)) {
        /* Type - directory */
        nRet = sprintf_s(&h[1080], BUILD_PATH_LEN - 1080, "5");
        securec_check_ss(nRet, "", "");
    } else {
        /* Type - regular file */
        nRet = sprintf_s(&h[1080], BUILD_PATH_LEN - 1080, "0");
        securec_check_ss(nRet, "", "");
    }

    /* Link tag 100 (NULL) */
    /* Now send the completed header. */
    (void)pq_putmessage_noblock('d', h, BUILD_PATH_LEN);
}

static XLogRecPtr GetMinArchiveSlotLSN(void)
{
    XLogRecPtr minArchSlotPtr = InvalidXLogRecPtr;

    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        XLogRecPtr restart_lsn;
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == false) {
            restart_lsn = slot->data.restart_lsn;
            if ((!XLByteEQ(restart_lsn, InvalidXLogRecPtr)) &&
                (XLByteEQ(minArchSlotPtr, InvalidXLogRecPtr) || XLByteLT(restart_lsn, minArchSlotPtr))) {
                minArchSlotPtr = restart_lsn;
            }
        }
        SpinLockRelease(&slot->mutex);
    }
    return minArchSlotPtr;
}

static XLogRecPtr GetMinLogicalSlotLSN(void)
{
    XLogRecPtr minLogicalSlotPtr = InvalidXLogRecPtr;

    for (int slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        XLogRecPtr restart_lsn;
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->data.database != InvalidOid) {
            restart_lsn = slot->data.restart_lsn;
            if ((!XLByteEQ(restart_lsn, InvalidXLogRecPtr)) &&
                (XLByteEQ(minLogicalSlotPtr, InvalidXLogRecPtr) || XLByteLT(restart_lsn, minLogicalSlotPtr))) {
                minLogicalSlotPtr = restart_lsn;
            }
        }
        SpinLockRelease(&slot->mutex);
    }
    return minLogicalSlotPtr;
}

void ut_save_xlogloc(const char *xloglocation)
{
    save_xlogloc(xloglocation);
}
