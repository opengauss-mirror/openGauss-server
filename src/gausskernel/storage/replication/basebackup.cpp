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
#include "replication/slot.h"
#include "access/xlog.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/page_compression.h"
#include "storage/pmsignal.h"
#include "storage/checksum.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
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

    rc = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, "%s/pg_xlog", t_thrd.proc_cxt.DataDir);
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

    XLogRecPtr minlsn = XLogGetReplicationSlotMinimumLSNByOther();
    if (!XLByteEQ(minlsn, InvalidXLogRecPtr) && (minlsn < startptr)) {
        /* If xlog file has been recycled, don't use this minlsn */
        if (XlogFileIsExisted(t_thrd.proc_cxt.DataDir, minlsn, DEFAULT_TIMELINE_ID) == true) {
            startptr = minlsn;
        }
    }
    disasterSlotRestartPtr = GetMinArchiveSlotLSN();
    if (disasterSlotRestartPtr != InvalidXLogRecPtr && XLByteLT(disasterSlotRestartPtr, startptr)) {
        if (XlogFileIsExisted(t_thrd.proc_cxt.DataDir, disasterSlotRestartPtr, DEFAULT_TIMELINE_ID) == true) {
            startptr = disasterSlotRestartPtr;
        }
    }
    LWLockAcquire(FullBuildXlogCopyStartPtrLock, LW_EXCLUSIVE);
    XlogCopyStartPtr = startptr;
    LWLockRelease(FullBuildXlogCopyStartPtrLock);
    ereport(
        INFO,
        (errmsg("The starting position of the xlog copy of the full build is: %X/%X. The slot minimum LSN is: %X/%X.",
                (uint32)(startptr >> 32), (uint32)startptr, (uint32)(minlsn >> 32), (uint32)minlsn)));
#ifdef ENABLE_MULTIPLE_NODES
    cbm_rotate_file(startptr);
#endif
    XLByteToSeg(startptr, startSegNo);
    XLogSegNo lastRemovedSegno = XLogGetLastRemovedSegno();
    if (startSegNo <= lastRemovedSegno) {
        startptr = (lastRemovedSegno + 1) * XLOG_SEG_SIZE;
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
            if (statbuf.st_size != XLogSegSize) {
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

                if (len == XLogSegSize)
                    break;
            }

            if (len != XLogSegSize) {
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

    /* Make sure we can open the directory with tablespaces in it */
    dir = AllocateDir("pg_tblspc");
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
    rc = snprintf_s(relativedirname, sizeof(relativedirname), sizeof(relativedirname) - 1, "%s_%s",
                    TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName);
    securec_check_ss(rc, "", "");

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

    /*
     * If there's a backup_label file, it belongs to a backup started by
     * the user with pg_start_backup(). It is *not* correct for this
      * backup, our backup_label is injected into the tar separately.
     */
    if (strcmp(dirName, BACKUP_LABEL_FILE) == 0)
        return true;
    if (strcmp(dirName, DISABLE_CONN_FILE) == 0)
        return true;

    return false;
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
 * @param pathBufLen pathLen
 * @param basepathlen subfix of path
 * @param statbuf path stat
 */
static void SendRealFile(bool sizeOnly, char* pathbuf, size_t pathBufLen, int basepathlen, struct stat* statbuf)
{
    int64 size = 0;
    // we must ensure the page integrity when in IncrementalCheckpoint
    if (!sizeOnly && g_instance.attr.attr_storage.enableIncrementalCheckpoint &&
        IsCompressedFile(pathbuf, strlen(pathbuf)) != COMPRESSED_TYPE_UNKNOWN) {
        SendCompressedFile(pathbuf, basepathlen, (*statbuf), false, &size);
    } else {
        bool sent = false;
        if (!sizeOnly) {
            sent = sendFile(pathbuf, pathbuf + basepathlen + 1, statbuf, true);
        }
        if (sent || sizeOnly) {
            /* Add size, rounded up to 512byte block */
            SEND_DIR_ADD_SIZE(size, (*statbuf));
        }
    }
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
                strcmp(pathbuf, "./postgresql.conf.bak.old") == 0) {
                continue;
            }

            if (strcmp(de->d_name, g_instance.attr.attr_security.ssl_cert_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_key_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_ca_file) == 0 ||
                strcmp(de->d_name, g_instance.attr.attr_security.ssl_crl_file) == 0 ||
                strcmp(de->d_name, ssl_cipher_file) == 0 || strcmp(de->d_name, ssl_rand_file) == 0) {
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
        if (strcmp(pathbuf, "./pg_xlog") == 0) {
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

                    if (!sizeonly)
                        _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);
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
                    _tarWriteHeader(pathbuf + basepathlen + 1, NULL, &statbuf);
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
                    _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);
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
                    _tarWriteHeader("pg_xlog/archive_status", NULL, &statbuf);
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
            if (!sizeonly)
                _tarWriteHeader(pathbuf + basepathlen + 1, linkpath, &statbuf);
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
            if (!sizeonly)
                _tarWriteHeader(pathbuf + basepathlen + 1, NULL, &statbuf);
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
            SendRealFile(sizeonly, pathbuf, strlen(pathbuf), basepathlen, &statbuf);
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
    /* Add a node for the base directory at the end */
    tablespaceinfo *ti = (tablespaceinfo *)palloc0(sizeof(tablespaceinfo));
    ti->size = opt->progress ? sendDir(".", 1, true, tablespaces, true) : -1;
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

static void TransferPcaFile(const char *readFileName, int basePathLen, const struct stat &statbuf,
                            PageCompressHeader *transfer,
                            size_t len)
{
    const char *tarfilename = readFileName + basePathLen + 1;
    _tarWriteHeader(tarfilename, NULL, (struct stat*)(&statbuf));
    char *data = (char *) transfer;
    size_t lenBuffer = len;
    while (lenBuffer > 0) {
        size_t transferLen = Min(TAR_SEND_SIZE, lenBuffer);
        if (pq_putmessage_noblock('d', data, transferLen)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
        }
        data = data + transferLen;
        lenBuffer -= transferLen;
    }
    size_t pad = ((len + 511) & ~511) - len;
    if (pad > 0) {
        securec_check(memset_s(t_thrd.basebackup_cxt.buf_block, pad, 0, pad), "", "");
        (void) pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, pad);
    }
}

static void FileStat(char* path, struct stat* fileStat)
{
    if (stat(path, fileStat) != 0) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", path)));
        }
    }
}

static void SendCompressedFile(char* readFileName, int basePathLen, struct stat& statbuf, bool missingOk, int64* size)
{
    char* tarfilename = readFileName + basePathLen + 1;
    SendFilePreInit();
    FILE* fp = SizeCheckAndAllocate(readFileName, statbuf, missingOk);
    if (fp == NULL) {
        return;
    }

    size_t readFileNameLen = strlen(readFileName);
    /* dont send pca file */
    if (readFileNameLen < 4 || strncmp(readFileName + readFileNameLen - 4, "_pca", 4) == 0 ||
        strncmp(readFileName + readFileNameLen - 4, "_pcd", 4) != 0) {
        FreeFile(fp);
        return;
    }

    char tablePath[MAXPGPATH] = {0};
    securec_check_c(memcpy_s(tablePath, MAXPGPATH, readFileName, readFileNameLen - 4), "", "");
    int segmentNo = 0;
    UndoFileType undoFileType = UNDO_INVALID;
    if (!is_row_data_file(tablePath, &segmentNo, &undoFileType)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("%s is not a relation file.", tablePath)));
    }

    char pcaFilePath[MAXPGPATH];
    securec_check(strcpy_s(pcaFilePath, MAXPGPATH, readFileName), "", "");
    /* change pcd => pca */
    pcaFilePath[readFileNameLen - 1] = 'a';

    FILE* pcaFile = AllocateFile(pcaFilePath, "rb");
    if (pcaFile == NULL) {
        if (errno == ENOENT && missingOk) {
            FreeFile(fp);
            return;
        }
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", pcaFilePath)));
    }

    uint16 chunkSize = ReadChunkSize(pcaFile, pcaFilePath, MAXPGPATH);

    struct stat pcaStruct;
    FileStat((char*)pcaFilePath, &pcaStruct);

    size_t pcaFileLen = SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunkSize);
    PageCompressHeader* map = pc_mmap_real_size(fileno(pcaFile), pcaFileLen, true);
    if (map == MAP_FAILED) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("Failed to mmap page compression address file %s: %m", pcaFilePath)));
    }

    PageCompressHeader* transfer = (PageCompressHeader*)palloc0(pcaFileLen);
    /* decompressed page buffer, avoid frequent allocation */
    BlockNumber blockNum = 0;
    size_t chunkIndex = 1;
    off_t totalLen = 0;
    off_t sendLen = 0;
    /* send the pkg header containing msg like file size */
    BlockNumber totalBlockNum = (BlockNumber)pg_atomic_read_u32(&map->nblocks);

    /* some chunks may have been allocated but not used.
     * Reserve 0 chunks for avoiding the error when the size of a compressed block extends */
    auto reservedChunks = 0;
    securec_check(memcpy_s(transfer, pcaFileLen, map, pcaFileLen), "", "");
    decltype(statbuf.st_size) realSize = (map->allocated_chunks + reservedChunks)  * chunkSize;
    statbuf.st_size = statbuf.st_size >= realSize ? statbuf.st_size : realSize;
    _tarWriteHeader(tarfilename, NULL, (struct stat*)(&statbuf));
    bool* onlyExtend = (bool*)palloc0(totalBlockNum * sizeof(bool));

    /* allocated in advance to prevent repeated allocated */
    ReadBlockChunksStruct rbStruct{map, fp, segmentNo, readFileName};
    for (blockNum = 0; blockNum < totalBlockNum; blockNum++) {
        PageCompressAddr* addr = GET_PAGE_COMPRESS_ADDR(transfer, chunkSize, blockNum);
        /* skip some blocks which only extends. The size of blocks is 0. */
        if (addr->nchunks == 0) {
            onlyExtend[blockNum] = true;
            continue;
        }
        /* read block to t_thrd.basebackup_cxt.buf_block */
        size_t bufferSize = TAR_SEND_SIZE - sendLen;
        size_t len = ReadAllChunkOfBlock(t_thrd.basebackup_cxt.buf_block + sendLen, bufferSize, blockNum, rbStruct);
        /* merge Blocks */
        sendLen += len;
        if (totalLen + (off_t)len > statbuf.st_size) {
            ReleaseMap(map, readFileName);
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("some blocks in %s had been changed. Retry backup please. PostBlocks:%u, currentReadBlocks "
                           ":%u, transferSize: %lu. totalLen: %lu, len: %lu",
                        readFileName,
                        totalBlockNum,
                        blockNum,
                        statbuf.st_size,
                        totalLen,
                        len)));
        }
        if (sendLen > TAR_SEND_SIZE - BLCKSZ) {
            if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, sendLen)) {
                ReleaseMap(map, readFileName);
                ereport(ERROR, (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
            }
            sendLen = 0;
        }
        uint8 nchunks = len / chunkSize;
        addr->nchunks = addr->allocated_chunks = nchunks;
        for (size_t i = 0; i < nchunks; i++) {
            addr->chunknos[i] = chunkIndex++;
        }
        addr->checksum = AddrChecksum32(blockNum, addr, chunkSize);
        totalLen += len;
    }
    ReleaseMap(map, readFileName);

    if (sendLen != 0) {
        if (pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, sendLen)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup could not send data, aborting backup")));
        }
    }

    /* If the file was truncated while we were sending it, pad it with zeros */
    if (totalLen < statbuf.st_size) {
        securec_check(memset_s(t_thrd.basebackup_cxt.buf_block, TAR_SEND_SIZE, 0, TAR_SEND_SIZE), "", "");
        while (totalLen < statbuf.st_size) {
            size_t cnt = Min(TAR_SEND_SIZE, statbuf.st_size - totalLen);
            (void)pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, cnt);
            totalLen += cnt;
        }
    }

    size_t pad = ((totalLen + 511) & ~511) - totalLen;
    if (pad > 0) {
        securec_check(memset_s(t_thrd.basebackup_cxt.buf_block, pad, 0, pad), "", "");
        (void)pq_putmessage_noblock('d', t_thrd.basebackup_cxt.buf_block, pad);
    }
    SEND_DIR_ADD_SIZE(*size, statbuf);

    // allocate chunks of some pages which only extend
    for (size_t blockNum = 0; blockNum < totalBlockNum; ++blockNum) {
        if (onlyExtend[blockNum]) {
            PageCompressAddr* addr = GET_PAGE_COMPRESS_ADDR(transfer, chunkSize, blockNum);
            for (size_t i = 0; i < addr->allocated_chunks; i++) {
                addr->chunknos[i] = chunkIndex++;
            }
        }
    }
    transfer->nblocks = transfer->last_synced_nblocks = blockNum;
    transfer->last_synced_allocated_chunks = transfer->allocated_chunks = chunkIndex;
    TransferPcaFile(pcaFilePath, basePathLen, pcaStruct, transfer, pcaFileLen);

    SEND_DIR_ADD_SIZE(*size, pcaStruct);
    FreeFile(pcaFile);
    FreeFile(fp);
    pfree(transfer);
    pfree(onlyExtend);
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
    const int MAX_RETRY_LIMIT = 60;
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

    while ((cnt = fread(t_thrd.basebackup_cxt.buf_block, 1, Min(TAR_SEND_SIZE, statbuf->st_size - len), fp)) > 0) {
        if (t_thrd.walsender_cxt.walsender_ready_to_stop)
            ereport(ERROR, (errcode_for_file_access(), errmsg("base backup receive stop message, aborting backup")));
    recheck:
        if (cnt != (size_t)Min(TAR_SEND_SIZE, statbuf->st_size - len)) {
            if (ferror(fp)) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", readfilename)));
            }
        }
        if (g_instance.attr.attr_storage.enableIncrementalCheckpoint && isNeedCheck) {
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
                    if (cnt > 0 && retryCnt < MAX_RETRY_LIMIT) {
                        retryCnt++;
                        pg_usleep(1000000);
                        goto recheck;
                    } else if (cnt > 0 && retryCnt == MAX_RETRY_LIMIT) {
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
void ut_save_xlogloc(const char *xloglocation)
{
    save_xlogloc(xloglocation);
}
