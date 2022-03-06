/*-------------------------------------------------------------------------
 *
 * pg_xlogdump.cpp - decode and display WAL
 *
 * Copyright (c) 2013-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/pg_xlogdump/pg_xlogdump.cpp
 *
 * NOTES
 *	  Hard to maintain old pg_xlogdump 'cause we will change wal format.
 *	  Merge PostgreSQL contrib tool pg_xlogdump to pg_xlogdump.
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"

#define __STDC_FORMAT_MACROS
#include <dirent.h>
#include <inttypes.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "getopt_long.h"
#include "lib/stringinfo.h"
#include "replication/replicainternal.h"
#include "rmgrdesc.h"
#include "storage/smgr/segment.h"

static const char* progname;

typedef struct XLogDumpPrivate {
    TimeLineID timeline;
    char* inpath;
    XLogRecPtr startptr;
    XLogRecPtr endptr;
    bool endptr_reached;
    char* shareStorageXlogFilePath;
    long shareStorageXlogSize;
} XLogDumpPrivate;

typedef struct XLogDumpConfig {
    /* display options */
    bool bkp_details;
    bool write_fpw;
    int stop_after_records;
    int already_displayed_records;
    bool follow;
    bool stats;

    /* filter options */
    int filter_by_rmgr;
    TransactionId filter_by_xid;
    bool filter_by_xid_enabled;
    bool verbose;
} XLogDumpConfig;

typedef struct Stats {
    uint64 count;
    uint64 rec_len;
    uint64 fpi_len;
} Stats;

#define MAX_XLINFO_TYPES 16

typedef struct XLogDumpStats {
    uint64 count;
    Stats rmgr_stats[RM_NEXT_ID];
    Stats record_stats[RM_NEXT_ID][MAX_XLINFO_TYPES];
} XLogDumpStats;

static void XLogDumpTablePage(XLogReaderState* record, int block_id, RelFileNode rnode, BlockNumber blk);
static void XLogDumpXLogRead(const char* directory, TimeLineID timeline_id, XLogRecPtr startptr, char* buf, Size count);
static int XLogDumpReadPage(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetPtr,
    char* readBuff, TimeLineID* curFileTLI, char* xlog_path = NULL);
static void XLogDumpCountRecord(XLogDumpConfig* config, XLogDumpStats* stats, XLogReaderState* record);
static void XLogDumpDisplayRecord(XLogDumpConfig* config, XLogReaderState* record);
static void XLogDumpStatsRow(const char* name, uint64 n, uint64 total_count, uint64 rec_len, uint64 total_rec_len,
    uint64 fpi_len, uint64 total_fpi_len, uint64 tot_len, uint64 total_len);
static void XLogDumpDisplayStats(XLogDumpConfig* config, XLogDumpStats* stats);
static void usage(void);

static int fuzzy_open_file(const char* directory, const char* fname);
static void split_path(char* path, char** dir, char** fname);
static bool verify_directory(const char* directory);
static void print_rmgr_list(void);
static void fatal_error(const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

/*
 * Big red button to push when things go horribly wrong.
 */
static void fatal_error(const char* fmt, ...)
{
    va_list args;

    fflush(stdout);

    fprintf(stderr, "%s: FATAL:  ", progname);
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fputc('\n', stderr);

    exit(EXIT_FAILURE);
}

static void print_rmgr_list(void)
{
    int i;

    for (i = 0; i <= RM_MAX_ID; i++) {
        printf("%s\n", RmgrDescTable[i].rm_name);
    }
}

/*
 * Check whether directory exists and whether we can open it. Keep errno set so
 * that the caller can report errors somewhat more accurately.
 */
static bool verify_directory(const char* directory)
{
    DIR* dir = opendir(directory);
    if (dir == NULL)
        return false;
    closedir(dir);
    return true;
}

/*
 * Split a pathname as dirname(1) and basename(1) would.
 *
 * XXX this probably doesn't do very well on Windows.  We probably need to
 * apply canonicalize_path(), at the very least.
 */
static void split_path(char* path, char** dir, char** fname)
{
    char* sep = NULL;

    /* split filepath into directory & filename */
    sep = strrchr(path, '/');

    /* directory path */
    if (sep != NULL) {
        *dir = strdup(path);
        (*dir)[(sep - path) + 1] = '\0'; /* no strndup */
        *fname = strdup(sep + 1);
    }
    /* local directory */
    else {
        *dir = NULL;
        *fname = strdup(path);
    }
}

/*
 * Try to find the file in several places:
 * if directory == NULL:
 *	 fname
 *	 XLOGDIR / fname
 *	 $PGDATA / XLOGDIR / fname
 * else
 *	 directory / fname
 *	 directory / XLOGDIR / fname
 *
 * return a read only fd
 */
static int fuzzy_open_file(const char* directory, const char* fname)
{
    int fd = -1;
    char fpath[MAXPGPATH];

    if (directory == NULL) {
        const char* datadir = NULL;

        /* fname */
        fd = open(fname, O_RDONLY | PG_BINARY, 0);
        if (fd < 0 && errno != ENOENT)
            return -1;
        else if (fd >= 0)
            return fd;

        /* XLOGDIR / fname */
        snprintf(fpath, MAXPGPATH, "%s/%s", XLOGDIR, fname);
        fd = open(fpath, O_RDONLY | PG_BINARY, 0);
        if (fd < 0 && errno != ENOENT)
            return -1;
        else if (fd >= 0)
            return fd;

        datadir = getenv("PGDATA");
        /* $PGDATA / XLOGDIR / fname */
        if (datadir != NULL) {
            snprintf(fpath, MAXPGPATH, "%s/%s/%s", datadir, XLOGDIR, fname);
            fd = open(fpath, O_RDONLY | PG_BINARY, 0);
            if (fd < 0 && errno != ENOENT)
                return -1;
            else if (fd >= 0)
                return fd;
        }
    } else {
        /* directory / fname */
        snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
        fd = open(fpath, O_RDONLY | PG_BINARY, 0);
        if (fd < 0 && errno != ENOENT)
            return -1;
        else if (fd >= 0)
            return fd;

        /* directory / XLOGDIR / fname */
        snprintf(fpath, MAXPGPATH, "%s/%s/%s", directory, XLOGDIR, fname);
        fd = open(fpath, O_RDONLY | PG_BINARY, 0);
        if (fd < 0 && errno != ENOENT)
            return -1;
        else if (fd >= 0)
            return fd;
    }
    return -1;
}

/*
 * write the full page to disk.
 */
static void XLogDumpTablePage(XLogReaderState* record, int block_id, RelFileNode rnode, BlockNumber blk)
{
    int fd = -1;
    int nbyte = 0;
    char block_path[MAXPGPATH] = {0};
    char* page = (char*)malloc(BLCKSZ);
    if (!page)
        fatal_error("out of memory");

    /* block path */
    snprintf(block_path, MAXPGPATH, "%d_%d", rnode.relNode, blk);

    fd = open(block_path, O_RDWR | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0)
        fatal_error("could not create file %s :%m", block_path);

    RestoreBlockImage(record->blocks[block_id].bkp_image,
        record->blocks[block_id].hole_offset,
        record->blocks[block_id].hole_length,
        page);

    nbyte = write(fd, page, BLCKSZ);
    if (nbyte != BLCKSZ)
        fatal_error("write file %s failed :%m", block_path);

    close(fd);
    free(page);
    printf(" write FPW page %s to disk", block_path);
}

// for dorado storage
static void XLogDumpReadSharedStorage(char* directory, XLogRecPtr startptr, long xlogSize, char* buf, Size count)
{
    char* p = buf;
    XLogRecPtr recptr;
    Size nbytes;

    static int sendFile = -1;
    static uint64 sendOff = 0;

    recptr = startptr;
    nbytes = count;

    while (nbytes > 0) {
        int segbytes;
        int readbytes;

        uint64 startoff = (recptr % xlogSize) + XLogSegSize;

        if (sendFile < 0) {
            canonicalize_path(directory);
            sendFile = open(directory, O_RDONLY | PG_BINARY, 0);

            if (sendFile < 0) {
                fatal_error("could not find file \"%s\": %s", directory, strerror(errno));
            }
            sendOff = 0;
        }

        /* Need to seek in the file? */
        if (sendOff != startoff) {
            if (lseek(sendFile, (off_t)startoff, SEEK_SET) < 0) {
                int err = errno;
                fatal_error("could not seek in log segment %s to offset %lu: %s", directory, startoff, strerror(err));
            }
            sendOff = startoff;
        }

        /* How many bytes are within this segment? */
        if (nbytes > (xlogSize - startoff)) {
            segbytes = xlogSize - startoff;
        } else {
            segbytes = nbytes;
        }
        readbytes = read(sendFile, p, segbytes);
        if (readbytes <= 0) {
            int err = errno;

            fatal_error("could not read from log segment %s, offset %ld, length %d: %s",
                directory,
                sendOff,
                segbytes,
                strerror(err));
        }

        /* Update state for read */
        XLByteAdvance(recptr, readbytes);

        sendOff += readbytes;
        nbytes -= readbytes;
        p += readbytes;
    }
}


/*
 * Read count bytes from a segment file in the specified directory, for the
 * given timeline, containing the specified record pointer; store the data in
 * the passed buffer.
 */
static void XLogDumpXLogRead(const char* directory, TimeLineID timeline_id, XLogRecPtr startptr, char* buf, Size count)
{
    char* p = NULL;
    XLogRecPtr recptr;
    Size nbytes;

    static int sendFile = -1;
    static XLogSegNo sendSegNo = 0;
    static uint32 sendOff = 0;

    p = buf;
    recptr = startptr;
    nbytes = count;

    while (nbytes > 0) {
        uint32 startoff;
        int segbytes;
        int readbytes;

        startoff = recptr % XLogSegSize;

        if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo)) {
            char fname[MAXFNAMELEN];

            /* Switch to another logfile segment */
            if (sendFile >= 0)
                close(sendFile);

            XLByteToSeg(recptr, sendSegNo);

            XLogFileName(fname, MAXFNAMELEN, timeline_id, sendSegNo);

            sendFile = fuzzy_open_file(directory, fname);

            if (sendFile < 0)
                fatal_error("could not find file \"%s\": %s", fname, strerror(errno));
            sendOff = 0;
        }

        /* Need to seek in the file? */
        if (sendOff != startoff) {
            if (lseek(sendFile, (off_t)startoff, SEEK_SET) < 0) {
                int err = errno;
                char fname[MAXPGPATH];

                XLogFileName(fname, MAXFNAMELEN, timeline_id, sendSegNo);

                fatal_error("could not seek in log segment %s to offset %u: %s", fname, startoff, strerror(err));
            }
            sendOff = startoff;
        }

        /* How many bytes are within this segment? */
        if (nbytes > (XLogSegSize - startoff))
            segbytes = XLogSegSize - startoff;
        else
            segbytes = nbytes;

        readbytes = read(sendFile, p, segbytes);
        if (readbytes <= 0) {
            int err = errno;
            char fname[MAXPGPATH];

            XLogFileName(fname, MAXFNAMELEN, timeline_id, sendSegNo);

            fatal_error("could not read from log segment %s, offset %d, length %d: %s",
                fname,
                sendOff,
                segbytes,
                strerror(err));
        }

        /* Update state for read */
        XLByteAdvance(recptr, readbytes);

        sendOff += readbytes;
        nbytes -= readbytes;
        p += readbytes;
    }
}

/*
 * XLogReader read_page callback
 */
static int XLogDumpReadPage(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetPtr,
    char* readBuff, TimeLineID* curFileTLI, char* xlog_path)
{
    XLogDumpPrivate* dumpprivate = (XLogDumpPrivate*)state->private_data;
    int count = XLOG_BLCKSZ;

    if (!XLByteEQ(dumpprivate->endptr, InvalidXLogRecPtr)) {
        int recptrdiff = XLByteDifference(dumpprivate->endptr, targetPagePtr);
        if (XLOG_BLCKSZ <= recptrdiff)
            count = XLOG_BLCKSZ;
        else if (reqLen <= recptrdiff)
            count = recptrdiff;
        else {
            dumpprivate->endptr_reached = true;
            return -1;
        }
    }

    if (dumpprivate->shareStorageXlogFilePath == NULL) {
        XLogDumpXLogRead(dumpprivate->inpath, dumpprivate->timeline, targetPagePtr, readBuff, count);
    } else {
        XLogDumpReadSharedStorage(dumpprivate->shareStorageXlogFilePath, targetPagePtr,
            dumpprivate->shareStorageXlogSize, readBuff, count);
    }

    return count;
}

/*
 * Store per-rmgr and per-record statistics for a given record.
 */
static void XLogDumpCountRecord(XLogDumpConfig* config, XLogDumpStats* stats, XLogReaderState* record)
{
    RmgrId rmid;
    uint8 recid;
    uint32 rec_len;
    uint32 fpi_len;
    int block_id;

    stats->count++;

    rmid = XLogRecGetRmid(record);
    rec_len = XLogRecGetDataLen(record) + SizeOfXLogRecord;

    /*
     * Calculate the amount of FPI data in the record. Each backup block
     * takes up BLCKSZ bytes, minus the "hole" length.
     *
     * XXX: We peek into xlogreader's private decoded backup blocks for the
     * hole_length. It doesn't seem worth it to add an accessor macro for
     * this.
     */
    fpi_len = 0;
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        if (XLogRecHasBlockImage(record, block_id))
            fpi_len += BLCKSZ - record->blocks[block_id].hole_length;
    }

    /* Update per-rmgr statistics */

    stats->rmgr_stats[rmid].count++;
    stats->rmgr_stats[rmid].rec_len += rec_len;
    stats->rmgr_stats[rmid].fpi_len += fpi_len;

    /*
     * Update per-record statistics, where the record is identified by a
     * combination of the RmgrId and the four bits of the xl_info field that
     * are the rmgr's domain (resulting in sixteen possible entries per
     * RmgrId).
     */

    recid = XLogRecGetInfo(record) >> 4;

    stats->record_stats[rmid][recid].count++;
    stats->record_stats[rmid][recid].rec_len += rec_len;
    stats->record_stats[rmid][recid].fpi_len += fpi_len;
}

const char* storage_type_names[] = {
    "HEAP DISK", /* Heap Disk */
    "SEGMENT PAGE", /* Segment Page */
    "Invalid Storage" /* Invalid Storage */
};

const char* virtual_fork_names[] = {
    "SEGMENT_EXT_8", /* segment extent 8 */
    "SEGMENT_EXT_128", /* segment extent 128 */
    "SEGMENT_EXT_1024", /* segment extent 1024 */
    "SEGMENT_EXT_8192"  /* segment extent 8192 */
};

static const char* XLogGetForkNames(ForkNumber forknum)
{
    return forkNames[forknum];
}

/*
 * Print a record to stdout
 */
static void XLogDumpDisplayRecord(XLogDumpConfig* config, XLogReaderState* record)
{
    const RmgrDescData* desc = &RmgrDescTable[XLogRecGetRmid(record)];
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blk;
    int block_id;
    XLogRecPtr lsn;
    XLogRecPtr xl_prev = XLogRecGetPrev(record);

    printf("REDO @ %X/%X; LSN %X/%X: prev %X/%X; xid " XID_FMT "; term %u; len %u; total %u; crc %u; "
           "desc: %s - ",
        (uint32)(record->ReadRecPtr >> 32),
        (uint32)record->ReadRecPtr,
        (uint32)(record->EndRecPtr >> 32),
        (uint32)record->EndRecPtr,
        (uint32)(xl_prev >> 32),
        (uint32)xl_prev,
        XLogRecGetXid(record),
        XLogRecGetTerm(record),
        XLogRecGetDataLen(record),
        XLogRecGetTotalLen(record),
        XLogRecGetCrc(record),
        desc->rm_name);

    /* the desc routine will printf the description directly to stdout */
    desc->rm_desc(NULL, record);


    /* print block references */
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        if (!XLogRecHasBlockRef(record, block_id))
            continue;

        XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk);
        XLogRecGetBlockLastLsn(record, block_id, &lsn);

        uint8 seg_fileno;
        BlockNumber seg_blockno;
        XLogRecGetPhysicalBlock(record, block_id, &seg_fileno, &seg_blockno);
        
        // output format: ", blkref #%u: rel %u/%u/%u/%d storage %s fork %s blk %u (phy loc %u/%u) lastlsn %X/%X"
        printf(", blkref #%u: rel %u/%u/%u", block_id, rnode.spcNode, rnode.dbNode, rnode.relNode);
        if (IsBucketFileNode(rnode)) {
            printf("/%d", rnode.bucketNode);
        }
        StorageType storage_type = HEAP_DISK;
        if (IsSegmentFileNode(rnode)) {
            storage_type = SEGMENT_PAGE;
        }
        printf(" storage %s", storage_type_names[storage_type]);
        if (forknum != MAIN_FORKNUM) {
            printf(" fork %s", XLogGetForkNames(forknum));
        }
        printf(" blk %u", blk);
        if (seg_fileno != EXTENT_INVALID) {
            printf(" (phy loc %u/%u)", EXTENT_TYPE_TO_SIZE(BKPBLOCK_GET_SEGFILENO(seg_fileno)), seg_blockno);
            if (record->blocks[block_id].has_vm_loc) {
                printf(" (vm phy loc %u/%u)", EXTENT_TYPE_TO_SIZE(record->blocks[block_id].vm_seg_fileno),
                    record->blocks[block_id].vm_seg_blockno);
            }
        }
        printf(" lastlsn %X/%X", (uint32)(lsn >> 32), (uint32)lsn);
        if (XLogRecHasBlockImage(record, block_id)) {
            if (config->bkp_details) {
                printf(" (FPW); hole: offset: %u, length: %u",
                    record->blocks[block_id].hole_offset,
                    record->blocks[block_id].hole_length);

                if (config->write_fpw)
                    XLogDumpTablePage(record, block_id, rnode, blk);
            } else {
                printf(" FPW");
            }
        }
    }
    putchar('\n');
    

    if (config->verbose) {
        printf("\tSYSID " UINT64_FORMAT "; record_origin %u; max_block_id %u; readSegNo " UINT64_FORMAT "; readOff %u; "
               "readPageTLI %u; curReadSegNo " UINT64_FORMAT "; curReadOff %u "
               "latestPagePtr %X/%X; latestPageTLI %u; currRecPtr %X/%X",
            record->system_identifier,
            record->record_origin,
            record->max_block_id,
            record->readSegNo,
            record->readOff,
            record->readPageTLI,
            record->curReadSegNo,
            record->curReadOff,
            (uint32)(record->latestPagePtr >> 32),
            (uint32)record->latestPagePtr,
            record->latestPageTLI,
            (uint32)(record->currRecPtr >> 32),
            (uint32)record->currRecPtr);

        putchar('\n');

        XLogDumpPrivate* dumpprivate = (XLogDumpPrivate*)record->private_data;
        if (dumpprivate != NULL) {
            printf("\tPRIVATE @%X/%X-%X/%X; TLI %u; endptr_reached %d",
                (uint32)(dumpprivate->startptr >> 32),
                (uint32)dumpprivate->startptr,
                (uint32)(dumpprivate->endptr >> 32),
                (uint32)dumpprivate->endptr,
                dumpprivate->timeline,
                dumpprivate->endptr_reached);
            putchar('\n');
        }

        printf("\tMAINDATA main_data_len %u; main_data_bufsz %u", record->main_data_len, record->main_data_bufsz);
        putchar('\n');
    }
}

/*
 * Display a single row of record counts and sizes for an rmgr or record.
 */
static void XLogDumpStatsRow(const char* name, uint64 n, uint64 total_count, uint64 rec_len, uint64 total_rec_len,
    uint64 fpi_len, uint64 total_fpi_len, uint64 tot_len, uint64 total_len)
{
    double n_pct, rec_len_pct, fpi_len_pct, tot_len_pct;

    n_pct = 0;
    if (total_count != 0)
        n_pct = 100 * (double)n / total_count;

    rec_len_pct = 0;
    if (total_rec_len != 0)
        rec_len_pct = 100 * (double)rec_len / total_rec_len;

    fpi_len_pct = 0;
    if (total_fpi_len != 0)
        fpi_len_pct = 100 * (double)fpi_len / total_fpi_len;

    tot_len_pct = 0;
    if (total_len != 0)
        tot_len_pct = 100 * (double)tot_len / total_len;

    printf("%-27s "
           "%20" PRIu64 " (%6.02f) "
           "%20" PRIu64 " (%6.02f) "
           "%20" PRIu64 " (%6.02f) "
           "%20" PRIu64 " (%6.02f)\n",
        name,
        n,
        n_pct,
        rec_len,
        rec_len_pct,
        fpi_len,
        fpi_len_pct,
        tot_len,
        tot_len_pct);
}

/*
 * Display summary statistics about the records seen so far.
 */
static void XLogDumpDisplayStats(XLogDumpConfig* config, XLogDumpStats* stats)
{
    int ri;
    uint64 total_count = 0;
    uint64 total_rec_len = 0;
    uint64 total_fpi_len = 0;
    uint64 total_len = 0;
    char total_rec_percent[32] = {0};
    char total_fpi_percent[32] = {0};
    double rec_len_pct, fpi_len_pct;

    /* ---
     * Make a first pass to calculate column totals:
     * count(*),
     * sum(xl_len+SizeOfXLogRecord),
     * sum(xl_tot_len-xl_len-SizeOfXLogRecord), and
     * sum(xl_tot_len).
     * These are used to calculate percentages for each record type.
     * ---
     */

    for (ri = 0; ri < RM_NEXT_ID; ri++) {
        total_count += stats->rmgr_stats[ri].count;
        total_rec_len += stats->rmgr_stats[ri].rec_len;
        total_fpi_len += stats->rmgr_stats[ri].fpi_len;
    }
    total_len = total_rec_len + total_fpi_len;

    /*
     * 27 is strlen("Transaction/COMMIT_PREPARED"),
     * 20 is strlen(2^64), 8 is strlen("(100.00%)")
     */

    printf("%-27s %20s %8s %20s %8s %20s %8s %20s %8s\n"
           "%-27s %20s %8s %20s %8s %20s %8s %20s %8s\n",
        "Type",
        "N",
        "(%)",
        "Record size",
        "(%)",
        "FPI size",
        "(%)",
        "Combined size",
        "(%)",
        "----",
        "-",
        "---",
        "-----------",
        "---",
        "--------",
        "---",
        "-------------",
        "---");

    for (ri = 0; ri < RM_NEXT_ID; ri++) {
        uint64 count, rec_len, fpi_len, tot_len;
        const RmgrDescData* desc = &RmgrDescTable[ri];

        count = stats->rmgr_stats[ri].count;
        rec_len = stats->rmgr_stats[ri].rec_len;
        fpi_len = stats->rmgr_stats[ri].fpi_len;
        tot_len = rec_len + fpi_len;

        XLogDumpStatsRow(
            desc->rm_name, count, total_count, rec_len, total_rec_len, fpi_len, total_fpi_len, tot_len, total_len);
    }

    printf("%-27s %20s %8s %20s %8s %20s %8s %20s\n", "", "--------", "", "--------", "", "--------", "", "--------");

    /*
     * The percentages in earlier rows were calculated against the
     * column total, but the ones that follow are against the row total.
     * Note that these are displayed with a % symbol to differentiate
     * them from the earlier ones, and are thus up to 9 characters long.
     */

    rec_len_pct = 0;
    if (total_len != 0)
        rec_len_pct = 100 * (double)total_rec_len / total_len;

    fpi_len_pct = 0;
    if (total_len != 0)
        fpi_len_pct = 100 * (double)total_fpi_len / total_len;

    snprintf(total_rec_percent, 32, "[%.02f%%]", rec_len_pct);
    snprintf(total_fpi_percent, 32, "[%.02f%%]", fpi_len_pct);

    printf("%-27s "
           "%20" PRIu64 " %-9s"
           "%20" PRIu64 " %-9s"
           "%20" PRIu64 " %-9s"
           "%20" PRIu64 " %-6s\n",
        "Total",
        stats->count,
        "",
        total_rec_len,
        total_rec_percent,
        total_fpi_len,
        total_fpi_percent,
        total_len,
        "[100%]");
}

static void usage(void)
{
    printf("%s decodes and displays openGauss transaction logs for debugging.\n\n", progname);
    printf("Usage:\n");
    printf("  %s [OPTION]... [STARTSEG [ENDSEG]] \n", progname);
    printf("\nOptions:\n");
    printf("  -b, --bkp-details      output detailed information about backup blocks\n");
    printf("  -e, --end=RECPTR       stop reading at log position RECPTR\n");
    printf("  -f, --follow           keep retrying after reaching end of WAL\n");
    printf("  -n, --limit=N          number of records to display\n");
    printf("  -p, --path=PATH        directory in which to find log segment files\n");
    printf("                         (default: ./pg_xlog)\n");
    printf("  -r, --rmgr=RMGR        only show records generated by resource manager RMGR\n");
    printf("                         use --rmgr=list to list valid resource manager names\n");
    printf("  -s, --start=RECPTR     start reading at log position RECPTR\n");
    printf("  -S, --size=n           for share storage, the length of xlog file size(not include ctl info length)\n"); 
    printf("                         default: 512*1024*1024*1024(512GB)\n");
    printf("  -t, --timeline=TLI     timeline from which to read log records\n");
    printf("                         (default: 1 or the value used in STARTSEG)\n");
    printf("  -V, --version          output version information, then exit\n");
    printf("  -w, --write-FPW        write the full page to disk, should enable -b\n");
    printf("  -x, --xid=XID          only show records with TransactionId XID\n");
    printf("  -z, --stats            show statistics instead of records\n");
    printf("  -v, --verbose          show detailed information\n");
    printf("  -?, --help             show this help, then exit\n");
}

int main(int argc, char** argv)
{
    uint32 hi = 0;
    uint32 lo = 0;
    XLogReaderState* xlogreader_state = NULL;
    XLogDumpPrivate dumpprivate;
    XLogDumpConfig config;
    XLogDumpStats stats;
    XLogRecord* record = NULL;
    XLogRecPtr first_record;
    char* errormsg = NULL;

    static struct option long_options[] = {{"bkp-details", no_argument, NULL, 'b'},
        {"end", required_argument, NULL, 'e'},
        {"follow", no_argument, NULL, 'f'},
        {"help", no_argument, NULL, '?'},
        {"limit", required_argument, NULL, 'n'},
        {"path", required_argument, NULL, 'p'},
        {"rmgr", required_argument, NULL, 'r'},
        {"start", required_argument, NULL, 's'},
        {"timeline", required_argument, NULL, 't'},
        {"write-fpw", no_argument, NULL, 'w'},
        {"xid", required_argument, NULL, 'x'},
        {"size", required_argument, NULL, 'S'},
        {"version", no_argument, NULL, 'V'},
        {"verbose", no_argument, NULL, 'v'},
        {"stats", no_argument, NULL, 'z'},
        {NULL, 0, NULL, 0}};

    int option;
    int optindex = 0;

    progname = get_progname(argv[0]);

    memset(&dumpprivate, 0, sizeof(XLogDumpPrivate));
    memset(&config, 0, sizeof(XLogDumpConfig));
    memset(&stats, 0, sizeof(XLogDumpStats));

    dumpprivate.timeline = 1;
    dumpprivate.startptr = InvalidXLogRecPtr;
    dumpprivate.endptr = InvalidXLogRecPtr;
    dumpprivate.endptr_reached = false;
    dumpprivate.shareStorageXlogFilePath = NULL;
    const long defaultShareStorageXlogSize = 512 * 1024 * 1024 * 1024L;
    dumpprivate.shareStorageXlogSize = defaultShareStorageXlogSize;

    config.bkp_details = false;
    config.write_fpw = false;
    config.stop_after_records = -1;
    config.already_displayed_records = 0;
    config.follow = false;
    config.filter_by_rmgr = -1;
    config.filter_by_xid = InvalidTransactionId;
    config.filter_by_xid_enabled = false;
    config.stats = false;
    config.verbose = false;

    if (argc <= 1) {
        fprintf(stderr, "%s: no arguments specified\n", progname);
        goto bad_argument;
    }

    while ((option = getopt_long(argc, argv, "be:?fn:p:r:s:S:t:Vvwx:z", long_options, &optindex)) != -1) {
        switch (option) {
            case 'b':
                config.bkp_details = true;
                break;
            case 'e':
                if (sscanf(optarg, "%X/%X", &hi, &lo) != 2) {
                    fprintf(stderr, "%s: could not parse end log position \"%s\"\n", progname, optarg);
                    goto bad_argument;
                }
                dumpprivate.endptr = (((uint64)hi) << 32) | lo;
                break;
            case 'f':
                config.follow = true;
                break;
            case '?':
                usage();
                exit(EXIT_SUCCESS);
                break;
            case 'n':
                if (sscanf(optarg, "%d", &config.stop_after_records) != 1) {
                    fprintf(stderr, "%s: could not parse limit \"%s\"\n", progname, optarg);
                    goto bad_argument;
                }
                break;
            case 'p':
                dumpprivate.inpath = strdup(optarg);
                break;
            case 'r': {
                int i = 0;

                if (pg_strcasecmp(optarg, "list") == 0) {
                    print_rmgr_list();
                    exit(EXIT_SUCCESS);
                }

                for (i = 0; i <= RM_MAX_ID; i++) {
                    if (pg_strcasecmp(optarg, RmgrDescTable[i].rm_name) == 0) {
                        config.filter_by_rmgr = i;
                        break;
                    }
                }

                if (config.filter_by_rmgr == -1) {
                    fprintf(stderr, "%s: resource manager \"%s\" does not exist\n", progname, optarg);
                    goto bad_argument;
                }
            } break;
            case 's':
                if (sscanf(optarg, "%X/%X", &hi, &lo) != 2) {
                    fprintf(stderr, "%s: could not parse start log position \"%s\"\n", progname, optarg);
                    goto bad_argument;
                }
                dumpprivate.startptr = (((uint64)hi) << 32) | lo;
                break;
            case 'S':
                dumpprivate.shareStorageXlogSize = atol(optarg);
                if (dumpprivate.shareStorageXlogSize == 0) {
                    fprintf(stderr, "%s: could not parse share storage xlog size \"%s\"\n", progname, optarg);
                    goto bad_argument;
                }
                break;
            case 't':
                if (sscanf(optarg, "%d", &dumpprivate.timeline) != 1) {
                    fprintf(stderr, "%s: could not parse timeline \"%s\"\n", progname, optarg);
                    goto bad_argument;
                }
                break;
            case 'V':
                puts("pg_xlogdump (PostgreSQL) " PG_VERSION);
                exit(EXIT_SUCCESS);
                break;
            case 'v':
                config.verbose = true;
                break;
            case 'w':
                config.write_fpw = true;
                break;
            case 'x':
                if (sscanf(optarg, XID_FMT, &config.filter_by_xid) != 1) {
                    fprintf(stderr, "%s: could not parse \"%s\" as a valid xid\n", progname, optarg);
                    goto bad_argument;
                }
                config.filter_by_xid_enabled = true;
                break;
            case 'z':
                config.stats = true;
                break;
            default:
                goto bad_argument;
        }
    }

    if ((optind + 2) < argc) {
        fprintf(stderr, "%s: too many command-line arguments (first is \"%s\")\n", progname, argv[optind + 2]);
        goto bad_argument;
    }

    if (dumpprivate.inpath != NULL) {
        /* validate path points to directory */
        if (!verify_directory(dumpprivate.inpath)) {
            fprintf(stderr, "%s: path \"%s\" cannot be opened: %s\n", progname, dumpprivate.inpath, strerror(errno));
            goto bad_argument;
        }
    }

    /* parse files as start/end boundaries, extract path if not specified */
    if (optind < argc) {
        char* directory = NULL;
        char* fname = NULL;
        int fd;
        XLogSegNo targetsegno;

        split_path(argv[optind], &directory, &fname);

        if (strspn(fname, "0123456789ABCDEFabcdef") != strlen(fname)) {
            dumpprivate.shareStorageXlogFilePath = strdup(argv[optind]);
            goto begin_read;
        }

        if (dumpprivate.inpath == NULL && directory != NULL) {
            dumpprivate.inpath = directory;

            if (!verify_directory(dumpprivate.inpath))
                fatal_error("cannot open directory \"%s\": %s", dumpprivate.inpath, strerror(errno));
        }

        fd = fuzzy_open_file(dumpprivate.inpath, fname);
        if (fd < 0)
            fatal_error("could not open file \"%s\"", fname);
        close(fd);

        /* parse position from file */
        XLogFromFileName(fname, &dumpprivate.timeline, &targetsegno);

        if (XLogRecPtrIsInvalid(dumpprivate.startptr)) {
            XLogSegNoOffsetToRecPtr(targetsegno, 0, dumpprivate.startptr);
        } else if (!XLByteInSeg(dumpprivate.startptr, targetsegno)) {
            fprintf(stderr,
                "%s: start log position %X/%X is not inside file \"%s\"\n",
                progname,
                (uint32)(dumpprivate.startptr >> 32),
                (uint32)dumpprivate.startptr,
                fname);
            goto bad_argument;
        }

        /* no second file specified, set end position */
        if (!(optind + 1 < argc) && XLogRecPtrIsInvalid(dumpprivate.endptr)) {
            XLogSegNoOffsetToRecPtr(targetsegno + 1, 0, dumpprivate.endptr);
        }

        /* parse ENDSEG if passed */
        if (optind + 1 < argc) {
            XLogSegNo endsegno;

            /* ignore directory, already have that */
            split_path(argv[optind + 1], &directory, &fname);

            fd = fuzzy_open_file(dumpprivate.inpath, fname);
            if (fd < 0)
                fatal_error("could not open file \"%s\"", fname);
            close(fd);

            /* parse position from file */
            XLogFromFileName(fname, &dumpprivate.timeline, &endsegno);

            if (endsegno < targetsegno)
                fatal_error("ENDSEG %s is before STARTSEG %s", argv[optind + 1], argv[optind]);

            if (XLogRecPtrIsInvalid(dumpprivate.endptr)) {
                XLogSegNoOffsetToRecPtr(endsegno + 1, 0, dumpprivate.endptr);
            }

            /* set segno to endsegno for check of --end */
            targetsegno = endsegno;
        }

        bool reachEnd = !XLByteInSeg(dumpprivate.endptr, targetsegno) &&
            (dumpprivate.endptr != (targetsegno + 1) * XLogSegSize);
        if (reachEnd) {
            fprintf(stderr,
                "%s: end log position %X/%X is not inside file \"%s\"\n",
                progname,
                (uint32)(dumpprivate.endptr >> 32),
                (uint32)dumpprivate.endptr,
                argv[argc - 1]);
            goto bad_argument;
        }
    }

begin_read:
    /* we don't know what to print */
    if (XLogRecPtrIsInvalid(dumpprivate.startptr)) {
        fprintf(stderr, "%s: no start log position given.\n", progname);
        goto bad_argument;
    }

    /* done with argument parsing, do the actual work */

    /* we have everything we need, start reading */
    xlogreader_state = XLogReaderAllocate(XLogDumpReadPage, &dumpprivate);
    if (!xlogreader_state)
        fatal_error("out of memory");

    /* first find a valid recptr to start from */
    first_record = XLogFindNextRecord(xlogreader_state, dumpprivate.startptr);

    if (XLByteEQ(first_record, InvalidXLogRecPtr))
        fatal_error("could not find a valid record after %X/%X",
            (uint32)(dumpprivate.startptr >> 32),
            (uint32)dumpprivate.startptr);

    /*
     * Display a message that we're skipping data if `from` wasn't a pointer to
     * the start of a record and also wasn't a pointer to the beginning of a
     * segment (e.g. we were used in file mode).
     */
    if (!XLByteEQ(first_record, dumpprivate.startptr) && (dumpprivate.startptr % XLogSegSize) != 0)
        printf("first record is after %X/%X, at %X/%X, skipping over %lu bytes\n",
            (uint32)(dumpprivate.startptr >> 32),
            (uint32)dumpprivate.startptr,
            (uint32)(first_record >> 32),
            (uint32)first_record,
            XLByteDifference(first_record, dumpprivate.startptr));

    for (;;) {
        /* try to read the next record */
        record = XLogReadRecord(xlogreader_state, first_record, &errormsg);
        if (!record) {
            if (!config.follow || dumpprivate.endptr_reached)
                break;
            else {
                pg_usleep(1000000L); /* 1 second */
                continue;
            }
        }

        /* after reading the first record, continue at next one */
        first_record = InvalidXLogRecPtr;

        /* apply all specified filters */
        if (config.filter_by_rmgr != -1 && config.filter_by_rmgr != record->xl_rmid)
            continue;

        if (config.filter_by_xid_enabled && config.filter_by_xid != record->xl_xid)
            continue;

        /* process the record */
        if (config.stats == true)
            XLogDumpCountRecord(&config, &stats, xlogreader_state);
        else
            XLogDumpDisplayRecord(&config, xlogreader_state);

        /* check whether we printed enough */
        config.already_displayed_records++;
        if (config.stop_after_records > 0 && config.already_displayed_records >= config.stop_after_records)
            break;
    }

    if (config.stats == true)
        XLogDumpDisplayStats(&config, &stats);

    if (errormsg)
        fatal_error("error in WAL record at %X/%X: %s\n",
            (uint32)(xlogreader_state->ReadRecPtr >> 32),
            (uint32)xlogreader_state->ReadRecPtr,
            errormsg);

    XLogReaderFree(xlogreader_state);

    return EXIT_SUCCESS;

bad_argument:
    fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
    return EXIT_FAILURE;
}
