/*-------------------------------------------------------------------------
 *
 * pg_probackup.h: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROBACKUPB_H
#define PG_PROBACKUPB_H

/* Information about single file (or dir) in backup */
typedef struct pgFile_t
{
    char   *name;            /* file or directory name */
    mode_t    mode;            /* protection (file type and permission) */
    size_t    size;            /* size of the file */
    time_t  mtime;            /* file st_mtime attribute, can be used only
                                during backup */
    size_t    read_size;        /* size of the portion read (if only some pages are
                               backed up, it's different from size) */
    int64    write_size;        /* size of the backed-up file. BYTES_INVALID means
                               that the file existed but was not backed up
                               because not modified since last backup. */
    size_t    uncompressed_size;    /* size of the backed-up file before compression
                                 * and adding block headers.
                                 */
                            /* we need int64 here to store '-1' value */
    pg_crc32 crc;            /* CRC value of the file, regular file only */
    char   *rel_path;        /* relative path of the file */
    char   *linked;            /* path of the linked file */
    bool    is_datafile;    /* true if the file is PostgreSQL data file */
    bool compressedFile;    /* true if the file is the openGauss compressed file */
    uint16 compressedChunkSize;       /* chunk size of compressed file */
    uint8 compressedAlgorithm;         /* algorithm of comrpessed file */
    Oid        tblspcOid;        /* tblspcOid extracted from path, if applicable */
    Oid        dbOid;            /* dbOid extracted from path, if applicable */
    Oid        relOid;            /* relOid extracted from path, if applicable */
    ForkName   forkName;    /* forkName extracted from path, if applicable */
    int        segno;            /* Segment number for ptrack */
    int        n_blocks;        /* number of blocks in the data file in data directory */
    bool    is_cfs;            /* Flag to distinguish files compressed by CFS*/
    bool    is_database;    /* Flag used strictly by ptrack 1.x backup */
    int        external_dir_num;    /* Number of external directory. 0 if not external */
    bool    exists_in_prev;        /* Mark files, both data and regular, that exists in previous backup */
    CompressAlg        compress_alg;        /* compression algorithm applied to the file */
    volatile         pg_atomic_flag lock;/* lock for synchronization of parallel threads  */
    datapagemap_t    pagemap;            /* bitmap of pages updated since previous backup
                                           may take up to 16kB per file */
    bool            pagemap_isabsent;    /* Used to mark files with unknown state of pagemap,
                                         * i.e. datafiles without _ptrack */
    /* Coordinates in header map */
    int      n_headers;        /* number of blocks in the data file in backup */
    pg_crc32 hdr_crc;        /* CRC value of header file: name_hdr */
    off_t    hdr_off;       /* offset in header map */
    int      hdr_size;       /* offset in header map */
} pgFile;

typedef struct page_map_entry
{
    const char    *path;        /* file or directory name */
    char        *pagemap;
    size_t         pagemapsize;
} page_map_entry;

/* Special values of datapagemap_t bitmapsize */
#define PageBitmapIsEmpty 0        /* Used to mark unchanged datafiles */

/* Current state of backup */
typedef enum BackupStatus
{
    BACKUP_STATUS_INVALID,        /* the pgBackup is invalid */
    BACKUP_STATUS_OK,            /* completed backup */
    BACKUP_STATUS_ERROR,        /* aborted because of unexpected error */
    BACKUP_STATUS_RUNNING,        /* running backup */
    BACKUP_STATUS_MERGING,        /* merging backups */
    BACKUP_STATUS_MERGED,        /* backup has been successfully merged and now awaits
                                 * the assignment of new start_time */
    BACKUP_STATUS_DELETING,        /* data files are being deleted */
    BACKUP_STATUS_DELETED,        /* data files have been deleted */
    BACKUP_STATUS_DONE,            /* completed but not validated yet */
    BACKUP_STATUS_ORPHAN,        /* backup validity is unknown but at least one parent backup is corrupted */
    BACKUP_STATUS_CORRUPT        /* files are corrupted, not available */
} BackupStatus;

typedef enum BackupMode
{
    BACKUP_MODE_INVALID = 0,
    BACKUP_MODE_DIFF_PTRACK,    /* incremental page backup with ptrack system */
    BACKUP_MODE_FULL            /* full backup */
} BackupMode;

typedef enum ShowFormat
{
    SHOW_PLAIN,
    SHOW_JSON
} ShowFormat;

/* special values of pgBackup fields */
#define INVALID_BACKUP_ID    0    /* backup ID is not provided by user */
#define BYTES_INVALID        (-1) /* file didn`t changed since previous backup, DELTA backup do not rely on it */
#define FILE_NOT_FOUND        (-2) /* file disappeared during backup */
#define BLOCKNUM_INVALID    (-1)
#define PROGRAM_VERSION    "2.4.2"
#define AGENT_PROTOCOL_VERSION 20402

typedef struct ConnectionOptions
{
    const char *pgdatabase;
    const char *pghost;
    const char *pgport;
    const char *pguser;
} ConnectionOptions;

typedef struct ConnectionArgs
{
    PGconn       *conn;
    PGcancel   *cancel_conn;
} ConnectionArgs;

/* Store values for --remote-* option for 'restore_command' constructor */
typedef struct ArchiveOptions
{
    const char *host;
    const char *port;
    const char *user;
} ArchiveOptions;

/*
 * An instance configuration. It can be stored in a configuration file or passed
 * from command line.
 */
typedef struct InstanceConfig
{
    char        *name;
    char        arclog_path[MAXPGPATH];
    char        backup_instance_path[MAXPGPATH];

    uint64        system_identifier;
    uint32        xlog_seg_size;

    char       *pgdata;
    char       *external_dir_str;

    ConnectionOptions conn_opt;

    /* Wait timeout for WAL segment archiving */
    uint32        archive_timeout;

    /* cmdline to be used as restore_command */
    char       *restore_command;

    /* Logger parameters */
    LoggerConfig logger;

    /* Remote access parameters */
    RemoteConfig remote;

    /* Retention options. 0 disables the option. */
    uint32        retention_redundancy;
    uint32        retention_window;
    uint32        wal_depth;

    CompressAlg    compress_alg;
    int            compress_level;

    /* Archive description */
    ArchiveOptions archive;
} InstanceConfig;

extern ConfigOption instance_options[];
extern InstanceConfig instance_config;
extern time_t current_time;

typedef struct PGNodeInfo
{
    uint32            block_size;
    uint32            wal_block_size;
    uint32            checksum_version;
    bool            is_superuser;
    bool            pgpro_support;

    int                server_version;
    char            server_version_str[100];
} PGNodeInfo;

/* structure used for access to block header map */
typedef struct HeaderMap
{
    char  path[MAXPGPATH];
    char  path_tmp[MAXPGPATH]; /* used only in merge */
    FILE  *fp;                 /* used only for writing */
    char  *buf;                   /* buffer */
    off_t  offset;             /* current position in fp */
    pthread_mutex_t mutex;

} HeaderMap;

typedef struct pgBackup pgBackup;

/* Information about single backup stored in backup.conf */
struct pgBackup
{
    BackupMode        backup_mode; /* Mode - one of BACKUP_MODE_xxx above*/
    time_t            backup_id;     /* Identifier of the backup.
                                  * Currently it's the same as start_time */
    BackupStatus    status;        /* Status - one of BACKUP_STATUS_xxx above*/
    TimeLineID        tli;         /* timeline of start and stop backup lsns */
    XLogRecPtr        start_lsn;    /* backup's starting transaction log location */
    XLogRecPtr        stop_lsn;    /* backup's finishing transaction log location */
    time_t            start_time;    /* since this moment backup has status
                                 * BACKUP_STATUS_RUNNING */
    time_t            merge_dest_backup;    /* start_time of incremental backup,
                                     * this backup is merging with.
                                     * Only available for FULL backups
                                     * with MERGING or MERGED statuses */
    time_t            merge_time; /* the moment when merge was started or 0 */
    time_t            end_time;    /* the moment when backup was finished, or the moment
                                 * when we realized that backup is broken */
    time_t            recovery_time;    /* Earliest moment for which you can restore
                                     * the state of the database cluster using
                                     * this backup */
    time_t            expire_time;    /* Backup expiration date */
    TransactionId    recovery_xid;    /* Earliest xid for which you can restore
                                     * the state of the database cluster using
                                     * this backup */
    /*
     * Amount of raw data. For a full backup, this is the total amount of
     * data while for a differential backup this is just the difference
     * of data taken.
     * BYTES_INVALID means nothing was backed up.
     */
    int64            data_bytes;
    /* Size of WAL files needed to replay on top of this
     * backup to reach the consistency.
     */
    int64            wal_bytes;
    /* Size of data files before applying compression and block header,
     * WAL files are not included.
     */
    int64            uncompressed_bytes;

    /* Size of data files in PGDATA at the moment of backup. */
    int64            pgdata_bytes;

    CompressAlg        compress_alg;
    int                compress_level;

    /* Fields needed for compatibility check */
    uint32            block_size;
    uint32            wal_block_size;
    uint32            checksum_version;
    char            program_version[100];
    char            server_version[100];

    bool            stream;            /* Was this backup taken in stream mode?
                                     * i.e. does it include all needed WAL files? */
    bool			from_replica; /* Was this backup taken from replica */
    time_t            parent_backup;     /* Identifier of the previous backup.
                                     * Which is basic backup for this
                                     * incremental backup. */
    pgBackup        *parent_backup_link;
    char            *external_dir_str;    /* List of external directories,
                                         * separated by ':' */
    char            *root_dir;        /* Full path for root backup directory:
                                       backup_path/instance_name/backup_id */
    char            *database_dir;    /* Full path to directory with data files:
                                       backup_path/instance_name/backup_id/database */
    parray            *files;            /* list of files belonging to this backup
                                     * must be populated explicitly */
    char            *note;

    char            recovery_name[100];

    pg_crc32         content_crc;

    /* map used for access to page headers */
    HeaderMap       hdr_map;
};

/* Recovery target for restore and validate subcommands */
typedef struct pgRecoveryTarget
{
    time_t            target_time;
    /* add one more field in order to avoid deparsing target_time back */
    const char       *time_string;
    TransactionId    target_xid;
    /* add one more field in order to avoid deparsing target_xid back */
    const char       *xid_string;
    XLogRecPtr        target_lsn;
    /* add one more field in order to avoid deparsing target_lsn back */
    const char       *lsn_string;
    TimeLineID        target_tli;
    bool            target_inclusive;
    bool            inclusive_specified;
    const char       *target_stop;
    const char       *target_name;
    const char       *target_action;
} pgRecoveryTarget;

/* Options needed for restore and validate commands */
typedef struct pgRestoreParams
{
    bool    force;
    bool    is_restore;
    bool    no_validate;
    bool    skip_external_dirs;
    bool    skip_block_validation; //Start using it
    const char *restore_command;

    /* options for incremental restore */
    IncrRestoreMode    incremental_mode;
    XLogRecPtr shift_lsn;
} pgRestoreParams;

/* Options needed for set-backup command */
typedef struct pgSetBackupParams
{
    int64   ttl; /* amount of time backup must be pinned
                  * -1 - do nothing
                  * 0 - disable pinning
                  */
    time_t  expire_time; /* Point in time until backup
                          * must be pinned.
                          */
    char   *note;
} pgSetBackupParams;

typedef struct
{
    PGNodeInfo *nodeInfo;

    const char *from_root;
    const char *to_root;
    const char *external_prefix;

    parray       *files_list;
    parray       *prev_filelist;
    parray       *external_dirs;
    XLogRecPtr    prev_start_lsn;

    ConnectionArgs conn_arg;
    int            thread_num;
    HeaderMap   *hdr_map;

    /*
     * Return value from the thread.
     * 0 means there is no error, 1 - there is an error.
     */
    int            ret;
} backup_files_arg;

typedef struct timelineInfo timelineInfo;

/* struct to collect info about timelines in WAL archive */
struct timelineInfo {

    TimeLineID tli;            /* this timeline */
    TimeLineID parent_tli;  /* parent timeline. 0 if none */
    timelineInfo *parent_link; /* link to parent timeline */
    XLogRecPtr switchpoint;       /* if this timeline has a parent, then
                                * switchpoint contains switchpoint LSN,
                                * otherwise 0 */
    XLogSegNo begin_segno;    /* first present segment in this timeline */
    XLogSegNo end_segno;    /* last present segment in this timeline */
    size_t    n_xlog_files;    /* number of segments (only really existing)
                             * does not include lost segments */
    size_t    size;            /* space on disk taken by regular WAL files */
    parray *backups;        /* array of pgBackup sturctures with info
                             * about backups belonging to this timeline */
    parray *xlog_filelist;    /* array of ordinary WAL segments, '.partial'
                             * and '.backup' files belonging to this timeline */
    parray *lost_segments;    /* array of intervals of lost segments */
    parray *keep_segments;    /* array of intervals of segments used by WAL retention */
    pgBackup *closest_backup; /* link to valid backup, closest to timeline */
    pgBackup *oldest_backup; /* link to oldest backup on timeline */
    XLogRecPtr anchor_lsn; /* LSN belonging to the oldest segno to keep for 'wal-depth' */
    TimeLineID anchor_tli;    /* timeline of anchor_lsn */
};

typedef struct xlogInterval
{
    XLogSegNo begin_segno;
    XLogSegNo end_segno;
} xlogInterval;

typedef struct lsnInterval
{
    TimeLineID tli;
    XLogRecPtr begin_lsn;
    XLogRecPtr end_lsn;
} lsnInterval;

typedef enum xlogFileType
{
    SEGMENT,
    TEMP_SEGMENT,
    PARTIAL_SEGMENT,
    BACKUP_HISTORY_FILE
} xlogFileType;

typedef struct xlogFile
{
    pgFile       file;
    XLogSegNo    segno;
    xlogFileType type;
    bool         keep; /* Used to prevent removal of WAL segments
                        * required by ARCHIVE backups. */
} xlogFile;

/*
 * When copying datafiles to backup we validate and compress them block
 * by block. Thus special header is required for each data block.
 */
typedef struct BackupPageHeader
{
    BlockNumber    block;            /* block number */
    int32        compressed_size;
} BackupPageHeader;

/* 4MB for 1GB file */
typedef struct BackupPageHeader2
{
    XLogRecPtr  lsn;
    int32        block;             /* block number */
    int32       pos;             /* position in backup file */
    uint16      checksum;
} BackupPageHeader2;

/* Special value for compressed_size field */
#define PageIsOk         0
#define SkipCurrentPage -1
#define PageIsTruncated -2
#define PageIsCorrupted -3 /* used by checkdb */

/*
 * return pointer that exceeds the length of prefix from character string.
 * ex. str="/xxx/yyy/zzz", prefix="/xxx/yyy", return="zzz".
 *
 * Deprecated. Do not use this in new code.
 */
#define GetRelativePath(str, prefix) \
    ((strlen(str) <= strlen(prefix)) ? "" : str + strlen(prefix) + 1)

/*
 * Return timeline, xlog ID and record offset from an LSN of the type
 * 0/B000188, usual result from pg_stop_backup() and friends.
 */
#define XLogDataFromLSN(ret, data, xlogid, xrecoff)        \
    ret = sscanf_s(data, "%X/%X", xlogid, xrecoff)

#define IsCompressedXLogFileName(fname) \
    (strlen(fname) == XLOG_FNAME_LEN + strlen(".gz") &&            \
     strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&        \
     strcmp((fname) + XLOG_FNAME_LEN, ".gz") == 0)

#if PG_VERSION_NUM >= 110000
#define GetXLogSegNo(xlrp, logSegNo, wal_segsz_bytes) \
    XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)
#define GetXLogRecPtr(segno, offset, wal_segsz_bytes, dest) \
    XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest)
#define GetXLogFileName(fname, len, tli, logSegNo, wal_segsz_bytes) \
    XLogFileName(fname, len, tli, logSegNo, wal_segsz_bytes)
#define IsInXLogSeg(xlrp, logSegNo, wal_segsz_bytes) \
    XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes)

#define GetXLogSegNoFromScrath(logSegNo, log, seg, wal_segsz_bytes)    \
        logSegNo = (uint64) log * XLogSegmentsPerXLogId(wal_segsz_bytes) + seg

#define GetXLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes) \
        XLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes)
#else
#define GetXLogSegNo(xlrp, logSegNo, wal_segsz_bytes) \
    XLByteToSeg(xlrp, logSegNo)
#define GetXLogRecPtr(segno, offset, wal_segsz_bytes, dest) \
    XLogSegNoOffsetToRecPtr(segno, offset, dest)
#define GetXLogFileName(fname, len, tli, logSegNo, wal_segsz_bytes) \
    XLogFileName(fname, len, tli, logSegNo )
#define IsInXLogSeg(xlrp, logSegNo, wal_segsz_bytes) \
    XLByteInSeg(xlrp, logSegNo)

#define GetXLogSegNoFromScrath(logSegNo, log, seg, wal_segsz_bytes)    \
        logSegNo = (uint64) log * XLogSegmentsPerXLogId + seg

#define GetXLogFromFileName(fname, tli, logSegNo, wal_segsz_bytes) \
        XLogFromFileName(fname, tli, logSegNo)
#endif

#endif /* PG_PROBACKUPB_H */
