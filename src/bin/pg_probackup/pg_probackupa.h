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
#ifndef PG_PROBACKUPA_H
#define PG_PROBACKUPA_H

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"

#include "access/xlog_internal.h"
#include "utils/pg_crc.h"

#if PG_VERSION_NUM >= 120000
#include "common/logging.h"
#endif

#ifdef FRONTEND
#undef FRONTEND
#include "atomics.h"
#define FRONTEND
#else
#include "atomics.h"
#endif

#include "configuration.h"
#include "logger.h"
#include "remote.h"
#include "parray.h"
#include "pgut.h"
#include "file.h"

#include "datapagemap.h"
#include "thread.h"

#ifdef WIN32
#define __thread __declspec(thread)
#else
#include <pthread.h>
#endif

/* pgut client variables and full path */
extern const char  *PROGRAM_NAME;
extern const char  *PROGRAM_NAME_FULL;
extern const char  *PROGRAM_FULL_PATH;

/* Directory/File names */
#define DATABASE_DIR                "database"
#define DSSDATA_DIR                  "dssdata"
#define BACKUPS_DIR                    "backups"
#define PG_XLOG_DIR                    "pg_xlog"
#define PG_LOG_DIR                     "pg_log"
#define PG_TBLSPC_DIR                "pg_tblspc"
#define PG_GLOBAL_DIR                "global"
#define PG_XLOG_CONTROL_FILE          "pg_control"
#define BACKUP_CONTROL_FILE            "backup.control"
#define BACKUP_CATALOG_CONF_FILE    "pg_probackup.conf"
#define BACKUP_CATALOG_PID            "backup.pid"
#define DATABASE_FILE_LIST            "backup_content.control"
#define PG_BACKUP_LABEL_FILE        "backup_label"
#define PG_TABLESPACE_MAP_FILE         "tablespace_map"
#define EXTERNAL_DIR                "external_directories/externaldir"
#define DATABASE_MAP                "database_map"
#define HEADER_MAP                  "page_header_map"
#define HEADER_MAP_TMP              "page_header_map_tmp"
#define PG_RELATIVE_TBLSPC_DIR       "pg_location"
#define PG_REPLSLOT_DIR              "pg_replslot"

/* Timeout defaults */
#define ARCHIVE_TIMEOUT_DEFAULT        300

/* Directory/File permission */
#define DIR_PERMISSION        (0700)
#define FILE_PERMISSION        (0600)

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#define STDOUT_FILENO 1
#endif

/* stdio buffer size */
#define STDIO_BUFSIZE 65536

#define ERRMSG_MAX_LEN 2048
#define CHUNK_SIZE (128 * 1024)
#define LARGE_CHUNK_SIZE (4 * 1024 * 1024)
#define OUT_BUF_SIZE (512 * 1024)

/* retry attempts */
#define PAGE_READ_ATTEMPTS 300

/* max size of note, that can be added to backup */
#define MAX_NOTE_SIZE 1024

/* Check if an XLogRecPtr value is pointed to 0 offset */
#define XRecOffIsNull(xlrp) \
        ((xlrp) % XLOG_BLCKSZ == 0)

typedef struct RedoParams
{
    TimeLineID  tli;
    XLogRecPtr  lsn;
    uint32      checksum_version;
} RedoParams;

typedef struct PageState
{
    uint16  checksum;
    XLogRecPtr  lsn;
} PageState;

typedef struct db_map_entry
{
    Oid dbOid;
    char *datname;
} db_map_entry;

typedef enum IncrRestoreMode
{
    INCR_NONE,
    INCR_CHECKSUM,
    INCR_LSN
} IncrRestoreMode;

typedef enum PartialRestoreType
{
    NONE,
    INCLUDE,
    EXCLUDE,
} PartialRestoreType;

typedef enum CompressAlg
{
    NOT_DEFINED_COMPRESS = 0,
    NONE_COMPRESS,
    PGLZ_COMPRESS,
#ifdef HAVE_LIBZ
    ZLIB_COMPRESS,
#endif
    LZ4_COMPRESS,
    ZSTD_COMPRESS,
} CompressAlg;

typedef enum ForkName
{
    vm,
    fsm,
    cfm,
    init,
    ptrack
} ForkName;

#define INIT_FILE_CRC32(use_crc32c, crc) \
do { \
    if (use_crc32c) \
        INIT_CRC32C(crc); \
    else \
        INIT_TRADITIONAL_CRC32(crc); \
} while (0)
#define COMP_FILE_CRC32(use_crc32c, crc, data, len) \
do { \
    if (use_crc32c) \
        COMP_CRC32C((crc), (data), (len)); \
    else \
        COMP_TRADITIONAL_CRC32(crc, data, len); \
} while (0)
#define FIN_FILE_CRC32(use_crc32c, crc) \
do { \
    if (use_crc32c) \
        FIN_CRC32C(crc); \
    else \
        FIN_TRADITIONAL_CRC32(crc); \
} while (0)

#endif /* PG_PROBACKUPA_H */
