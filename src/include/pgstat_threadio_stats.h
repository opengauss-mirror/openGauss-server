/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * pgstat_threadio_stats.h
 *
 * PgStat_ThreadIOStats (indexed by thread role / object type / context type)
 *
 * IDENTIFICATION
 *        src/include/pgstat_threadio_stats.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PGSTAT_THREADIO_STATS_H
#define PGSTAT_THREADIO_STATS_H

#include "c.h"          /* int64 */

typedef struct PgStat_ThreadIOStats {
    /* Basic read/write statistics */
    int64 num_reads;          /* read count */
    int64 num_writes;         /* write count */
    int64 bytes_read;         /* bytes read */
    int64 bytes_written;      /* bytes written */
    int64 read_time;          /* total read I/O time (us) */
    int64 write_time;         /* total write I/O time (us) */

    /* Dirty-page writeback statistics (a core tail-latency source) */
    int64 writebacks;         /* writeback count (BLCKSZ) */
    int64 writeback_time;     /* total writeback time */
    int64 max_writeback_time; /* max single writeback time (tail latency) */

    /* File extension statistics (a large-table extension jitter source) */
    int64 extend_bytes;       /* extended bytes */
    int64 extend_time;        /* total extension time */
    int64 max_extend_time;    /* max single extension time (tail latency) */

    /* Buffer scheduling statistics (memory I/O jitter) */
    int64 hits;               /* buffer hit count */
    int64 evictions;          /* buffer eviction count */
    int64 reuses;             /* ring-buffer reuse count */

    /* Persistence fsync statistics (durability blocking) */
    int64 fsyncs;             /* fsync call count */
    int64 total_fsync_time;   /* total fsync time */

    /* Basic read/write tail-latency maxima */
    int64 max_read_time;      /* max single read time */
    int64 max_write_time;     /* max single write time */
} PgStat_ThreadIOStats;

typedef enum ThreadIOContextType {
    IO_CONTEXT_NORMAL,        /* regular single-point read/write */
    IO_CONTEXT_BULKREAD,      /* bulk prefetch, full table scan */
    IO_CONTEXT_BULKWRITE,     /* bulk write, COPY import */
    IO_CONTEXT_VACUUM,        /* garbage collection */
    IO_CONTEXT_RECOVERY,      /* crash recovery replay */
    IO_CONTEXT_REPLICATION,   /* primary-standby replication */
    THREAD_IO_CONTEXT_MAX
} ThreadIOContextType;

typedef enum ThreadIOObjectType {
    IO_OBJECT_RELATION,                /* table and index files */
    IO_OBJECT_TEMP_RELATION,           /* temporary table files */
    IO_OBJECT_WAL,                     /* WAL log files */
    IO_OBJECT_UNDO,                    /* undo log files */
    IO_OBJECT_ARCHIVE,                 /* archive files */
    IO_OBJECT_OTHER,                   /* other file types */
    THREAD_IO_OBJECT_MAX
} ThreadIOObjectType;

#endif /* PGSTAT_THREADIO_STATS_H */
