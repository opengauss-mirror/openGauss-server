/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
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
 * IDENTIFICATION
 *    src/include/pgstat_shmem.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGSTAT_SHMEM_H
#define PGSTAT_SHMEM_H

#include "pgstat.h"
#include "storage/lock/lwlock.h"
#include "utils/hsearch.h"

#define PGSTAT_DB_NPARTITIONS 16
#define PGSTAT_TAB_NPARTITIONS 512
#define PGSTAT_FUNC_NPARTITIONS 16
#define PGSTAT_THREADIO_NPARTITIONS 4

typedef struct PgStatSharedDBEntry {
    Oid databaseid;
    PgStat_Counter n_xact_commit;
    PgStat_Counter n_xact_rollback;
    PgStat_Counter n_blocks_fetched;
    PgStat_Counter n_blocks_hit;

    PgStat_Counter n_cu_mem_hit;
    PgStat_Counter n_cu_hdd_sync;
    PgStat_Counter n_cu_hdd_asyn;

    PgStat_Counter n_tuples_returned;
    PgStat_Counter n_tuples_fetched;
    PgStat_Counter n_tuples_inserted;
    PgStat_Counter n_tuples_updated;
    PgStat_Counter n_tuples_deleted;
    TimestampTz last_autovac_time;
    PgStat_Counter n_conflict_tablespace;
    PgStat_Counter n_conflict_lock;
    PgStat_Counter n_conflict_snapshot;
    PgStat_Counter n_conflict_bufferpin;
    PgStat_Counter n_conflict_startup_deadlock;
    PgStat_Counter n_temp_files;
    PgStat_Counter n_temp_bytes;
    PgStat_Counter n_deadlocks;
    PgStat_Counter n_block_read_time; /* times in microseconds */
    PgStat_Counter n_block_write_time;
    PgStat_Counter n_mem_mbytes_reserved;

    TimestampTz stat_reset_timestamp;
} PgStatSharedDBEntry;

typedef struct PgStatSharedTabKey {
    Oid databaseid;
    Oid tableid;
    uint32 statFlag;
} PgStatSharedTabKey;

typedef struct PgStatSharedTabEntry {
    PgStatSharedTabKey key;

    PgStat_Counter numscans;
    TimestampTz lastscan;

    PgStat_Counter tuples_returned;
    PgStat_Counter tuples_fetched;

    PgStat_Counter tuples_inserted;
    PgStat_Counter tuples_updated;
    PgStat_Counter tuples_deleted;
    PgStat_Counter tuples_inplace_updated;
    PgStat_Counter tuples_hot_updated;

    PgStat_Counter n_live_tuples;
    PgStat_Counter n_dead_tuples;
    PgStat_Counter changes_since_analyze;

    PgStat_Counter blocks_fetched;
    PgStat_Counter blocks_hit;

    PgStat_Counter cu_mem_hit;
    PgStat_Counter cu_hdd_sync;
    PgStat_Counter cu_hdd_asyn;

    PgStat_Counter success_prune_cnt;
    PgStat_Counter total_prune_cnt;

    TimestampTz vacuum_timestamp; /* user initiated vacuum */
    PgStat_Counter vacuum_count;
    TimestampTz autovac_vacuum_timestamp; /* autovacuum initiated */
    PgStat_Counter autovac_vacuum_count;
    TimestampTz analyze_timestamp; /* user initiated */
    PgStat_Counter analyze_count;
    TimestampTz autovac_analyze_timestamp; /* autovacuum initiated */
    PgStat_Counter autovac_analyze_count;
    TimestampTz data_changed_timestamp; /* start to insert/delete/upate */
    uint64 autovac_status;
} PgStatSharedTabEntry;

typedef struct PgStatSharedFuncKey {
    Oid databaseid;
    Oid functionid;
} PgStatSharedFuncKey;

typedef struct PgStatSharedFuncEntry {
    PgStatSharedFuncKey key;

    PgStat_Counter f_numcalls;

    PgStat_Counter f_total_time; /* times in microseconds */
    PgStat_Counter f_self_time;
} PgStatSharedFuncEntry;

typedef struct PgStatSharedThreadIOKey {
    uint32 role_id;
    uint32 object_id;
    uint32 context_id;
} PgStatSharedThreadIOKey;

typedef struct PgStatSharedThreadIOEntry {
    PgStatSharedThreadIOKey key;
    PgStat_ThreadIOStats stats;
} PgStatSharedThreadIOEntry;

typedef struct PgStatSharedState {
    LWLockPadded db_locks[PGSTAT_DB_NPARTITIONS];
    LWLockPadded tab_locks[PGSTAT_TAB_NPARTITIONS];
    LWLockPadded func_locks[PGSTAT_FUNC_NPARTITIONS];
    LWLockPadded threadio_locks[PGSTAT_THREADIO_NPARTITIONS];
    LWLockPadded global_lock;

    HTAB* db_hash;
    HTAB* tab_hash;
    HTAB* func_hash;
    HTAB* threadio_hash;

    PgStat_GlobalStats global_stats;
    /* true after first attempt to load permanent stats file into shmem (used for lazy load in backend). */
    bool stats_file_load_attempted;
    /* Bumped on reset_db/drop_db/resetsinglecounter; backends discard pending when epoch changes. */
    uint64 stats_epoch;
} PgStatSharedState;

extern uint64 pgstat_shared_get_epoch(void);
extern void pgstat_shared_bump_epoch(void);

/* Accessor: pgstat shared state is stored in g_instance.stat_cxt.pgstat_shared. */
static inline PgStatSharedState* pgstat_get_shared_state(void)
{
    return (PgStatSharedState*)g_instance.stat_cxt.pgstat_shared;
}

extern Size PgStatShmemSize(void);
/** Estimated size in use (by current entry counts); <= PgStatShmemSize(). */
extern Size PgStatShmemUsedSize(void);
extern void PgStatShmemInit(void);

extern PgStatSharedDBEntry* pgstat_shared_get_db_entry(Oid dbid, bool create, LWLockMode mode, LWLock** lock,
    bool* found);
extern uint32 pgstat_tab_partition_index(const PgStatSharedTabKey* key);
extern LWLock* pgstat_shared_tab_lock_for_key(const PgStatSharedTabKey* key);
extern PgStatSharedTabEntry* pgstat_shared_get_tab_entry_under_tablock(const PgStatSharedTabKey* key, bool create,
    bool* found);
extern PgStatSharedTabEntry* pgstat_shared_get_tab_entry(const PgStatSharedTabKey* key, bool create,
    LWLockMode mode, LWLock** lock, bool* found);
extern PgStatSharedFuncEntry* pgstat_shared_get_func_entry(const PgStatSharedFuncKey* key, bool create,
    LWLockMode mode, LWLock** lock, bool* found);

extern uint32 pgstat_threadio_partition_index(const PgStatSharedThreadIOKey* key);
extern LWLock* pgstat_shared_threadio_lock_for_key(const PgStatSharedThreadIOKey* key);
extern PgStatSharedThreadIOEntry* pgstat_shared_get_threadio_entry_under_threadiolock(const PgStatSharedThreadIOKey* key,
    bool create, bool* found);
extern PgStatSharedThreadIOEntry* pgstat_shared_get_threadio_entry(const PgStatSharedThreadIOKey* key, bool create,
    LWLockMode mode, LWLock** lock, bool* found);

extern void pgstat_shared_release_lock(LWLock* lock);

extern bool pgstat_shared_remove_tab_entry(Oid dbid, Oid relid, uint32 statFlag, PgStatSharedTabEntry* removed);
extern bool pgstat_shared_remove_func_entry(Oid dbid, Oid funcid);

extern void pgstat_shared_reset_db(Oid dbid);
extern void pgstat_shared_reset_sharedcounter(PgStat_Shared_Reset_Target target);
extern void pgstat_shared_drop_db(Oid dbid);

extern void pgstat_shared_copy_snapshot(Oid onlydb, MemoryContext mcxt, HTAB** out_dbhash,
    PgStat_GlobalStats* out_global);
extern void pgstat_shared_import_snapshot(HTAB* dbhash, const PgStat_GlobalStats* global);

static inline LWLock* pgstat_shared_global_lock(void)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    return (s == NULL) ? NULL : &s->global_lock.lock;
}

static inline PgStat_GlobalStats* pgstat_shared_global_stats(void)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    return (s == NULL) ? NULL : &s->global_stats;
}

#endif /* PGSTAT_SHMEM_H */
