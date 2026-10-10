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
 *    src/gausskernel/process/postmaster/pgstat_shmem.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pgstat_shmem.h"
#include "storage/shmem.h"
#include "storage/lock/lwlock.h"
#include "utils/atomic.h"
#include "utils/dynahash.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "access/hash.h"

#define PGSTAT_SHMEM_DB_HASH_SIZE 256
#define PGSTAT_SHMEM_TAB_HASH_SIZE 524288 /* 512K, covers 400K+ tables/partitions/TOAST with headroom */
#define PGSTAT_SHMEM_FUNC_HASH_SIZE 8192
#define PGSTAT_SHMEM_THREADIO_HASH_SIZE 1024
/* Ratio (0.0-1.0) of hash size above which we log "near full" warning. */
#define PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO 0.9

#define PGSTAT_SNAPSHOT_DB_HASH_SIZE 16
#define PGSTAT_SNAPSHOT_TAB_HASH_SIZE 512
#define PGSTAT_SNAPSHOT_FUNC_HASH_SIZE 512
#define PGSTAT_SNAPSHOT_THREADIO_HASH_SIZE 512

static inline uint32 pgstat_hash_dbid(Oid dbid)
{
    return hash_uint32((uint32)dbid);
}

static inline uint32 pgstat_hash_tabkey(const PgStatSharedTabKey* key)
{
    return tag_hash((const void*)key, sizeof(PgStatSharedTabKey));
}

static inline uint32 pgstat_hash_funckey(const PgStatSharedFuncKey* key)
{
    return tag_hash((const void*)key, sizeof(PgStatSharedFuncKey));
}

static inline uint32 pgstat_hash_threadiokey(const PgStatSharedThreadIOKey* key)
{
    return tag_hash((const void*)key, sizeof(PgStatSharedThreadIOKey));
}

static inline LWLock* pgstat_db_lock(PgStatSharedState* s, Oid dbid)
{
    uint32 hash = pgstat_hash_dbid(dbid);
    return &s->db_locks[hash % PGSTAT_DB_NPARTITIONS].lock;
}

static inline LWLock* pgstat_tab_lock(PgStatSharedState* s, const PgStatSharedTabKey* key)
{
    uint32 hash = pgstat_hash_tabkey(key);
    return &s->tab_locks[hash % PGSTAT_TAB_NPARTITIONS].lock;
}

static inline LWLock* pgstat_func_lock(PgStatSharedState* s, const PgStatSharedFuncKey* key)
{
    uint32 hash = pgstat_hash_funckey(key);
    return &s->func_locks[hash % PGSTAT_FUNC_NPARTITIONS].lock;
}

static inline LWLock* pgstat_threadio_lock(PgStatSharedState* s, const PgStatSharedThreadIOKey* key)
{
    uint32 hash = pgstat_hash_threadiokey(key);
    return &s->threadio_locks[hash % PGSTAT_THREADIO_NPARTITIONS].lock;
}

static void pgstat_lock_all(LWLockPadded* locks, int count, LWLockMode mode)
{
    for (int i = 0; i < count; i++)
        LWLockAcquire(&locks[i].lock, mode);
}

static void pgstat_unlock_all(LWLockPadded* locks, int count)
{
    for (int i = count - 1; i >= 0; i--)
        LWLockRelease(&locks[i].lock);
}

static inline bool pgstat_dbid_visible(Oid dbid, Oid onlydb)
{
    if (onlydb == InvalidOid) {
        return true;
    }
    return (dbid == onlydb);
}

static void pgstat_shared_init_db_entry(PgStatSharedDBEntry* entry, Oid dbid)
{
    errno_t rc = memset_s(entry, sizeof(PgStatSharedDBEntry), 0, sizeof(PgStatSharedDBEntry));
    securec_check(rc, "\0", "\0");
    entry->databaseid = dbid;
    entry->stat_reset_timestamp = GetCurrentTimestamp();
}

static void pgstat_shared_init_tab_entry(PgStatSharedTabEntry* entry, const PgStatSharedTabKey* key)
{
    errno_t rc = memset_s(entry, sizeof(PgStatSharedTabEntry), 0, sizeof(PgStatSharedTabEntry));
    securec_check(rc, "\0", "\0");
    entry->key = *key;
}

static void pgstat_shared_init_func_entry(PgStatSharedFuncEntry* entry, const PgStatSharedFuncKey* key)
{
    errno_t rc = memset_s(entry, sizeof(PgStatSharedFuncEntry), 0, sizeof(PgStatSharedFuncEntry));
    securec_check(rc, "\0", "\0");
    entry->key = *key;
}

static void pgstat_shared_init_threadio_entry(PgStatSharedThreadIOEntry* entry, const PgStatSharedThreadIOKey* key)
{
    errno_t rc = memset_s(entry, sizeof(PgStatSharedThreadIOEntry), 0, sizeof(PgStatSharedThreadIOEntry));
    securec_check(rc, "\0", "\0");
    entry->key = *key;
}

static void pgstat_threadio_hash_warn_near_full(PgStatSharedState* s)
{
    long num_entries = hash_get_num_entries(s->threadio_hash);
    if (num_entries >= (long)(PGSTAT_SHMEM_THREADIO_HASH_SIZE * PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO)) {
        ereport(WARNING,
            (errmsg("pgstat threadio hash near full: current entries %ld, limit %d; "
                     "new threadio stats may not be recorded",
                num_entries, PGSTAT_SHMEM_THREADIO_HASH_SIZE)));
    }
}

static void pgstat_func_hash_warn_near_full(PgStatSharedState* s)
{
    long num_entries = hash_get_num_entries(s->func_hash);
    if (num_entries >= (long)(PGSTAT_SHMEM_FUNC_HASH_SIZE * PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO)) {
        ereport(WARNING,
            (errmsg("pgstat func hash near full: current entries %ld, limit %d; "
                     "new functions may not be recorded",
                num_entries, PGSTAT_SHMEM_FUNC_HASH_SIZE)));
    }
}

/*
 * Total shared memory = PgStatSharedState + DB hash + TAB hash + FUNC hash.
 * With default sizes (DB=256, TAB=524288, FUNC=8192) on 64-bit, approximate total is ~145 MB,
 * of which the TAB hash (table/partition/TOAST stats) accounts for the vast majority (~140 MB).
 */
Size PgStatShmemSize(void)
{
    Size size = MAXALIGN(sizeof(PgStatSharedState));
    size = add_size(size, hash_estimate_size(PGSTAT_SHMEM_DB_HASH_SIZE, sizeof(PgStatSharedDBEntry)));
    size = add_size(size, hash_estimate_size(PGSTAT_SHMEM_TAB_HASH_SIZE, sizeof(PgStatSharedTabEntry)));
    size = add_size(size, hash_estimate_size(PGSTAT_SHMEM_FUNC_HASH_SIZE, sizeof(PgStatSharedFuncEntry)));
    size = add_size(size, hash_estimate_size(PGSTAT_SHMEM_THREADIO_HASH_SIZE, sizeof(PgStatSharedThreadIOEntry)));
    return size;
}

/*
 * Estimate of shared memory "in use" by current entry counts (DB/TAB/FUNC hashes).
 * May be less than PgStatShmemSize() when tables are not full; useful for monitoring.
 */
Size PgStatShmemUsedSize(void)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL || s->db_hash == NULL || s->tab_hash == NULL || s->func_hash == NULL || s->threadio_hash == NULL) {
        return 0;
    }
    long n_db = hash_get_num_entries(s->db_hash);
    long n_tab = hash_get_num_entries(s->tab_hash);
    long n_func = hash_get_num_entries(s->func_hash);
    long n_threadio = hash_get_num_entries(s->threadio_hash);
    Size used = MAXALIGN(sizeof(PgStatSharedState));
    used = add_size(used, hash_estimate_size(n_db, sizeof(PgStatSharedDBEntry)));
    used = add_size(used, hash_estimate_size(n_tab, sizeof(PgStatSharedTabEntry)));
    used = add_size(used, hash_estimate_size(n_func, sizeof(PgStatSharedFuncEntry)));
    used = add_size(used, hash_estimate_size(n_threadio, sizeof(PgStatSharedThreadIOEntry)));
    return used;
}

void PgStatShmemInit(void)
{
    bool found = false;
    HASHCTL ctl;
    errno_t rc;

    g_instance.stat_cxt.pgstat_shared = (PgStatSharedState*)ShmemInitStruct("PgStat Shared State",
        sizeof(PgStatSharedState), &found);

    if (found) {
        return;
    }

    PgStatSharedState* s = g_instance.stat_cxt.pgstat_shared;
    for (int i = 0; i < PGSTAT_DB_NPARTITIONS; i++)
        LWLockInitialize(&s->db_locks[i].lock, LWTRANCHE_PGSTAT_HASH);
    for (int i = 0; i < PGSTAT_TAB_NPARTITIONS; i++)
        LWLockInitialize(&s->tab_locks[i].lock, LWTRANCHE_PGSTAT_HASH);
    for (int i = 0; i < PGSTAT_FUNC_NPARTITIONS; i++)
        LWLockInitialize(&s->func_locks[i].lock, LWTRANCHE_PGSTAT_HASH);
    for (int i = 0; i < PGSTAT_THREADIO_NPARTITIONS; i++)
        LWLockInitialize(&s->threadio_locks[i].lock, LWTRANCHE_PGSTAT_HASH);
    LWLockInitialize(&s->global_lock.lock, LWTRANCHE_PGSTAT_HASH);

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(PgStatSharedDBEntry);
    ctl.hash = oid_hash;
    s->db_hash = ShmemInitHash("PgStat DB Hash", PGSTAT_SHMEM_DB_HASH_SIZE,
        PGSTAT_SHMEM_DB_HASH_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION);

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PgStatSharedTabKey);
    ctl.entrysize = sizeof(PgStatSharedTabEntry);
    ctl.hash = tag_hash;
    s->tab_hash = ShmemInitHash("PgStat TAB Hash", PGSTAT_SHMEM_TAB_HASH_SIZE,
        PGSTAT_SHMEM_TAB_HASH_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION);

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PgStatSharedFuncKey);
    ctl.entrysize = sizeof(PgStatSharedFuncEntry);
    ctl.hash = tag_hash;
    s->func_hash = ShmemInitHash("PgStat FUNC Hash", PGSTAT_SHMEM_FUNC_HASH_SIZE,
        PGSTAT_SHMEM_FUNC_HASH_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION);

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PgStatSharedThreadIOKey);
    ctl.entrysize = sizeof(PgStatSharedThreadIOEntry);
    ctl.hash = tag_hash;
    s->threadio_hash = ShmemInitHash("PgStat THREADIO Hash", PGSTAT_SHMEM_THREADIO_HASH_SIZE,
        PGSTAT_SHMEM_THREADIO_HASH_SIZE, &ctl, HASH_ELEM | HASH_FUNCTION);

    rc = memset_s(&s->global_stats, sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
    securec_check(rc, "\0", "\0");
    s->global_stats.stat_reset_timestamp = GetCurrentTimestamp();
    s->stats_file_load_attempted = false;
    s->stats_epoch = 0;
}

uint64 pgstat_shared_get_epoch(void)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return 0;
    }
    return pg_atomic_read_u64((volatile uint64*)&s->stats_epoch);
}

void pgstat_shared_bump_epoch(void)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s != NULL) {
        (void)pg_atomic_fetch_add_u64((volatile uint64*)&s->stats_epoch, 1);
    }
}

PgStatSharedDBEntry* pgstat_shared_get_db_entry(Oid dbid, bool create, LWLockMode mode, LWLock** lock, bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_db_lock(s, dbid);
    bool locked = false;
    if (!LWLockHeldByMe(l)) {
        LWLockAcquire(l, mode);
        locked = true;
    }

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedDBEntry* entry =
        (PgStatSharedDBEntry*)hash_search(s->db_hash, &dbid, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat db entry creation failed (hash may be full), database %u", dbid)));
        }
        if (locked) {
            LWLockRelease(l);
        }
        if (lock) {
            *lock = NULL;
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_db_entry(entry, dbid);
        {
            long num_entries = hash_get_num_entries(s->db_hash);
            if (num_entries >= (long)(PGSTAT_SHMEM_DB_HASH_SIZE * PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO)) {
                ereport(WARNING,
                    (errmsg("pgstat db hash near full: current entries %ld, limit %d; "
                             "new databases may not be recorded",
                        num_entries, PGSTAT_SHMEM_DB_HASH_SIZE)));
            }
        }
    }

    if (found) {
        *found = local_found;
    }
    if (lock) {
        *lock = locked ? l : NULL;
    }
    return entry;
}

uint32 pgstat_tab_partition_index(const PgStatSharedTabKey* key)
{
    return pgstat_hash_tabkey(key) % (uint32)PGSTAT_TAB_NPARTITIONS;
}

LWLock* pgstat_shared_tab_lock_for_key(const PgStatSharedTabKey* key)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }
    return pgstat_tab_lock(s, key);
}

/*
 * Like pgstat_shared_get_tab_entry, but caller must already hold the tab partition LWLock
 * for this key (same lock as pgstat_shared_tab_lock_for_key(key)).
 */
PgStatSharedTabEntry* pgstat_shared_get_tab_entry_under_tablock(const PgStatSharedTabKey* key, bool create,
    bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_tab_lock(s, key);
    Assert(LWLockHeldByMe(l));

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedTabEntry* entry = (PgStatSharedTabEntry*)hash_search(s->tab_hash, key, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat tab entry creation failed (hash may be full), database %u table %u statFlag %u",
                    key->databaseid, key->tableid, key->statFlag)));
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_tab_entry(entry, key);
        {
            long num_entries = hash_get_num_entries(s->tab_hash);
            if (num_entries >= (long)(PGSTAT_SHMEM_TAB_HASH_SIZE * PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO)) {
                ereport(WARNING,
                    (errmsg("pgstat tab hash near full: current entries %ld, limit %d; "
                             "new relations may not be recorded, consider running VACUUM or increasing capacity",
                        num_entries, PGSTAT_SHMEM_TAB_HASH_SIZE)));
            }
        }
    }

    if (found) {
        *found = local_found;
    }
    return entry;
}

PgStatSharedTabEntry* pgstat_shared_get_tab_entry(const PgStatSharedTabKey* key, bool create, LWLockMode mode,
    LWLock** lock, bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_tab_lock(s, key);
    bool locked = false;
    if (!LWLockHeldByMe(l)) {
        LWLockAcquire(l, mode);
        locked = true;
    }

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedTabEntry* entry =
        (PgStatSharedTabEntry*)hash_search(s->tab_hash, key, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat tab entry creation failed (hash may be full), database %u table %u statFlag %u",
                    key->databaseid, key->tableid, key->statFlag)));
        }
        if (locked) {
            LWLockRelease(l);
        }
        if (lock) {
            *lock = NULL;
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_tab_entry(entry, key);
        /* Log when tab hash is near full so operators can take action (e.g. VACUUM or increase hash size). */
        {
            long num_entries = hash_get_num_entries(s->tab_hash);
            if (num_entries >= (long)(PGSTAT_SHMEM_TAB_HASH_SIZE * PGSTAT_SHMEM_HASH_NEAR_FULL_RATIO)) {
                ereport(WARNING,
                    (errmsg("pgstat tab hash near full: current entries %ld, limit %d; "
                             "new relations may not be recorded, consider running VACUUM or increasing capacity",
                        num_entries, PGSTAT_SHMEM_TAB_HASH_SIZE)));
            }
        }
    }

    if (found) {
        *found = local_found;
    }
    if (lock) {
        *lock = locked ? l : NULL;
    }
    return entry;
}

PgStatSharedFuncEntry* pgstat_shared_get_func_entry(
    const PgStatSharedFuncKey* key, bool create, LWLockMode mode, LWLock** lock, bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_func_lock(s, key);
    bool locked = false;
    if (!LWLockHeldByMe(l)) {
        LWLockAcquire(l, mode);
        locked = true;
    }

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedFuncEntry* entry =
        (PgStatSharedFuncEntry*)hash_search(s->func_hash, key, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat func entry creation failed (hash may be full), database %u function %u",
                    key->databaseid, key->functionid)));
        }
        if (locked) {
            LWLockRelease(l);
        }
        if (lock) {
            *lock = NULL;
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_func_entry(entry, key);
        pgstat_func_hash_warn_near_full(s);
    }

    if (found) {
        *found = local_found;
    }
    if (lock) {
        *lock = locked ? l : NULL;
    }
    return entry;
}

uint32 pgstat_threadio_partition_index(const PgStatSharedThreadIOKey* key)
{
    return pgstat_hash_threadiokey(key) % (uint32)PGSTAT_THREADIO_NPARTITIONS;
}

LWLock* pgstat_shared_threadio_lock_for_key(const PgStatSharedThreadIOKey* key)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }
    return pgstat_threadio_lock(s, key);
}

PgStatSharedThreadIOEntry* pgstat_shared_get_threadio_entry_under_threadiolock(
    const PgStatSharedThreadIOKey* key, bool create, bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_threadio_lock(s, key);
    Assert(LWLockHeldByMe(l));

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedThreadIOEntry* entry =
        (PgStatSharedThreadIOEntry*)hash_search(s->threadio_hash, key, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat threadio entry creation failed (hash may be full), role %u object %u context %u",
                    key->role_id, key->object_id, key->context_id)));
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_threadio_entry(entry, key);
        pgstat_threadio_hash_warn_near_full(s);
    }

    if (found) {
        *found = local_found;
    }
    return entry;
}

PgStatSharedThreadIOEntry* pgstat_shared_get_threadio_entry(
    const PgStatSharedThreadIOKey* key, bool create, LWLockMode mode, LWLock** lock, bool* found)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return NULL;
    }

    LWLock* l = pgstat_threadio_lock(s, key);
    bool locked = false;
    if (!LWLockHeldByMe(l)) {
        LWLockAcquire(l, mode);
        locked = true;
    }

    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    bool local_found = false;
    PgStatSharedThreadIOEntry* entry =
        (PgStatSharedThreadIOEntry*)hash_search(s->threadio_hash, key, action, &local_found);

    if (entry == NULL) {
        if (create) {
            ereport(WARNING,
                (errmsg("pgstat threadio entry creation failed (hash may be full), role %u object %u context %u",
                    key->role_id, key->object_id, key->context_id)));
        }
        if (locked) {
            LWLockRelease(l);
        }
        if (lock) {
            *lock = NULL;
        }
        if (found) {
            *found = false;
        }
        return NULL;
    }

    if (!local_found && create) {
        pgstat_shared_init_threadio_entry(entry, key);
        pgstat_threadio_hash_warn_near_full(s);
    }

    if (found) {
        *found = local_found;
    }
    if (lock) {
        *lock = locked ? l : NULL;
    }
    return entry;
}

void pgstat_shared_release_lock(LWLock* lock)
{
    if (lock != NULL) {
        LWLockRelease(lock);
    }
}

bool pgstat_shared_remove_tab_entry(Oid dbid, Oid relid, uint32 statFlag, PgStatSharedTabEntry* removed)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return false;
    }

    PgStatSharedTabKey key;
    key.databaseid = dbid;
    key.tableid = relid;
    key.statFlag = statFlag;

    LWLock* l = pgstat_tab_lock(s, &key);
    LWLockAcquire(l, LW_EXCLUSIVE);

    PgStatSharedTabEntry* entry = (PgStatSharedTabEntry*)hash_search(s->tab_hash, &key, HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(l);
        return false;
    }

    if (removed != NULL) {
        errno_t rc = memcpy_s(removed, sizeof(PgStatSharedTabEntry), entry, sizeof(PgStatSharedTabEntry));
        securec_check(rc, "\0", "\0");
    }

    (void)hash_search(s->tab_hash, &key, HASH_REMOVE, NULL);
    LWLockRelease(l);
    return true;
}

bool pgstat_shared_remove_func_entry(Oid dbid, Oid funcid)
{
    PgStatSharedState* s = pgstat_get_shared_state();
    if (s == NULL) {
        return false;
    }

    PgStatSharedFuncKey key;
    key.databaseid = dbid;
    key.functionid = funcid;

    LWLock* l = pgstat_func_lock(s, &key);
    LWLockAcquire(l, LW_EXCLUSIVE);

    PgStatSharedFuncEntry* entry =
        (PgStatSharedFuncEntry*)hash_search(s->func_hash, &key, HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(l);
        return false;
    }

    (void)hash_search(s->func_hash, &key, HASH_REMOVE, NULL);
    LWLockRelease(l);
    return true;
}

static void pgstat_shared_remove_tab_by_dbid(Oid dbid)
{
    if (pgstat_get_shared_state() == NULL) {
        return;
    }

    pgstat_lock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS, LW_EXCLUSIVE);

    HASH_SEQ_STATUS seq;
    hash_seq_init(&seq, pgstat_get_shared_state()->tab_hash);
    PgStatSharedTabEntry* entry = NULL;
    while ((entry = (PgStatSharedTabEntry*)hash_seq_search(&seq)) != NULL) {
        if (entry->key.databaseid != dbid) {
            continue;
        }
        PgStatSharedTabKey key = entry->key;
        (void)hash_search(pgstat_get_shared_state()->tab_hash, &key, HASH_REMOVE, NULL);
    }

    pgstat_unlock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS);
}

static void pgstat_shared_remove_func_by_dbid(Oid dbid)
{
    if (pgstat_get_shared_state() == NULL) {
        return;
    }

    pgstat_lock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS, LW_EXCLUSIVE);

    HASH_SEQ_STATUS seq;
    hash_seq_init(&seq, pgstat_get_shared_state()->func_hash);
    PgStatSharedFuncEntry* entry = NULL;
    while ((entry = (PgStatSharedFuncEntry*)hash_seq_search(&seq)) != NULL) {
        if (entry->key.databaseid != dbid) {
            continue;
        }
        PgStatSharedFuncKey key = entry->key;
        (void)hash_search(pgstat_get_shared_state()->func_hash, &key, HASH_REMOVE, NULL);
    }

    pgstat_unlock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS);
}

void pgstat_shared_reset_db(Oid dbid)
{
    LWLock* lock = NULL;
    PgStatSharedDBEntry* dbentry = pgstat_shared_get_db_entry(dbid, false, LW_EXCLUSIVE, &lock, NULL);
    if (dbentry != NULL) {
        errno_t rc = memset_s(dbentry, sizeof(PgStatSharedDBEntry), 0, sizeof(PgStatSharedDBEntry));
        securec_check(rc, "\0", "\0");
        dbentry->databaseid = dbid;
        dbentry->stat_reset_timestamp = GetCurrentTimestamp();
        pgstat_shared_release_lock(lock);
    }

    pgstat_shared_remove_tab_by_dbid(dbid);
    pgstat_shared_remove_func_by_dbid(dbid);
    pgstat_shared_bump_epoch();
}

void pgstat_shared_reset_sharedcounter(PgStat_Shared_Reset_Target target)
{
    if (pgstat_get_shared_state() == NULL) {
        return;
    }

    if (target != RESET_BGWRITER) {
        return;
    }

    LWLock* lock = pgstat_shared_global_lock();
    if (lock == NULL) {
        return;
    }

    LWLockAcquire(lock, LW_EXCLUSIVE);
    errno_t rc = memset_s(&pgstat_get_shared_state()->global_stats,
        sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
    securec_check(rc, "\0", "\0");
    pgstat_get_shared_state()->global_stats.stat_reset_timestamp = GetCurrentTimestamp();
    LWLockRelease(lock);
}

void pgstat_shared_drop_db(Oid dbid)
{
    LWLock* lock = NULL;
    PgStatSharedDBEntry* dbentry = pgstat_shared_get_db_entry(dbid, false, LW_EXCLUSIVE, &lock, NULL);
    if (dbentry != NULL) {
        (void)hash_search(pgstat_get_shared_state()->db_hash, &dbid, HASH_REMOVE, NULL);
        pgstat_shared_release_lock(lock);
    }

    pgstat_shared_remove_tab_by_dbid(dbid);
    pgstat_shared_remove_func_by_dbid(dbid);
    pgstat_shared_bump_epoch();
}

static PgStat_StatDBEntry* snapshot_get_db_entry(HTAB* dbhash, MemoryContext mcxt, Oid dbid, bool create)
{
    bool found = false;
    HASHACTION action = create ? HASH_ENTER : HASH_FIND;
    PgStat_StatDBEntry* entry = (PgStat_StatDBEntry*)hash_search(dbhash, &dbid, action, &found);
    if (entry == NULL) {
        return NULL;
    }

    if (!found && create) {
        errno_t rc = memset_s(entry, sizeof(PgStat_StatDBEntry), 0, sizeof(PgStat_StatDBEntry));
        securec_check(rc, "\0", "\0");
        entry->databaseid = dbid;

        HASHCTL hash_ctl;
        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");
        hash_ctl.keysize = sizeof(PgStat_StatTabKey);
        hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
        hash_ctl.hash = tag_hash;
        hash_ctl.hcxt = mcxt;
        entry->tables =
            hash_create("Per-database table", PGSTAT_SNAPSHOT_TAB_HASH_SIZE, &hash_ctl,
                HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
        hash_ctl.hash = oid_hash;
        entry->functions =
            hash_create("Per-database function", PGSTAT_SNAPSHOT_FUNC_HASH_SIZE, &hash_ctl,
                HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        hash_ctl.keysize = sizeof(PgStat_StatThreadIOKey);
        hash_ctl.entrysize = sizeof(PgStat_StatThreadIOEntry);
        hash_ctl.hash = tag_hash;
        entry->thread_io =
            hash_create("Per-database thread io", PGSTAT_SNAPSHOT_THREADIO_HASH_SIZE, &hash_ctl,
                HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }

    return entry;
}

static void copy_snapshot_copy_tab_from_shared(PgStat_StatTabEntry* tabentry, const PgStatSharedTabEntry* stab)
{
    tabentry->numscans = stab->numscans;
    tabentry->lastscan = stab->lastscan;
    tabentry->tuples_returned = stab->tuples_returned;
    tabentry->tuples_fetched = stab->tuples_fetched;
    tabentry->tuples_inserted = stab->tuples_inserted;
    tabentry->tuples_updated = stab->tuples_updated;
    tabentry->tuples_deleted = stab->tuples_deleted;
    tabentry->tuples_inplace_updated = stab->tuples_inplace_updated;
    tabentry->tuples_hot_updated = stab->tuples_hot_updated;
    tabentry->n_live_tuples = stab->n_live_tuples;
    tabentry->n_dead_tuples = stab->n_dead_tuples;
    tabentry->changes_since_analyze = stab->changes_since_analyze;
    tabentry->blocks_fetched = stab->blocks_fetched;
    tabentry->blocks_hit = stab->blocks_hit;
    tabentry->cu_mem_hit = stab->cu_mem_hit;
    tabentry->cu_hdd_sync = stab->cu_hdd_sync;
    tabentry->cu_hdd_asyn = stab->cu_hdd_asyn;
    tabentry->success_prune_cnt = stab->success_prune_cnt;
    tabentry->total_prune_cnt = stab->total_prune_cnt;
    tabentry->vacuum_timestamp = stab->vacuum_timestamp;
    tabentry->vacuum_count = stab->vacuum_count;
    tabentry->autovac_vacuum_timestamp = stab->autovac_vacuum_timestamp;
    tabentry->autovac_vacuum_count = stab->autovac_vacuum_count;
    tabentry->analyze_timestamp = stab->analyze_timestamp;
    tabentry->analyze_count = stab->analyze_count;
    tabentry->autovac_analyze_timestamp = stab->autovac_analyze_timestamp;
    tabentry->autovac_analyze_count = stab->autovac_analyze_count;
    tabentry->data_changed_timestamp = stab->data_changed_timestamp;
    tabentry->autovac_status = stab->autovac_status;
}

static void copy_snapshot_fill_db_entries(HTAB* dbhash, MemoryContext mcxt, Oid onlydb)
{
    pgstat_lock_all(pgstat_get_shared_state()->db_locks, PGSTAT_DB_NPARTITIONS, LW_SHARED);
    HASH_SEQ_STATUS hstat;
    hash_seq_init(&hstat, pgstat_get_shared_state()->db_hash);
    PgStatSharedDBEntry* sdb = NULL;
    while ((sdb = (PgStatSharedDBEntry*)hash_seq_search(&hstat)) != NULL) {
        if (!pgstat_dbid_visible(sdb->databaseid, onlydb)) {
            continue;
        }
        PgStat_StatDBEntry* dbentry = snapshot_get_db_entry(dbhash, mcxt, sdb->databaseid, true);
        if (dbentry == NULL) {
            continue;
        }
        dbentry->n_xact_commit = sdb->n_xact_commit;
        dbentry->n_xact_rollback = sdb->n_xact_rollback;
        dbentry->n_blocks_fetched = sdb->n_blocks_fetched;
        dbentry->n_blocks_hit = sdb->n_blocks_hit;
        dbentry->n_cu_mem_hit = sdb->n_cu_mem_hit;
        dbentry->n_cu_hdd_sync = sdb->n_cu_hdd_sync;
        dbentry->n_cu_hdd_asyn = sdb->n_cu_hdd_asyn;
        dbentry->n_tuples_returned = sdb->n_tuples_returned;
        dbentry->n_tuples_fetched = sdb->n_tuples_fetched;
        dbentry->n_tuples_inserted = sdb->n_tuples_inserted;
        dbentry->n_tuples_updated = sdb->n_tuples_updated;
        dbentry->n_tuples_deleted = sdb->n_tuples_deleted;
        dbentry->last_autovac_time = sdb->last_autovac_time;
        dbentry->n_conflict_tablespace = sdb->n_conflict_tablespace;
        dbentry->n_conflict_lock = sdb->n_conflict_lock;
        dbentry->n_conflict_snapshot = sdb->n_conflict_snapshot;
        dbentry->n_conflict_bufferpin = sdb->n_conflict_bufferpin;
        dbentry->n_conflict_startup_deadlock = sdb->n_conflict_startup_deadlock;
        dbentry->n_temp_files = sdb->n_temp_files;
        dbentry->n_temp_bytes = sdb->n_temp_bytes;
        dbentry->n_deadlocks = sdb->n_deadlocks;
        dbentry->n_block_read_time = sdb->n_block_read_time;
        dbentry->n_block_write_time = sdb->n_block_write_time;
        dbentry->n_mem_mbytes_reserved = sdb->n_mem_mbytes_reserved;
        dbentry->stat_reset_timestamp = sdb->stat_reset_timestamp;
    }
    pgstat_unlock_all(pgstat_get_shared_state()->db_locks, PGSTAT_DB_NPARTITIONS);
}

static void copy_snapshot_fill_tab_entries(HTAB* dbhash, MemoryContext mcxt, Oid onlydb)
{
    pgstat_lock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS, LW_SHARED);
    HASH_SEQ_STATUS tstat;
    hash_seq_init(&tstat, pgstat_get_shared_state()->tab_hash);
    PgStatSharedTabEntry* stab = NULL;
    while ((stab = (PgStatSharedTabEntry*)hash_seq_search(&tstat)) != NULL) {
        if (!pgstat_dbid_visible(stab->key.databaseid, onlydb)) {
            continue;
        }
        PgStat_StatDBEntry* dbentry = snapshot_get_db_entry(dbhash, mcxt, stab->key.databaseid, true);
        if (dbentry == NULL || dbentry->tables == NULL) {
            continue;
        }
        PgStat_StatTabKey tabkey;
        tabkey.tableid = stab->key.tableid;
        tabkey.statFlag = stab->key.statFlag;
        bool found = false;
        PgStat_StatTabEntry* tabentry =
            (PgStat_StatTabEntry*)hash_search(dbentry->tables, &tabkey, HASH_ENTER, &found);
        if (tabentry != NULL) {
            tabentry->tablekey = tabkey;
            copy_snapshot_copy_tab_from_shared(tabentry, stab);
        }
    }
    pgstat_unlock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS);
}

static void copy_snapshot_fill_func_entries(HTAB* dbhash, MemoryContext mcxt, Oid onlydb)
{
    pgstat_lock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS, LW_SHARED);
    HASH_SEQ_STATUS fstat;
    hash_seq_init(&fstat, pgstat_get_shared_state()->func_hash);
    PgStatSharedFuncEntry* sfunc = NULL;
    while ((sfunc = (PgStatSharedFuncEntry*)hash_seq_search(&fstat)) != NULL) {
        if (!pgstat_dbid_visible(sfunc->key.databaseid, onlydb)) {
            continue;
        }
        PgStat_StatDBEntry* dbentry = snapshot_get_db_entry(dbhash, mcxt, sfunc->key.databaseid, true);
        if (dbentry == NULL || dbentry->functions == NULL) {
            continue;
        }
        bool found = false;
        PgStat_StatFuncEntry* funcentry =
            (PgStat_StatFuncEntry*)hash_search(dbentry->functions, &sfunc->key.functionid, HASH_ENTER, &found);
        if (funcentry != NULL) {
            funcentry->functionid = sfunc->key.functionid;
            funcentry->f_numcalls = sfunc->f_numcalls;
            funcentry->f_total_time = sfunc->f_total_time;
            funcentry->f_self_time = sfunc->f_self_time;
        }
    }
    pgstat_unlock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS);
}

static void copy_snapshot_fill_threadio_entries(HTAB* dbhash, MemoryContext mcxt, Oid onlydb)
{
    (void)onlydb;
    PgStat_StatDBEntry* dbentry = snapshot_get_db_entry(dbhash, mcxt, InvalidOid, true);
    if (dbentry == NULL || dbentry->thread_io == NULL) {
        return;
    }

    pgstat_lock_all(pgstat_get_shared_state()->threadio_locks, PGSTAT_THREADIO_NPARTITIONS, LW_SHARED);
    HASH_SEQ_STATUS iostat;
    hash_seq_init(&iostat, pgstat_get_shared_state()->threadio_hash);
    PgStatSharedThreadIOEntry* sthreadio = NULL;
    while ((sthreadio = (PgStatSharedThreadIOEntry*)hash_seq_search(&iostat)) != NULL) {
        PgStat_StatThreadIOKey key;
        key.role_id = sthreadio->key.role_id;
        key.object_id = sthreadio->key.object_id;
        key.context_id = sthreadio->key.context_id;
        bool found = false;
        PgStat_StatThreadIOEntry* threadioentry =
            (PgStat_StatThreadIOEntry*)hash_search(dbentry->thread_io, &key, HASH_ENTER, &found);
        if (threadioentry != NULL) {
            threadioentry->key = key;
            threadioentry->stats = sthreadio->stats;
        }
    }
    pgstat_unlock_all(pgstat_get_shared_state()->threadio_locks, PGSTAT_THREADIO_NPARTITIONS);
}

void pgstat_shared_copy_snapshot(Oid onlydb, MemoryContext mcxt, HTAB** out_dbhash, PgStat_GlobalStats* out_global)
{
    if (out_dbhash != NULL) {
        *out_dbhash = NULL;
    }
    if (out_global != NULL) {
        (void)memset_s(out_global, sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
    }
    if (pgstat_get_shared_state() == NULL || mcxt == NULL) {
        return;
    }

    MemoryContext old = MemoryContextSwitchTo(mcxt);
    HASHCTL hash_ctl;
    errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PgStat_StatDBEntry);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = mcxt;
    HTAB* dbhash =
        hash_create("Databases hash", PGSTAT_SNAPSHOT_DB_HASH_SIZE,
            &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    if (out_dbhash != NULL) {
        *out_dbhash = dbhash;
    }

    LWLock* glock = pgstat_shared_global_lock();
    if (glock != NULL && out_global != NULL) {
        LWLockAcquire(glock, LW_SHARED);
        *out_global = pgstat_get_shared_state()->global_stats;
        LWLockRelease(glock);
    }
    copy_snapshot_fill_db_entries(dbhash, mcxt, onlydb);
    copy_snapshot_fill_tab_entries(dbhash, mcxt, onlydb);
    copy_snapshot_fill_func_entries(dbhash, mcxt, onlydb);
    copy_snapshot_fill_threadio_entries(dbhash, mcxt, onlydb);
    MemoryContextSwitchTo(old);
}

static void pgstat_shared_clear_all_hashes(void)
{
    if (pgstat_get_shared_state() == NULL) {
        return;
    }

    pgstat_lock_all(pgstat_get_shared_state()->db_locks, PGSTAT_DB_NPARTITIONS, LW_EXCLUSIVE);
    HASH_SEQ_STATUS dbseq;
    hash_seq_init(&dbseq, pgstat_get_shared_state()->db_hash);
    PgStatSharedDBEntry* entry = NULL;
    while ((entry = (PgStatSharedDBEntry*)hash_seq_search(&dbseq)) != NULL) {
        Oid dbid = entry->databaseid;
        (void)hash_search(pgstat_get_shared_state()->db_hash, &dbid, HASH_REMOVE, NULL);
    }
    pgstat_unlock_all(pgstat_get_shared_state()->db_locks, PGSTAT_DB_NPARTITIONS);

    pgstat_lock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS, LW_EXCLUSIVE);
    HASH_SEQ_STATUS tabseq;
    hash_seq_init(&tabseq, pgstat_get_shared_state()->tab_hash);
    PgStatSharedTabEntry* tab_entry = NULL;
    while ((tab_entry = (PgStatSharedTabEntry*)hash_seq_search(&tabseq)) != NULL) {
        PgStatSharedTabKey key = tab_entry->key;
        (void)hash_search(pgstat_get_shared_state()->tab_hash, &key, HASH_REMOVE, NULL);
    }
    pgstat_unlock_all(pgstat_get_shared_state()->tab_locks, PGSTAT_TAB_NPARTITIONS);

    pgstat_lock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS, LW_EXCLUSIVE);
    HASH_SEQ_STATUS funcseq;
    hash_seq_init(&funcseq, pgstat_get_shared_state()->func_hash);
    PgStatSharedFuncEntry* func_entry = NULL;
    while ((func_entry = (PgStatSharedFuncEntry*)hash_seq_search(&funcseq)) != NULL) {
        PgStatSharedFuncKey key = func_entry->key;
        (void)hash_search(pgstat_get_shared_state()->func_hash, &key, HASH_REMOVE, NULL);
    }
    pgstat_unlock_all(pgstat_get_shared_state()->func_locks, PGSTAT_FUNC_NPARTITIONS);
}

static void import_snapshot_copy_tab_to_shared(PgStatSharedTabEntry* stab, const PgStat_StatTabEntry* tabentry)
{
    stab->numscans = tabentry->numscans;
    stab->lastscan = tabentry->lastscan;
    stab->tuples_returned = tabentry->tuples_returned;
    stab->tuples_fetched = tabentry->tuples_fetched;
    stab->tuples_inserted = tabentry->tuples_inserted;
    stab->tuples_updated = tabentry->tuples_updated;
    stab->tuples_deleted = tabentry->tuples_deleted;
    stab->tuples_inplace_updated = tabentry->tuples_inplace_updated;
    stab->tuples_hot_updated = tabentry->tuples_hot_updated;
    stab->n_live_tuples = tabentry->n_live_tuples;
    stab->n_dead_tuples = tabentry->n_dead_tuples;
    stab->changes_since_analyze = tabentry->changes_since_analyze;
    stab->blocks_fetched = tabentry->blocks_fetched;
    stab->blocks_hit = tabentry->blocks_hit;
    stab->cu_mem_hit = tabentry->cu_mem_hit;
    stab->cu_hdd_sync = tabentry->cu_hdd_sync;
    stab->cu_hdd_asyn = tabentry->cu_hdd_asyn;
    stab->success_prune_cnt = tabentry->success_prune_cnt;
    stab->total_prune_cnt = tabentry->total_prune_cnt;
    stab->vacuum_timestamp = tabentry->vacuum_timestamp;
    stab->vacuum_count = tabentry->vacuum_count;
    stab->autovac_vacuum_timestamp = tabentry->autovac_vacuum_timestamp;
    stab->autovac_vacuum_count = tabentry->autovac_vacuum_count;
    stab->analyze_timestamp = tabentry->analyze_timestamp;
    stab->analyze_count = tabentry->analyze_count;
    stab->autovac_analyze_timestamp = tabentry->autovac_analyze_timestamp;
    stab->autovac_analyze_count = tabentry->autovac_analyze_count;
    stab->data_changed_timestamp = tabentry->data_changed_timestamp;
    stab->autovac_status = tabentry->autovac_status;
}

static void import_snapshot_copy_tables_to_shmem(Oid databaseid, HTAB* tables)
{
    if (tables == NULL) {
        return;
    }
    HASH_SEQ_STATUS tstat;
    hash_seq_init(&tstat, tables);
    PgStat_StatTabEntry* tabentry = NULL;
    while ((tabentry = (PgStat_StatTabEntry*)hash_seq_search(&tstat)) != NULL) {
        LWLock* tlock = NULL;
        PgStatSharedTabKey key;
        key.databaseid = databaseid;
        key.tableid = tabentry->tablekey.tableid;
        key.statFlag = tabentry->tablekey.statFlag;
        PgStatSharedTabEntry* stab =
            pgstat_shared_get_tab_entry(&key, true, LW_EXCLUSIVE, &tlock, NULL);
        if (stab != NULL) {
            import_snapshot_copy_tab_to_shared(stab, tabentry);
            pgstat_shared_release_lock(tlock);
        }
    }
}

static void import_snapshot_copy_functions_to_shmem(Oid databaseid, HTAB* functions)
{
    if (functions == NULL) {
        return;
    }
    HASH_SEQ_STATUS fstat;
    hash_seq_init(&fstat, functions);
    PgStat_StatFuncEntry* funcentry = NULL;
    while ((funcentry = (PgStat_StatFuncEntry*)hash_seq_search(&fstat)) != NULL) {
        LWLock* flock = NULL;
        PgStatSharedFuncKey key;
        key.databaseid = databaseid;
        key.functionid = funcentry->functionid;
        PgStatSharedFuncEntry* sfunc =
            pgstat_shared_get_func_entry(&key, true, LW_EXCLUSIVE, &flock, NULL);
        if (sfunc != NULL) {
            sfunc->f_numcalls = funcentry->f_numcalls;
            sfunc->f_total_time = funcentry->f_total_time;
            sfunc->f_self_time = funcentry->f_self_time;
            pgstat_shared_release_lock(flock);
        }
    }
}

static void import_snapshot_copy_one_db(PgStat_StatDBEntry* dbentry)
{
    LWLock* dblock = NULL;
    PgStatSharedDBEntry* sdb =
        pgstat_shared_get_db_entry(dbentry->databaseid, true, LW_EXCLUSIVE, &dblock, NULL);
    if (sdb == NULL) {
        return;
    }
    sdb->n_xact_commit = dbentry->n_xact_commit;
    sdb->n_xact_rollback = dbentry->n_xact_rollback;
    sdb->n_blocks_fetched = dbentry->n_blocks_fetched;
    sdb->n_blocks_hit = dbentry->n_blocks_hit;
    sdb->n_cu_mem_hit = dbentry->n_cu_mem_hit;
    sdb->n_cu_hdd_sync = dbentry->n_cu_hdd_sync;
    sdb->n_cu_hdd_asyn = dbentry->n_cu_hdd_asyn;
    sdb->n_tuples_returned = dbentry->n_tuples_returned;
    sdb->n_tuples_fetched = dbentry->n_tuples_fetched;
    sdb->n_tuples_inserted = dbentry->n_tuples_inserted;
    sdb->n_tuples_updated = dbentry->n_tuples_updated;
    sdb->n_tuples_deleted = dbentry->n_tuples_deleted;
    sdb->last_autovac_time = dbentry->last_autovac_time;
    sdb->n_conflict_tablespace = dbentry->n_conflict_tablespace;
    sdb->n_conflict_lock = dbentry->n_conflict_lock;
    sdb->n_conflict_snapshot = dbentry->n_conflict_snapshot;
    sdb->n_conflict_bufferpin = dbentry->n_conflict_bufferpin;
    sdb->n_conflict_startup_deadlock = dbentry->n_conflict_startup_deadlock;
    sdb->n_temp_files = dbentry->n_temp_files;
    sdb->n_temp_bytes = dbentry->n_temp_bytes;
    sdb->n_deadlocks = dbentry->n_deadlocks;
    sdb->n_block_read_time = dbentry->n_block_read_time;
    sdb->n_block_write_time = dbentry->n_block_write_time;
    sdb->n_mem_mbytes_reserved = dbentry->n_mem_mbytes_reserved;
    sdb->stat_reset_timestamp = dbentry->stat_reset_timestamp;
    pgstat_shared_release_lock(dblock);
    import_snapshot_copy_tables_to_shmem(dbentry->databaseid, dbentry->tables);
    import_snapshot_copy_functions_to_shmem(dbentry->databaseid, dbentry->functions);
}

void pgstat_shared_import_snapshot(HTAB* dbhash, const PgStat_GlobalStats* global)
{
    if (pgstat_get_shared_state() == NULL || dbhash == NULL) {
        return;
    }
    pgstat_shared_clear_all_hashes();
    if (global != NULL) {
        LWLock* glock = pgstat_shared_global_lock();
        if (glock != NULL) {
            LWLockAcquire(glock, LW_EXCLUSIVE);
            pgstat_get_shared_state()->global_stats = *global;
            LWLockRelease(glock);
        }
    }
    HASH_SEQ_STATUS hstat;
    hash_seq_init(&hstat, dbhash);
    PgStat_StatDBEntry* dbentry = NULL;
    while ((dbentry = (PgStat_StatDBEntry*)hash_seq_search(&hstat)) != NULL)
        import_snapshot_copy_one_db(dbentry);
}
