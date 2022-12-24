/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * tde_key_storage.cpp
 *    TDE key storage is the external interface of key storage
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/tde_key_storage.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tde_key_management/tde_key_storage.h"
#include "knl/knl_instance.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "access/hash.h"
#include "storage/smgr/relfilenode_hash.h"

namespace TDE {
void TDEKeyStorage::init()
{
    if (tde_cache_mem == nullptr) {
        tde_cache_mem = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem, "TDE_CACHE_CONTEXT",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }

    if (tde_cache != NULL) {
        return;
    }

    HASHCTL tde_ctl;
    errno_t rc = memset_s(&tde_ctl, sizeof(tde_ctl), 0, sizeof(tde_ctl));
    securec_check(rc, "", "");
    tde_ctl.keysize = sizeof(char *);
    tde_ctl.entrysize = sizeof(TDECacheEntry);
    tde_ctl.hash = (HashValueFunc)tde_cache_entry_hash_func;
    tde_ctl.match = (HashCompareFunc)tde_cache_entry_match_func;
    tde_ctl.hcxt = tde_cache_mem;
    int flags = HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE;
    tde_cache = hash_create(tde_cache_name, max_bukect, &tde_ctl, flags);
    if (tde_cache == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FUNCTION_HASH_NOT_INITED),
            errmsg("could not initialize TDE key manager hash table")));
    }
}

uint32 TDEKeyStorage::tde_cache_entry_hash_func(const void* key, Size keysize)
{
    Assert(keysize > 0);
    Assert(key != NULL);

    const char *dek = *(char **)key;
    int s_len = strlen(dek);
    return (DatumGetUInt32(hash_any((const unsigned char *)dek, s_len)));
}

int TDEKeyStorage::tde_cache_entry_match_func(const void* key1, const void* key2, Size keySize)
{
    Assert(keySize > 0);
    Assert(key1 != NULL);
    Assert(key2 != NULL);

    /* de-reference char ** to char * to do comparison */
    const char *dek1 = *(char **)key1;
    const char *dek2 = *(char **)key2;
    int dek1_len = strlen(dek1);
    int dek2_len = strlen(dek2);
    return (strncmp(dek1, dek2, ((dek1_len > dek2_len) ? dek1_len : dek2_len)));
}

TDEKeyStorage::TDEKeyStorage()
{
    tde_cache_mem = nullptr;
    tde_cache = NULL;
}

TDEKeyStorage::~TDEKeyStorage()
{
    reset();
    if (tde_cache_mem != nullptr) {
        MemoryContextDelete(tde_cache_mem);
        tde_cache_mem = nullptr;
    }
}

bool TDEKeyStorage::empty()
{
    if (tde_cache == NULL) {
        return true;
    }
    return false;
}

void TDEKeyStorage::reset()
{
    clear();
    if (tde_cache != NULL) {
        LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
        HeapMemResetHash(tde_cache, tde_cache_name);
        tde_cache = NULL;
        LWLockRelease(TDEKeyCacheLock);
    }
}

void TDEKeyStorage::clear()
{
    if (tde_cache != NULL) {
        LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
        HASH_SEQ_STATUS scan_state;
        hash_seq_init(&scan_state, tde_cache);
        TDECacheEntry* item = NULL;
        while ((item = reinterpret_cast<TDECacheEntry*>(hash_seq_search(&scan_state))) != NULL) {
            clean_cache_entry_value(item->dek_plaintext);
            if (hash_search(tde_cache, (const void*)&item->key_cipher, HASH_REMOVE, NULL) == NULL) {
                LWLockRelease(TDEKeyCacheLock);
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("TDE Key storage clear cache: remove entry failed")));
            }
        }
        LWLockRelease(TDEKeyCacheLock);
    }
}

void TDEKeyStorage::reset_dek_plaintext(char* dek_plaintext)
{
    errno_t rc = 0;
    rc = memset_s(dek_plaintext, strlen(dek_plaintext), 0, strlen(dek_plaintext));
    securec_check(rc, "\0", "\0");
}

void TDEKeyStorage::clean_cache_entry_value(char* dek_plaintext)
{
    if (dek_plaintext != NULL) {
        reset_dek_plaintext(dek_plaintext);
        pfree_ext(dek_plaintext);
    }
}

bool TDEKeyStorage::insert_cache(TDECacheEntry* tde_cache_entry)
{
    bool found = false;
    errno_t rc = EOK;

    MemoryContext old = MemoryContextSwitchTo(tde_cache_mem);
    /* build tde cache key */
    char* tde_cache_key = (char*)palloc0(strlen(tde_cache_entry->key_cipher) + 1);
    rc = memcpy_s(tde_cache_key, (strlen(tde_cache_entry->key_cipher) + 1), tde_cache_entry->key_cipher, 
        (strlen(tde_cache_entry->key_cipher) + 1));
    securec_check(rc, "\0", "\0");

    /* insert hash table */
    LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
    TDECacheEntry* entry = NULL;
    entry = reinterpret_cast<TDECacheEntry*>(hash_search(tde_cache, (const void*)&tde_cache_key, HASH_FIND, &found));
    if (!found) {
        entry = reinterpret_cast<TDECacheEntry*>
            (hash_search(tde_cache, (const void*)&tde_cache_key, HASH_ENTER, &found));
        if (entry == NULL) {
            pfree_ext(tde_cache_key);
            LWLockRelease(TDEKeyCacheLock);
            MemoryContextSwitchTo(old);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("build TDE key cache hash table failed")));
            return false;
        }
        /* insert new cache key-value */
        entry->dek_plaintext = (char*)palloc0(strlen(tde_cache_entry->dek_plaintext) + 1);
        rc = memcpy_s(entry->dek_plaintext, (strlen(tde_cache_entry->dek_plaintext) + 1), 
            tde_cache_entry->dek_plaintext, (strlen(tde_cache_entry->dek_plaintext) + 1));
        securec_check(rc, "\0", "\0");
        entry->timestamp = tde_cache_entry->timestamp;
    } else {
        /* update cache key-value */
        rc = memcpy_s(entry->dek_plaintext, (strlen(tde_cache_entry->dek_plaintext) + 1), 
            tde_cache_entry->dek_plaintext, (strlen(tde_cache_entry->dek_plaintext) + 1));
        securec_check(rc, "\0", "\0");
        entry->timestamp = tde_cache_entry->timestamp;
    }
    MemoryContextSwitchTo(old);
    LWLockRelease(TDEKeyCacheLock);
    return true;
}

char* TDEKeyStorage::search_cache(const char* dek_cipher)
{
    bool found = false;
    MemoryContext old = MemoryContextSwitchTo(tde_cache_mem);
    LWLockAcquire(TDEKeyCacheLock, LW_SHARED);
    TDECacheEntry* entry = NULL;
    entry = reinterpret_cast<TDECacheEntry*>(hash_search(tde_cache, (const void*)&dek_cipher, HASH_FIND, &found));
    if (!found) {
        MemoryContextSwitchTo(old);
        LWLockRelease(TDEKeyCacheLock);
        return NULL;
    }
    MemoryContextSwitchTo(old);
    LWLockRelease(TDEKeyCacheLock);
    return entry->dek_plaintext;
}

void TDEKeyStorage::cache_watch_dog()
{
    TimestampTz cur_timestamp = 0;
    if (tde_cache != NULL) {
        cur_timestamp = GetCurrentTimestamp();
        LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
        HASH_SEQ_STATUS scan_state;
        hash_seq_init(&scan_state, tde_cache);
        TDECacheEntry* item = NULL;
        while ((item = reinterpret_cast<TDECacheEntry*>(hash_seq_search(&scan_state))) != NULL) {
            if ((cur_timestamp - item->timestamp) > USECS_PER_HOUR) {
                clean_cache_entry_value(item->dek_plaintext);
                if (hash_search(tde_cache, (const void*)&item->key_cipher, HASH_REMOVE, NULL) == NULL) {
                    LWLockRelease(TDEKeyCacheLock);
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), 
                        errmsg("TDE Key storage WatchDog cache: remove entry failed")));
                }
            }
        }
        LWLockRelease(TDEKeyCacheLock);
    }
    return;
}

/* TDE buffer rel file node cache management */
TDEBufferCache::TDEBufferCache()
{
    tde_buffer_mem = nullptr;
    tde_buffer_cache = NULL;
}

TDEBufferCache::~TDEBufferCache()
{
    reset();
    if (tde_buffer_mem != nullptr) {
        MemoryContextDelete(tde_buffer_mem);
        tde_buffer_mem = nullptr;
    }
}

bool TDEBufferCache::empty()
{
    if (tde_buffer_cache == NULL) {
        return true;
    }
    return false;
}

void TDEBufferCache::reset()
{
    clear();
    if (tde_buffer_cache != NULL) {
        LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
        HeapMemResetHash(tde_buffer_cache, tde_buffer_name);
        tde_buffer_cache = NULL;
        LWLockRelease(TDEKeyCacheLock);
    }
}

void TDEBufferCache::clear()
{
    if (tde_buffer_cache != NULL) {
        LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
        HASH_SEQ_STATUS scan_state;
        hash_seq_init(&scan_state, tde_buffer_cache);
        TdeFileNodeEntry* item = NULL;
        while ((item = reinterpret_cast<TdeFileNodeEntry*>(hash_seq_search(&scan_state))) != NULL) {
            if (hash_search(tde_buffer_cache, (const void*)&item->tde_node, HASH_REMOVE, NULL) == NULL) {
                LWLockRelease(TDEKeyCacheLock);
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_DATA_EXCEPTION), 
                    errmsg("clear TDE buffer cache key failed"), errdetail("N/A"), 
                    errcause("TDE buffer cache crash"), 
                    erraction("check TDE_BUFFER_CACHE_CONTEXT or system cache")));
            }
        }
        LWLockRelease(TDEKeyCacheLock);
    }
}

void TDEBufferCache::init()
{
    if (tde_buffer_mem == nullptr) {
        tde_buffer_mem = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem, "TDE_BUFFER_CACHE_CONTEXT",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }
    if (tde_buffer_cache != NULL) {
        return;
    }
    /* initialize the hash table */
    HASHCTL tde_buffer_ctl;
    errno_t rc = memset_s(&tde_buffer_ctl, sizeof(tde_buffer_ctl), 0, sizeof(tde_buffer_ctl));
    securec_check(rc, "", "");
    tde_buffer_ctl.keysize = sizeof(RelFileNode);
    tde_buffer_ctl.entrysize = sizeof(TdeFileNodeEntry);
    tde_buffer_ctl.hash = file_node_ignore_opt_hash;
    tde_buffer_ctl.match = file_node_ignore_opt_match;
    tde_buffer_ctl.hcxt = tde_buffer_mem;
    tde_buffer_cache = hash_create(tde_buffer_name, max_bucket, &tde_buffer_ctl, 
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
    if (tde_buffer_cache == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FUNCTION_HASH_NOT_INITED), 
            errmsg("init TDE buffer cache is NULL"), errdetail("N/A"), 
            errcause("initialize cache is failed"), erraction("check TDE_BUFFER_CACHE_CONTEXT or system cache")));
    }
}

bool TDEBufferCache::insert_cache(RelFileNode tde_rnode, TdeInfo *tde_info)
{
    bool found = false;
    errno_t rc = EOK;

    MemoryContext old = MemoryContextSwitchTo(tde_buffer_mem);
    /* build tde cache key */
    RelFileNode tde_key = tde_rnode;

    /* insert hash table */
    LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
    TdeFileNodeEntry *entry = NULL;
    entry = reinterpret_cast<TdeFileNodeEntry*>(hash_search(tde_buffer_cache, (const void*)&tde_key, 
        HASH_FIND, &found));
    if (!found) {
        entry = reinterpret_cast<TdeFileNodeEntry*>(hash_search(tde_buffer_cache, 
            (const void*)&tde_key, HASH_ENTER, &found));
        if (entry == NULL) {
            LWLockRelease(TDEKeyCacheLock);
            MemoryContextSwitchTo(old);
            return false;
        }
        /* insert new cache key-value */
        entry->tde_info = (TdeInfo*)palloc0(sizeof(TdeInfo));
        rc = strcpy_s(entry->tde_info->dek_cipher, DEK_CIPHER_LEN, tde_info->dek_cipher);
        securec_check(rc, "\0", "\0");
        rc = strcpy_s(entry->tde_info->cmk_id, CMK_ID_LEN, tde_info->cmk_id);
        securec_check(rc, "\0", "\0");
        entry->tde_info->algo = tde_info->algo;
    } else {
        /* update cache key-value */
        if (strncmp(entry->tde_info->dek_cipher, tde_info->dek_cipher, DEK_CIPHER_LEN) != 0) {
            rc = strcpy_s(entry->tde_info->dek_cipher, DEK_CIPHER_LEN, tde_info->dek_cipher);
            securec_check(rc, "\0", "\0");
        }
        if (strncmp(entry->tde_info->cmk_id, tde_info->cmk_id, CMK_ID_LEN) != 0) {
            rc = strcpy_s(entry->tde_info->cmk_id, CMK_ID_LEN, tde_info->cmk_id);
            securec_check(rc, "\0", "\0");
        }
        if (entry->tde_info->algo != tde_info->algo) {
            entry->tde_info->algo = tde_info->algo;
        }
    }
    MemoryContextSwitchTo(old);
    LWLockRelease(TDEKeyCacheLock);
    return true;
}

void TDEBufferCache::search_cache(RelFileNode tde_rnode, TdeInfo *tde_info)
{
    bool found = false;
    if (tde_rnode.bucketNode != InvalidBktId) {
        tde_rnode.bucketNode = SegmentBktId;
    }
    MemoryContext old = MemoryContextSwitchTo(tde_buffer_mem);
    LWLockAcquire(TDEKeyCacheLock, LW_SHARED);
    TdeFileNodeEntry* entry = NULL;
    entry = reinterpret_cast<TdeFileNodeEntry*>(hash_search(tde_buffer_cache, (const void*)&tde_rnode, 
        HASH_FIND, &found));
    if (!found) {
        MemoryContextSwitchTo(old);
        LWLockRelease(TDEKeyCacheLock);
        return;
    }

    errno_t rc = memcpy_s(tde_info, sizeof(TdeInfo), entry->tde_info, sizeof(TdeInfo));
    securec_check(rc, "\0", "\0");
    MemoryContextSwitchTo(old);
    LWLockRelease(TDEKeyCacheLock);
    return;
}
}
