/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
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
    return strcmp(dek1, dek2);
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
    return tde_cache == NULL;
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
            remove_cache_entry(item);
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
    Size plain_size = strlen(tde_cache_entry->dek_plaintext) + 1;
    if (plain_size != DEK_PLAIN_HEX_LEN + 1) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid TDE cache entry size")));
    }
    char* volatile key = NULL;
    char* volatile plain = NULL;
    LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
    PG_TRY();
    {
        bool found = false;
        TDECacheEntry* entry = (TDECacheEntry*)hash_search(tde_cache,
            (const void*)&tde_cache_entry->key_cipher, HASH_FIND, &found);
        if (found && GetCurrentTimestamp() - entry->timestamp >= USECS_PER_HOUR) {
            remove_cache_entry(entry);
            found = false;
        }
        if (!found) {
            /* Allocate only on a miss, before publishing; the catch owns both buffers. */
            key = MemoryContextStrdup(tde_cache_mem, tde_cache_entry->key_cipher);
            plain = MemoryContextStrdup(tde_cache_mem, tde_cache_entry->dek_plaintext);
            entry = (TDECacheEntry*)hash_search(tde_cache, (const void*)&key, HASH_ENTER, &found);
            if (entry == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("could not insert TDE cache entry")));
            }
            entry->dek_plaintext = (char*)plain;
            entry->timestamp = tde_cache_entry->timestamp;
            key = NULL;
            plain = NULL;
        } else {
            /* A complete envelope identifies immutable key material. */
            if (strcmp(entry->dek_plaintext, tde_cache_entry->dek_plaintext) != 0) {
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("conflicting plaintext for a TDE envelope")));
            }
        }
    }
    PG_CATCH();
    {
        LWLockRelease(TDEKeyCacheLock);
        if (plain != NULL) {
            clean_cache_entry_value((char*)plain);
        }
        if (key != NULL) {
            pfree((void*)key);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
    LWLockRelease(TDEKeyCacheLock);
    return true;
}

/* Caller holds TDEKeyCacheLock exclusively; copy pointers before removing the entry. */
void TDEKeyStorage::remove_cache_entry(TDECacheEntry* entry)
{
    char* key = entry->key_cipher;
    char* plain = entry->dek_plaintext;
    if (hash_search(tde_cache, (const void*)&key, HASH_REMOVE, NULL) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("could not remove TDE key cache entry")));
    }
    clean_cache_entry_value(plain);
    pfree(key);
}

bool TDEKeyStorage::search_cache(const char* dek_cipher, char* dek_plaintext, Size output_size)
{
    bool found = false;
    LWLockAcquire(TDEKeyCacheLock, LW_SHARED);
    TDECacheEntry* entry = NULL;
    entry = reinterpret_cast<TDECacheEntry*>(hash_search(tde_cache, (const void*)&dek_cipher, HASH_FIND, &found));
    if (!found) {
        LWLockRelease(TDEKeyCacheLock);
        return false;
    }
    if (GetCurrentTimestamp() - entry->timestamp >= USECS_PER_HOUR) {
        LWLockRelease(TDEKeyCacheLock);
        return false;
    }
    Size key_len = strlen(entry->dek_plaintext);
    if (key_len + 1 > output_size) {
        LWLockRelease(TDEKeyCacheLock);
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("TDE key cache value exceeds the output buffer")));
    }
    errno_t rc = memcpy_s(dek_plaintext, output_size, entry->dek_plaintext, key_len + 1);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    LWLockRelease(TDEKeyCacheLock);
    return true;
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
            /* Fixed lifetime from insertion, regardless of intervening cache hits. */
            if ((cur_timestamp - item->timestamp) >= USECS_PER_HOUR) {
                remove_cache_entry(item);
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
    return tde_buffer_cache == NULL;
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
            TdeInfo* info = item->tde_info;
            if (hash_search(tde_buffer_cache, (const void*)&item->tde_node, HASH_REMOVE, NULL) == NULL) {
                LWLockRelease(TDEKeyCacheLock);
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_DATA_EXCEPTION), 
                    errmsg("clear TDE buffer cache key failed"), errdetail("N/A"), 
                    errcause("TDE buffer cache crash"), 
                    erraction("check TDE_BUFFER_CACHE_CONTEXT or system cache")));
            }
            pfree(info);
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
    TdeInfo value = {0};
    errno_t rc = strcpy_s(value.dek_cipher, DEK_CIPHER_LEN, tde_info->dek_cipher);
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(value.cmk_id, CMK_ID_LEN, tde_info->cmk_id);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    value.algo = tde_info->algo;
    if (tde_rnode.bucketNode != InvalidBktId) {
        tde_rnode.bucketNode = SegmentBktId;
    }
    TdeInfo* volatile allocated = NULL;
    LWLockAcquire(TDEKeyCacheLock, LW_EXCLUSIVE);
    PG_TRY();
    {
        bool found = false;
        TdeFileNodeEntry* entry = (TdeFileNodeEntry*)hash_search(tde_buffer_cache,
            (const void*)&tde_rnode, HASH_FIND, &found);
        if (!found) {
            /* Finish allocating the value before making the hash entry visible. */
            allocated = (TdeInfo*)MemoryContextAlloc(tde_buffer_mem, sizeof(TdeInfo));
            *allocated = value;
            entry = (TdeFileNodeEntry*)hash_search(tde_buffer_cache, (const void*)&tde_rnode, HASH_ENTER, &found);
            if (entry == NULL) {
                ereport(ERROR, (errmsg("could not insert TDE relation cache entry")));
            }
            entry->tde_info = (TdeInfo*)allocated;
            allocated = NULL;
        } else {
            *entry->tde_info = value;
        }
    }
    PG_CATCH();
    {
        LWLockRelease(TDEKeyCacheLock);
        if (allocated != NULL) {
            pfree((void*)allocated);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
    LWLockRelease(TDEKeyCacheLock);
    return true;
}

void TDEBufferCache::search_cache(RelFileNode tde_rnode, TdeInfo *tde_info)
{
    bool found = false;
    if (tde_rnode.bucketNode != InvalidBktId) {
        tde_rnode.bucketNode = SegmentBktId;
    }
    LWLockAcquire(TDEKeyCacheLock, LW_SHARED);
    TdeFileNodeEntry* entry = NULL;
    entry = reinterpret_cast<TdeFileNodeEntry*>(hash_search(tde_buffer_cache, (const void*)&tde_rnode, 
        HASH_FIND, &found));
    if (!found) {
        LWLockRelease(TDEKeyCacheLock);
        return;
    }

    errno_t rc = memcpy_s(tde_info, sizeof(TdeInfo), entry->tde_info, sizeof(TdeInfo));
    securec_check(rc, "\0", "\0");
    LWLockRelease(TDEKeyCacheLock);
    return;
}
}
