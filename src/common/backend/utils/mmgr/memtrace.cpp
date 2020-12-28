/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * memtrace.cpp
 *
 * Used to track the memory usage
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/memtrace.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "utils/aset.h"
#include "utils/memprot.h"
#include "utils/memtrace.h"
#include "workload/cpwlm.h"

#define MAX_CONTEXT_NUM 16

uint32 MemoryInfoHashFunc(const void* key, Size keysize)
{
    uint64* val = (uint64*)key;
    uint32 val2 = (*val & 0x0000000ffffffffe) >> 3;
    return val2;
}

int MemoryInfoHashMatch(const void* key1, const void* key2, Size keysize)
{
    uint64* val1 = (uint64*)key1;
    uint64* val2 = (uint64*)key2;
    if (*val1 == *val2) {
        return 0;
    } else {
        return 1;
    }
}

void CreateTrackMemoryInfoHash()
{
#define NUM_FREELISTS 32
    /* create the hash table for tracking memory alloc info */
    int64 info_entry_size;
    HASHCTL hctl;
    errno_t rc = memset_s(&hctl, sizeof(hctl), 0, sizeof(hctl));
    securec_check(rc, "\0", "\0");

    hctl.keysize = sizeof(void*);
    hctl.entrysize = sizeof(MemoryAllocInfo);
    hctl.hash = MemoryInfoHashFunc;
    hctl.match = MemoryInfoHashMatch;
    hctl.hcxt = g_instance.stat_cxt.track_context;
    hctl.num_partitions = NUM_FREELISTS;
    /**
     * we suppose that 50% allocator is tracked memoryContext, and each allocator
     * is 256 Byte, so the max entry size is show as below.
     */
    info_entry_size = (((uint64)maxChunksPerProcess << chunkSizeInBits) / 256) * 0.5;
    ereport(LOG, (errmsg("memory info hash table size is: %ld", info_entry_size)));

    g_instance.stat_cxt.track_memory_info_hash = hash_create("Track MemoryInfo hash",
        info_entry_size, &hctl, HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_CONTEXT | HASH_PARTITION);
}

void CreateTrackMemoryContextHash()
{
    if (g_instance.stat_cxt.track_context_hash != NULL) {
        return;
    }

    HASHCTL hctl;
    errno_t rc = memset_s(&hctl, sizeof(hctl), 0, sizeof(hctl));
    securec_check(rc, "\0", "\0");

    hctl.keysize = NAMEDATALEN;
    hctl.entrysize = NAMEDATALEN;
    hctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB);

    g_instance.stat_cxt.track_context_hash = hash_create("Track MemoryContext hash",
        MAX_CONTEXT_NUM, &hctl, HASH_ELEM | HASH_SHRCTX | HASH_CONTEXT);
}

void CopyContextName(char* source)
{
    char* tmp;
    char name[NAMEDATALEN] = {0};
    errno_t rc;

    tmp = trim(source);  // ignore space before and end
    rc = strncpy_s(name, NAMEDATALEN, tmp, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    if (strlen(tmp) > NAMEDATALEN - 1) {
        ereport(WARNING, (errmsg("the context_name(%s) will be cut off to %s",
            tmp, name)));
    }
    (void)hash_search(g_instance.stat_cxt.track_context_hash, name, HASH_ENTER, NULL);
}

bool ParseTrackMemory(const char* context_name)
{
    int len = strlen(context_name) + 1;
    char* origin_name = (char*)palloc(len);
    int count = 0;
    errno_t rc;

    rc = strncpy_s(origin_name, len, context_name, len - 1);
    securec_check(rc, "\0", "\0");
    while (origin_name != NULL) {
        if (count >= MAX_CONTEXT_NUM) {
            ereport(WARNING, (errmsg("The number of memoryContext could not exceed 16.")));
            return false;
        }
        char* tmp = strrchr(origin_name, ',');
        if (tmp != NULL) {
            tmp[0] = '\0';
            CopyContextName(tmp + 1);
        } else {
            CopyContextName(origin_name);
            origin_name = NULL;
        }
        count++;
    }
    
    return true;
}

void RemoveHashContext()
{
    HASH_SEQ_STATUS hash_seq;
    char* entry = NULL;

    CreateTrackMemoryContextHash();

    /* get the sum of same alloc location */
    hash_seq_init(&hash_seq, g_instance.stat_cxt.track_context_hash);
    while ((entry = (char*)hash_seq_search(&hash_seq)) != NULL) {
        (void)hash_search(g_instance.stat_cxt.track_context_hash, entry, HASH_REMOVE, NULL);
    }
}

inline void ResetTrackMemoryStructure()
{
    RemoveHashContext();
    g_instance.stat_cxt.track_memory_info_hash = NULL;
    MemoryContextReset(g_instance.stat_cxt.track_context);
}

bool TrackMemoryInfoInit(char* context_name)
{
    if (pthread_rwlock_wrlock(&g_instance.stat_cxt.track_memory_lock) != 0) {
        return false;
    }

    g_instance.stat_cxt.track_memory_inited = false;

    if (g_instance.stat_cxt.track_context == NULL) {
        /* init memory context for tracking memory alloc info */
        g_instance.stat_cxt.track_context = AllocSetContextCreate(
                                                g_instance.instance_context,
                                                "TrackMemoryContext",
                                                ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE,
                                                SHARED_CONTEXT
                                            );
    } else {
        ResetTrackMemoryStructure();
    }

    MemoryContext oldContext = MemoryContextSwitchTo(g_instance.stat_cxt.track_context);

    /* context_name is "" means stop this track. */
    if (trim(context_name)[0] == '\0') {
        (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
        (void)MemoryContextSwitchTo(oldContext);
        return true;
    }

    CreateTrackMemoryContextHash();
    CreateTrackMemoryInfoHash();

    /*
     * ParseTrackMemory return false means number of memory context is
     * exceed 16, this function call will not do anything. We should reset
     * the g_instance.stat_cxt.track_context to free not used memory.
     */
    if (!ParseTrackMemory(context_name)) {
        ResetTrackMemoryStructure();
        (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
        (void)MemoryContextSwitchTo(oldContext);
        return false;
    }

    g_instance.stat_cxt.track_memory_inited = true;

    (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);

    (void)MemoryContextSwitchTo(oldContext);

    return true;
}

Datum track_memory_context(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "track_memory_context");
    }

    char* context_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool result = true;
    result = TrackMemoryInfoInit(context_name);

    PG_RETURN_BOOL(result);
}

bool MemoryContextShouldTrack(const char* name)
{
    bool found = false;

    if (g_instance.stat_cxt.track_memory_inited == false) {
        return false;
    }

    /* Check is memory context in track hash table */
    (void)hash_search(g_instance.stat_cxt.track_context_hash, name, HASH_FIND, &found);
    if (found) {
        return true;
    }

    return false;
}

void InsertTrackMemoryInfo(const void* pointer, const MemoryContext context,
                           const char* file, int line, Size size)
{
    MemoryAllocInfo* entry = NULL;
    uint64 key = (uint64)pointer;
    bool found = false;
    errno_t rc;

    if (g_instance.stat_cxt.track_memory_inited == false) {
        return;
    }

    entry = (MemoryAllocInfo*)hash_search(g_instance.stat_cxt.track_memory_info_hash, &key, HASH_ENTER, &found);
    if (!found && entry != NULL) {
        entry->file = file;
        entry->line = line;
        entry->size = size;
        entry->context = context;
        rc = strncpy_s(entry->context_name, NAMEDATALEN, context->name, NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
    }
}

void RemoveTrackMemoryInfo(const void* pointer)
{
    uint64 key = (uint64)pointer;

    if (g_instance.stat_cxt.track_memory_inited == false) {
        return;
    }

    (void)hash_search(g_instance.stat_cxt.track_memory_info_hash, &key, HASH_REMOVE, NULL);
}

void RemoveTrackMemoryContext(const MemoryContext context)
{
    HASH_SEQ_STATUS hash_seq;
    MemoryAllocInfo* entry = NULL;

    if (g_instance.stat_cxt.track_memory_inited == false) {
        return;
    }

    /* remove all alloc info while memory context delete or reset */
    hash_seq_init(&hash_seq, g_instance.stat_cxt.track_memory_info_hash);
    while ((entry = (MemoryAllocInfo*)hash_seq_search(&hash_seq)) != NULL) {
        if (context == entry->context) {
            uint64 key = (uint64)entry->pointer;
            (void)hash_search(g_instance.stat_cxt.track_memory_info_hash, &key, HASH_REMOVE, NULL);
        }
    }
}

uint32 MemoryDetailHashFunc(const void* key, Size keysize)
{
    MemoryAllocDetailKey* val = (MemoryAllocDetailKey*)key;
    uint32 val1 = string_hash(val->name, NAMEDATALEN);
    uint32 val2 = string_hash(val->file, NAMEDATALEN);
    uint32 val3 = uint32_hash(&val->line, sizeof(int));
    return (val1 & (val2 + val3));
}

int MemoryDetailHashMatch(const void* key1, const void* key2, Size keysize)
{
    MemoryAllocDetailKey* val1 = (MemoryAllocDetailKey*)key1;
    MemoryAllocDetailKey* val2 = (MemoryAllocDetailKey*)key2;

    if (strcmp(val1->name, val2->name) != 0) {
        return 1;
    }
    if (strcmp(val1->file, val2->file) != 0) {
        return 1;
    }
    if (val1->line != val2->line) {
        return 1;
    }

    return 0;
}

HTAB* CreateTrackMemoryInfoDetailHash()
{
    HTAB* result = NULL;
    HASHCTL hctl;
    errno_t rc = memset_s(&hctl, sizeof(hctl), 0, sizeof(hctl));
    securec_check(rc, "\0", "\0");

    hctl.keysize = sizeof(MemoryAllocDetailKey);
    hctl.entrysize = sizeof(MemoryAllocDetail);
    hctl.hash = MemoryDetailHashFunc;
    hctl.match = MemoryDetailHashMatch;
    /* CurrentMemoryContext is funcctx->multi_call_memory_ctx */
    hctl.hcxt = CurrentMemoryContext;

    result = hash_create("Track MemoryInfo Detail hash",
        10000, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    return result;
}

MemoryAllocDetailList* GetMemoryTrackInfo()
{
    HTAB* detail_hash;
    MemoryAllocDetailList result;
    MemoryAllocInfo* entry;
    HASH_SEQ_STATUS hash_seq;
    bool found = false;
    errno_t rc;

    if (pthread_rwlock_wrlock(&g_instance.stat_cxt.track_memory_lock) != 0) {
        return NULL;
    }

    if (g_instance.stat_cxt.track_memory_inited == false) {
        (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
        return NULL;
    }

    detail_hash = CreateTrackMemoryInfoDetailHash();

    /* get the sum of same alloc location */
    hash_seq_init(&hash_seq, g_instance.stat_cxt.track_memory_info_hash);
    while ((entry = (MemoryAllocInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* this check means this pointer already be freed while go through hash table */
        if (entry->context == NULL) {
            if (entry->pointer != NULL) {
                (void)hash_search(g_instance.stat_cxt.track_memory_info_hash, entry->pointer, HASH_REMOVE, NULL);
            }
            continue;
        }
        MemoryAllocDetail* total_entry = NULL;
        MemoryAllocDetailKey entry_key;
        entry_key.file = entry->file;
        entry_key.line = entry->line;
        rc = strncpy_s(entry_key.name, NAMEDATALEN, entry->context_name, NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");

        total_entry = (MemoryAllocDetail*)hash_search(detail_hash, &entry_key, HASH_ENTER, &found);
        if (total_entry) {
            if (found) {
                total_entry->size += entry->size;
            } else {
                total_entry->size = entry->size;
            }
        }
    }

    (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);

    MemoryAllocDetail* node = NULL;
    result.next = NULL;
    hash_seq_init(&hash_seq, detail_hash);
    while ((node = (MemoryAllocDetail*)hash_seq_search(&hash_seq)) != NULL) {
        MemoryAllocDetailList* res_entry = (MemoryAllocDetailList*)palloc(sizeof(MemoryAllocDetailList));
        res_entry->entry = node;
        res_entry->next = result.next;
        result.next = res_entry;
    }

    return result.next;
}

