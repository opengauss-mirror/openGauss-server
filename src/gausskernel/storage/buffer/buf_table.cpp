/* -------------------------------------------------------------------------
 *
 * buf_table.cpp
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.	The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/buffer/buf_table.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "utils/dynahash.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"

extern uint32 hashquickany(uint32 seed, register const unsigned char *data, register int len);
/* entry for buffer lookup hashtable */
typedef struct {
    BufferTag key; /* Tag of a disk page */
    int id;        /* Associated buffer ID */
} BufferLookupEnt;

/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than g_instance.attr.attr_storage.NBuffers)
 */
Size BufTableShmemSize(int size)
{
    return hash_estimate_size(size, sizeof(BufferLookupEnt));
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than g_instance.attr.attr_storage.NBuffers)
 */
void InitBufTable(int size)
{
    HASHCTL info;

    /* assume no locking is needed yet
     *
     * BufferTag maps to Buffer
     */
    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(BufferLookupEnt);
    info.hash = tag_hash;
    info.num_partitions = NUM_BUFFER_PARTITIONS;

    t_thrd.storage_cxt.SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table", size, size, &info,
                                                     HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32 BufTableHashCode(BufferTag *tagPtr)
{
    return hashquickany(0xFFFFFFFF, (unsigned char *)tagPtr, sizeof(BufferTag));
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int BufTableLookup(BufferTag *tag, uint32 hashcode)
{
    BufferLookupEnt *result = NULL;

    result = (BufferLookupEnt *)buf_hash_operate<HASH_FIND>(t_thrd.storage_cxt.SharedBufHash, tag, hashcode, NULL);

    if (SECUREC_UNLIKELY(result == NULL)) {
        return -1;
    }

    return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.	If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int BufTableInsert(BufferTag *tag, uint32 hashcode, int buf_id)
{
    BufferLookupEnt *result = NULL;
    bool found = false;

    Assert(buf_id >= 0);            /* -1 is reserved for not-in-table */
    Assert(tag->blockNum != P_NEW); /* invalid tag */

    result = (BufferLookupEnt *)buf_hash_operate<HASH_ENTER>(t_thrd.storage_cxt.SharedBufHash, tag, hashcode, &found);

    if (found) { /* found something already in the table */
        return result->id;
    }

    result->id = buf_id;

    return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void BufTableDelete(BufferTag *tag, uint32 hashcode)
{
    BufferLookupEnt *result = NULL;

    result = (BufferLookupEnt *)buf_hash_operate<HASH_REMOVE>(t_thrd.storage_cxt.SharedBufHash, tag, hashcode, NULL);

    if (result == NULL) { /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("shared buffer hash table corrupted."))));
    }
}
