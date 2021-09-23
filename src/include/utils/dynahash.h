/* -------------------------------------------------------------------------
 *
 * dynahash
 *	  openGauss dynahash.h file definitions
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dynahash.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DYNAHASH_H
#define DYNAHASH_H

#include "storage/buf/buf_internals.h"
#include "storage/lock/s_lock.h"
#include "utils/hsearch.h"

/* Number of freelists to be used for a partitioned hash table. */
#define NUM_FREELISTS 32

/* A hash bucket is a linked list of HASHELEMENTs */
typedef HASHELEMENT* HASHBUCKET;

/* A hash segment is an array of bucket headers */
typedef HASHBUCKET* HASHSEGMENT;

/*
 * Using array of FreeListData instead of separate arrays of mutexes, nentries
 * and freeLists prevents, at least partially, sharing one cache line between
 * different mutexes (see below).
 */
typedef struct {
    slock_t mutex;         /* spinlock */
    long nentries;         /* number of entries */
    HASHELEMENT* freeList; /* list of free elements */
} FreeListData;

/*
 * Header structure for a hash table --- contains all changeable info
 *
 * In a shared-memory hash table, the HASHHDR is in shared memory, while
 * each backend has a local HTAB struct.  For a non-shared table, there isn't
 * any functional difference between HASHHDR and HTAB, but we separate them
 * anyway to share code between shared and non-shared tables.
 */
struct HASHHDR {
    /*
     * The freelist can become a point of contention on high-concurrency hash
     * tables, so we use an array of freelist, each with its own mutex and
     * nentries count, instead of just a single one.
     *
     * If hash table is not partitioned only freeList[0] is used and spinlocks
     * are not used at all.
     */
    FreeListData freeList[NUM_FREELISTS];

    /* These fields can change, but not in a partitioned table */
    /* Also, dsize can't change in a shared table, even if unpartitioned */
    long dsize;        /* directory size */
    long nsegs;        /* number of allocated segments (<= dsize) */
    uint32 max_bucket; /* ID of maximum bucket in use */
    uint32 high_mask;  /* mask to modulo into entire table */
    uint32 low_mask;   /* mask to modulo into lower half of table */

    /* These fields are fixed at hashtable creation */
    Size keysize;        /* hash key length in bytes */
    Size entrysize;      /* total user element size in bytes */
    long num_partitions; /* # partitions (must be power of 2), or 0 */
    long ffactor;        /* target fill factor */
    long max_dsize;      /* 'dsize' limit if directory is fixed size */
    long ssize;          /* segment size --- must be power of 2 */
    int sshift;          /* segment shift = log2(ssize) */
    int nelem_alloc;     /* number of entries to allocate at once */

#ifdef HASH_STATISTICS

    /*
     * Count statistics here.  NB: stats code doesn't bother with mutex, so
     * counts could be corrupted a bit in a partitioned table.
     */
    long accesses;
    long collisions;
#endif
};

/* the offset of the last padding if exists*/
#define HTAB_PAD_OFFSET 104

/*
 * Top control structure for a hashtable --- in a shared table, each backend
 * has its own copy (OK since no fields change at runtime)
 */
struct HTAB {
    HASHHDR* hctl;           /* => shared control information */
    HASHSEGMENT* dir;        /* directory of segment starts */
    HashValueFunc hash;      /* hash function */
    HashCompareFunc match;   /* key comparison function */
    HashCopyFunc keycopy;    /* key copying function */
    HashAllocFunc alloc;     /* memory allocator */
    HashDeallocFunc dealloc; /* memory deallocator */
    MemoryContext hcxt;      /* memory context if default allocator used */
    char* tabname;           /* table name (for error messages) */
    bool isshared;           /* true if table is in shared memory */
    bool isfixed;            /* if true, don't enlarge */

    /* freezing a shared table isn't allowed, so we can keep state here */
    bool frozen; /* true = no more inserts allowed */

    /* We keep local copies of these fixed values to reduce contention */
    Size keysize; /* hash key length in bytes */
    long ssize;   /* segment size --- must be power of 2 */
    int sshift;   /* segment shift = log2(ssize) */
#ifdef __aarch64__
    char pad[PG_CACHE_LINE_SIZE - HTAB_PAD_OFFSET];
#endif
};

extern int my_log2(long num);
template <HASHACTION action>
void* buf_hash_operate(HTAB* hashp, const BufferTag* keyPtr, uint32 hashvalue, bool* foundPtr);
#endif /* DYNAHASH_H */
