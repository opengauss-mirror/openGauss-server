/* -------------------------------------------------------------------------
 *
 * memnodes.h
 *	  openGauss memory context node definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/memnodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef MEMNODES_H
#define MEMNODES_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"

/*
 * MemoryContext
 *		A logical context in which memory allocations occur.
 *
 * MemoryContext itself is an abstract type that can have multiple
 * implementations, though for now we have only AllocSetContext.
 * The function pointers in MemoryContextMethods define one specific
 * implementation of MemoryContext --- they are a virtual function table
 * in C++ terms.
 *
 * Node types that are actual implementations of memory contexts must
 * begin with the same fields as MemoryContext.
 *
 * Note: for largely historical reasons, typedef MemoryContext is a pointer
 * to the context struct rather than the struct type itself.
 */
typedef struct MemoryContextMethods {
    void* (*alloc)(MemoryContext context, Size align, Size size, const char* file, int line);
    /* call this free_p in case someone #define's free() */
    void (*free_p)(MemoryContext context, void* pointer);
    void* (*realloc)(MemoryContext context, void* pointer, Size align, Size size, const char* file, int line);
    void (*init)(MemoryContext context);
    void (*reset)(MemoryContext context);
    void (*delete_context)(MemoryContext context);
    Size (*get_chunk_space)(MemoryContext context, void* pointer);
    bool (*is_empty)(MemoryContext context);
    void (*stats)(MemoryContext context, int level);
#ifdef MEMORY_CONTEXT_CHECKING
    void (*check)(MemoryContext context);
#endif
} MemoryContextMethods;

typedef struct MemoryTrackData* MemoryTrack; /* forward reference */

typedef struct MemoryTrackData {
    NodeTag type;           /* identifies exact kind of context */
    MemoryTrack parent;     /* NULL if no parent (toplevel context) */
    MemoryTrack firstchild; /* head of linked list of children */
    MemoryTrack nextchild;  /* next child of same parent */
    char* name;             /* context name */
    Size peakSpace;         /* the peak bytes allocated by this context */
    Size allBytesPeak;      /* the peak bytes allocated by this and its children's context */
    Size allBytesAlloc;     /* all bytes allocated by this and its children's context */
    Size allBytesFreed;     /* all bytes freed by this and its children's context */
    int sequentCount;       /* the sequent count when creating in the thread */   
#ifdef MEMORY_CONTEXT_CHECKING
    bool isTracking; /* flag to indicate which is tracked by memory_detail_tracking setting */
#endif
} MemoryTrackData;

typedef struct MemoryContextData {
    NodeTag type;                  /* identifies exact kind of context */
    bool allowInCritSection;       /* allow palloc in critical section */
    MemoryContextMethods* methods; /* virtual function table */
    MemoryContext parent;          /* NULL if no parent (toplevel context) */
    MemoryContext firstchild;      /* head of linked list of children */
    MemoryContext prevchild;       /* previous child of same parent */
    MemoryContext nextchild;       /* next child of same parent */
    char* name;                    /* context name (just for debugging) */
    pthread_rwlock_t lock;         /* lock to protect members if the context is shared */
    bool is_sealed;				   /* sealed context prevent any memory change */
    bool is_shared;                /* context is shared by threads */
    bool isReset;                  /* T = no space alloced since last reset */
    int level;                     /* context level */
    uint64 session_id;             /* session id of context owner */
    ThreadId thread_id;            /* thread id of context owner */   
    ListCell cell;                 /* cell to pointer to this context*/
} MemoryContextData;

#define MemoryContextIsShared(context) (((MemoryContextData*)(context))->is_shared)

#define MemoryContextLock(context)                                                                       \
    do {                                                                                                 \
        START_CRIT_SECTION();                                                                            \
        int err = pthread_rwlock_wrlock(&((MemoryContextData*)(context))->lock);                         \
        if (err != 0) {                                                                                  \
            END_CRIT_SECTION();                                                                          \
            ereport(ERROR,                                                                               \
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),                                                \
                     errmsg("system call failed when lock, errno:%d.", err)));                           \
        }                                                                                                \
    } while (0)

#define MemoryContextUnlock(context)                                                                     \
    do {                                                                                                 \
        int unlock_err = pthread_rwlock_unlock(&((MemoryContextData*)(context))->lock);                  \
        if (unlock_err != 0) {                                                                           \
            END_CRIT_SECTION();                                                                          \
            ereport(PANIC,                                                                               \
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),                                                \
                     errmsg("system call failed when unlock, errno:%d.", unlock_err)));                  \
        }                                                                                                \
        END_CRIT_SECTION();                                                                              \
    } while (0)

typedef struct AllocBlockData* AllocBlock; /* forward reference */
typedef struct AllocChunkData* AllocChunk;
typedef struct AsanBlockData* AsanBlock;
/*
 * AllocPointer
 *      Aligned pointer which may be a member of an allocation set.
 */
typedef void* AllocPointer;

#define ALLOCSET_NUM_FREELISTS 11

/*
 * AllocSetContext is our standard implementation of MemoryContext.
 *
 * Note: header.isReset means there is nothing for AllocSetReset to do.
 * This is different from the aset being physically empty (empty blocks list)
 * because we may still have a keeper block.  It's also different from the set
 * being logically empty, because we don't attempt to detect pfree'ing the
 * last active chunk.
 */
typedef struct AllocSetContext {
    MemoryContextData header; /* Standard memory-context fields */
    /* Info about storage allocated in this context: */
    AllocBlock blocks;                           /* head of list of blocks in this set */
    AllocChunk freelist[ALLOCSET_NUM_FREELISTS]; /* free chunk lists */
    /* Allocation parameters for this context: */
    Size initBlockSize;   /* initial block size */
    Size maxBlockSize;    /* maximum block size */
    Size nextBlockSize;   /* next block size to allocate */
    Size allocChunkLimit; /* effective chunk size limit */
    AllocBlock keeper;    /* if not NULL, keep this block over resets */
    Size totalSpace;      /* all bytes allocated by this context */
    Size freeSpace;       /* all bytes freed by this context */

    /* maximum memory allocation of MemoryContext.For more information,we could see @StackSetContext too. */
    Size maxSpaceSize;

    MemoryTrack track; /* used to track the memory allocation information */
} AllocSetContext;

typedef AllocSetContext* AllocSet;

/*
 * AllocBlock
 *		An AllocBlock is the unit of memory that is obtained by aset.c
 *		from malloc().	It contains one or more AllocChunks, which are
 *		the units requested by palloc() and freed by pfree().  AllocChunks
 *		cannot be returned to malloc() individually, instead they are put
 *		on freelists by pfree() and re-used by the next palloc() that has
 *		a matching request size.
 *
 *		AllocBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct AllocBlockData {
    AllocSet aset;   /* aset that owns this block */
    AllocBlock prev; /* prev block in aset's blocks list, if any */
    AllocBlock next; /* next block in aset's blocks list */
    char* freeptr;   /* start of free space in this block */
    char* endptr;    /* end of space in this block */
    Size allocSize;  /* allocated size */
#ifdef MEMORY_CONTEXT_CHECKING
    uint64 magicNum; /* DADA */
#endif
} AllocBlockData;

/*
 * AllocChunk
 *		The prefix of each piece of memory in an AllocBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 */
typedef struct AllocChunkData {
    /* aset is the owning aset if allocated, or the freelist link if free */
    void* aset;
    /* size is always the size of the usable space in the chunk */
    Size size;
#ifdef MEMORY_CONTEXT_CHECKING
    /* when debugging memory usage, also store actual requested size */
    /* this is zero in a free chunk */
    Size requested_size;
#endif
#ifdef MEMORY_CONTEXT_TRACK
    const char* file; /* __FILE__ of palloc/palloc0 call */
    int line;         /* __LINE__ of palloc/palloc0 call */
#endif
#ifdef MEMORY_CONTEXT_CHECKING
    uint32 prenum;    /* prefix magic number */
#endif
} AllocChunkData;

#define ALLOC_CHUNKHDRSZ MAXALIGN(sizeof(AllocChunkData))
#define ALLOC_BLOCKHDRSZ MAXALIGN(sizeof(AllocBlockData))
/* AsanSetContext is our asan implementation of MemoryContext.
 * Note:
 * AsanSetContext's structure must be consistent with AllocSetContext.
 * header.isReset means there is nothing for AsanSetContext to do.
 * This is different from the aset being physically empty (empty blocks list)
 * because we may still have a keeper block.  It's also different from the set
 * being logically empty, because we don't attempt to detect pfree'ing the * last active chunk. */
typedef struct AsanSetContext {
    MemoryContextData header; /* Standard memory-context fields */
    /* Info about storage allocated in this context: */
    AsanBlock blocks;                      /* head of list of blocks in this set */
    char* reserve[ALLOCSET_NUM_FREELISTS]; /* Allocation parameters for this context: */
    Size initBlockSize;                    /* initial block size */
    Size maxBlockSize;                     /* maximum block size */
    Size nextBlockSize;                    /* next block size to allocate */
    Size allocChunkLimit;                  /* effective chunk size limit */
    AsanBlock keeper;                      /* if not NULL, keep this block over resets */
    Size totalSpace;                       /* all bytes allocated by this context */
    Size freeSpace;                        /* all bytes freed by this context */
    /* maximum memory allocation of MemoryContext.For more information,we could see @StackSetContext too. */
    Size maxSpaceSize;
    MemoryTrack track; /* used to track the memory allocation information */
} AsanSetContext;

typedef AsanSetContext* AsanSet;

typedef struct AsanBlockData {
    AsanSet aset;       /* aset that owns this block */
    AsanBlock prev;     /* prev block in aset's blocks list, if any */
    AsanBlock next;     /* next block in aset's blocks list */
    uint32 requestSize; /* request size */
    int line;           /* __LINE__ of palloc/palloc0 call */
    const char* file;   /* __FILE__ of palloc/palloc0 call */
} AsanBlockData;

#define ASAN_BLOCKHDRSZ MAXALIGN(sizeof(AsanBlockData))
/* utils/palloc.h contains typedef struct MemoryContextData *MemoryContext */
typedef struct StackBlockData* StackBlock;

typedef struct StackSetContext {
    MemoryContextData header; /* Standard memory-context fields */
    StackBlock blocks;
    StackBlock freelist[ALLOCSET_NUM_FREELISTS]; /* free chunk lists */

    // Allocation parameters for this context:
    //
    Size initBlockSize;    // initial block size
    Size maxBlockSize;     // maximum block size
    Size nextBlockSize;    // next block size
    Size allocChunkLimit;  // limit chunk
    StackBlock keeper;
    Size totalSpace;
    Size freeSpace;

    /* The parameter named maxSpaceSize is an allocation parameter,but this parameter must be here.
     * Otherwise the pointer of MemoryContextMethods will be out-of-order.
     * So AllocSetContext is.You can see @AllocSetContext
     */
    Size maxSpaceSize;
    MemoryTrack track; /* used to track the memory allocation information */
} StackSetContext;

typedef struct MemoryProtectFuncDef {
    void* (*malloc)(Size sz, bool needProtect);
    void (*free)(void* ptr, Size sz);
    void* (*realloc)(void* ptr, Size oldsz, Size newsz, bool needProtect);
    int (*memalign)(void** memptr, Size alignment, Size sz, bool needProtect);
} MemoryProtectFuncDef;

extern MemoryProtectFuncDef GenericFunctions;
extern MemoryProtectFuncDef SessionFunctions;
extern MemoryProtectFuncDef SharedFunctions;

/*
 * MemoryContextIsValid
 *		True iff memory context is valid.
 *
 * Add new context types to the set accepted by this macro.
 */
#define MemoryContextIsValid(context)                                                                                 \
    ((context) != NULL &&                                                                                             \
        (IsA((context), AllocSetContext) || IsA((context), AsanSetContext) || IsA((context), StackAllocSetContext) || \
            IsA((context), SharedAllocSetContext) || IsA((context), MemalignAllocSetContext) ||                       \
            IsA((context), MemalignSharedAllocSetContext)))

#define AllocSetContextUsedSpace(aset) ((aset)->totalSpace - (aset)->freeSpace)
#endif /* MEMNODES_H */

