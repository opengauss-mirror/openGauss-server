/* -------------------------------------------------------------------------
 *
 * palloc.h
 *	  openGauss memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.	Keep it lean!
 *
 * Memory allocation occurs within "contexts".	Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/palloc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H
#ifndef FRONTEND_PARSER
#include "postgres.h"
#include "c.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

typedef struct MemoryContextData* MemoryContext;

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

/*
 * McxtAllocationMethods
 *      A virtual function table for memory allocation from memory context.
 */
typedef struct McxtAllocationMethods {
    void *(*alloc_from_context)(MemoryContext context, Size size, const char* file, int line);
    void *(*alloc_from_context_debug)(MemoryContext context, Size size, const char* file, int line);
    void *(*alloc_zero_from_context_debug)(MemoryContext context, Size size, const char* file, int line);
    void *(*alloc_zero_aligned_from_context_debug)(MemoryContext context, Size size, const char* file, int line);
    void *(*alloc_huge_from_context_debug)(MemoryContext context, Size size, const char* file, int line);
    void *(*alloc_huge_zero_from_context_debug)(MemoryContext context, Size size, const char* file, int line);
    void* (*alloc_extend_from_context_debug)(MemoryContext context, Size size, int flags, const char* file, int line);
    char *(*strdup_from_context_debug)(MemoryContext context, const char* string, const char* file, int line);
    void *(*alloc_extend)(Size size, int flags);
    void *(*alloc_noexcept)(Size size);
} McxtAllocationMethods;

/*
 * McxtOperationMethods
 *      A virtual function table for memory-context-type-independent functions.
 */
typedef struct McxtOperationMethods {
    void (*mcxt_reset)(MemoryContext context);
    void (*mcxt_delete)(MemoryContext context);
    void (*mcxt_delete_children)(MemoryContext context, List* context_list);
    void (*mcxt_destroy)(MemoryContext context);
    void (*mcxt_reset_and_delete_children)(MemoryContext context);
    void (*mcxt_set_parent)(MemoryContext context, MemoryContext new_parent);
#ifdef MEMORY_CONTEXT_CHECKING
    void (*mcxt_check)(MemoryContext context, bool own_by_session);
#endif
} McxtOperationMethods;

typedef struct MemoryContextData {
    NodeTag type;                  /* identifies exact kind of context */
    bool is_sealed;				   /* sealed context prevent any memory change */
    bool is_shared;                /* context is shared by threads */
    bool isReset;                  /* T = no space alloced since last reset */
    bool allowInCritSection;       /* allow palloc in critical section */
    MemoryContextMethods* methods; /* virtual function table */
    McxtAllocationMethods* alloc_methods;
    McxtOperationMethods* mcxt_methods;
    MemoryContext parent;          /* NULL if no parent (toplevel context) */
    MemoryContext firstchild;      /* head of linked list of children */
    MemoryContext prevchild;       /* previous child of same parent */
    MemoryContext nextchild;       /* next child of same parent */
    char* name;                    /* context name (just for debugging) */
    pthread_rwlock_t lock;         /* lock to protect members if the context is shared */
    int level;                     /* context level */
    uint64 session_id;             /* session id of context owner */
    ThreadId thread_id;            /* thread id of context owner */   
    ListCell cell;                 /* cell to pointer to this context*/
} MemoryContextData;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * We declare it here so that palloc() can be a macro.	Avoid accessing it
 * directly!  Instead, use MemoryContextSwitchTo() to change the setting.
 */
#ifdef WIN32
extern THR_LOCAL MemoryContext CurrentMemoryContext;
extern THR_LOCAL MemoryContext SelfMemoryContext;
extern THR_LOCAL MemoryContext TopMemoryContext;
#else
extern THR_LOCAL PGDLLIMPORT MemoryContext CurrentMemoryContext;
extern THR_LOCAL PGDLLIMPORT MemoryContext SelfMemoryContext;
extern THR_LOCAL PGDLLIMPORT MemoryContext TopMemoryContext;
#endif /* WIN32 */

#ifdef MEMORY_CONTEXT_CHECKING
const uint64 BlkMagicNum = 0xDADADADADADADADA;
const uint32 PremagicNum = 0xBABABABA;
#endif
/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE 0x01   /* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM 0x02 /* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO 0x04   /* zero allocated memory */

#define ENABLE_MEMORY_PROTECT() (t_thrd.utils_cxt.memNeedProtect = true)
#define DISABLE_MEMORY_PROTECT() (t_thrd.utils_cxt.memNeedProtect = false)

/* Definition for the unchanged interfaces */
#ifndef MEMORY_CONTEXT_CHECKING
#define MemoryContextAlloc(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_from_context(context, size, __FILE__, __LINE__))
#else
#define MemoryContextAlloc(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_from_context_debug(context, size, __FILE__, __LINE__))
#endif /* MEMORY_CONTEXT_CHECKING */

#define MemoryContextAllocZero(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_zero_from_context_debug(context, size, __FILE__, __LINE__))
#define MemoryContextAllocZeroAligned(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_zero_aligned_from_context_debug(context, size, __FILE__, __LINE__))
#define MemoryContextAllocExtended(context, size, flags) \
    (((MemoryContext)context)->alloc_methods->alloc_extend_from_context_debug(context, size, flags, __FILE__, __LINE__))
#define MemoryContextStrdup(context, str) \
    (((MemoryContext)context)->alloc_methods->strdup_from_context_debug(context, str, __FILE__, __LINE__))
#define palloc_huge(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_huge_from_context_debug(context, size, __FILE__, __LINE__))
#define palloc0_huge(context, size) \
    (((MemoryContext)context)->alloc_methods->alloc_huge_zero_from_context_debug(context, size, __FILE__, __LINE__))

#ifndef FRONTEND
#define palloc(sz) \
    (MemoryContextAlloc(CurrentMemoryContext, (sz)))
#define palloc_extended(size, flags) \
    (CurrentMemoryContext->alloc_methods->alloc_extend(size, flags))
#endif

#define palloc0_noexcept(size) \
    (CurrentMemoryContext->alloc_methods->alloc_noexcept(size))

#define palloc0(sz) MemoryContextAllocZero(CurrentMemoryContext, (sz))
#define selfpalloc(sz) MemoryContextAlloc(SelfMemoryContext, (sz))
#define selfpalloc0(sz) MemoryContextAllocZero(SelfMemoryContext, (sz))
#define selfpstrdup(str) MemoryContextStrdup(SelfMemoryContext, (str))
#define pstrdup(str) MemoryContextStrdup(CurrentMemoryContext, (str))
#define pstrdup_ext(str) \
    (((str) == NULL) ? NULL : (pstrdup((const char*)(str))))

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz)                                                          \
    (MemSetTest(0, sz) ? MemoryContextAllocZeroAligned(CurrentMemoryContext, sz) \
                       : MemoryContextAllocZero(CurrentMemoryContext, sz))

#define repalloc(pointer, size) \
    repallocDebug(pointer, size, __FILE__, __LINE__)
#define repalloc_noexcept(pointer, size) \
    repalloc_noexcept_Debug(pointer, size, __FILE__, __LINE__)
#define pnstrdup(in, len) \
    pnstrdupDebug(in, len, __FILE__, __LINE__)

#define INSTANCE_GET_MEM_CXT_GROUP  g_instance.mcxt_group->GetMemCxtGroup
#define THREAD_GET_MEM_CXT_GROUP    t_thrd.mcxt_group->GetMemCxtGroup
#define SESS_GET_MEM_CXT_GROUP      u_sess->mcxt_group->GetMemCxtGroup

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void* MemoryAllocFromContext(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocHugeDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocHugeZeroDebug(MemoryContext context, Size size, const char* file, int line);
extern void* repallocHugeDebug(void* pointer, Size size, const char* file, int line);
extern void* MemoryContextAllocZeroDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocZeroAlignedDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocExtendedDebug(MemoryContext context, Size size, int flags, const char* file, int line);
extern char* MemoryContextStrdupDebug(MemoryContext context, const char* string, const char* file, int line);
extern void* MemoryContextMemalignAllocDebug(MemoryContext context, Size align, Size size, const char* file, int line);
extern void MemoryContextMemalignFree(MemoryContext context, void* pointer);
extern void* std_palloc_extended(Size size, int flags);
extern void* std_palloc0_noexcept(Size size);

extern void* opt_MemoryAllocFromContext(MemoryContext context, Size size, const char* file, int line);
#define opt_MemoryContextAllocDebug(context, size, file, line) \
    opt_MemoryAllocFromContext(context, size, file, line)
#define opt_MemoryContextAllocHugeDebug(context, size, file, line) \
    opt_MemoryAllocFromContext(context, size, file, line)
extern void* opt_MemoryContextAllocHugeZeroDebug(MemoryContext context, Size size, const char* file, int line);
extern void* opt_MemoryContextAllocZeroDebug(MemoryContext context, Size size, const char* file, int line);
extern void* opt_MemoryContextAllocZeroAlignedDebug(MemoryContext context, Size size, const char* file, int line);
extern void* opt_MemoryContextAllocExtendedDebug(MemoryContext context, Size size, int flags, const char* file, int line);
extern char* opt_MemoryContextStrdupDebug(MemoryContext context, const char* string, const char* file, int line);
extern void* opt_palloc_extended(Size size, int flags);
extern void* opt_palloc0_noexcept(Size size);

#define repalloc_huge(pointer, size) repallocHugeDebug(pointer, size, __FILE__, __LINE__)

#define selfrepalloc(ptr, sz) repalloc((ptr), (sz))

extern THR_LOCAL MemoryContext AlignMemoryContext;
#define mem_align_alloc(align, size) \
    MemoryContextMemalignAllocDebug(AlignMemoryContext, (align), (size), __FILE__, __LINE__)
#define mem_align_free(ptr) MemoryContextMemalignFree(AlignMemoryContext, (ptr))

extern void pfree(void* pointer);
extern void opt_pfree(void* pointer);

#define selfpfree(ptr) pfree((ptr))

template <typename T>
bool isConst(T& x)
{
    return false;
}

template <typename T>
bool isConst(T const& x)
{
    return true;
}

#define FREE_POINTER(ptr)      \
    do {                       \
        if ((ptr) != NULL) {   \
            pfree((void*)ptr); \
            if (!isConst(ptr)) \
                (ptr) = NULL;  \
        }                      \
    } while (0)

// call pfree() and set pointer to be NULL.
#define pfree_ext(__p) FREE_POINTER(__p)

extern void* repallocDebug(void* pointer, Size size, const char* file, int line);
extern void* opt_repallocDebug(void* pointer, Size size, const char* file, int line);
extern void* repalloc_noexcept_Debug(void* pointer, Size size, const char* file, int line);
extern void* opt_repalloc_noexcept_Debug(void* pointer, Size size, const char* file, int line);

/*
 * MemoryContextSwitchTo can't be a macro in standard C compilers.
 * But we can make it an inline function if the compiler supports it.
 *
 * This file has to be includable by some non-backend code such as
 * pg_resetxlog, so don't expose the CurrentMemoryContext reference
 * if FRONTEND is defined.
 */
#if defined(USE_INLINE) && !defined(FRONTEND)

static inline MemoryContext MemoryContextSwitchTo(MemoryContext context)
{
    MemoryContext old = CurrentMemoryContext;

    CurrentMemoryContext = context;
    return old;
}
#else

extern MemoryContext MemoryContextSwitchTo(MemoryContext context);
#endif /* USE_INLINE && !FRONTEND */

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char* MemoryContextStrdupDebug(MemoryContext context, const char* string, const char* file, int line);

extern char* pnstrdupDebug(const char* in, Size len, const char* file, int line);

extern void AllocSetCheckPointer(void* pointer);

#if defined(WIN32) || defined(__CYGWIN__)
extern void* pgport_palloc(Size sz);
extern char* pgport_pstrdup(const char* str);
extern void pgport_pfree(void* pointer);
#endif

#define New(pmc) new (pmc, __FILE__, __LINE__)

// BaseObject is a basic class
// All other class should inherit from BaseObject class which
// override operator new/delete.
//
class BaseObject {
public:
    ~BaseObject()
    {}

    void* operator new(size_t size, MemoryContextData* pmc, const char* file, int line)
    {
        return MemoryContextAllocDebug(pmc, size, file, line);
    }

    void* operator new[](size_t size, MemoryContextData* pmc, const char* file, int line)
    {
        return MemoryContextAllocDebug(pmc, size, file, line);
    }

    void operator delete(void* p)
    {
        pfree(p);
    }

    void operator delete[](void* p)
    {
        pfree(p);
    }
};

/*
 *It is used for delete object whose destructor is null and free memory in Destroy()
 *_objptr can't include type change, for example (A*)b, that will lead to compile error in (_objptr) = NULL
 *If _objptr include type change, please use DELETE_EX_TYPE to complete it.
 *It is supplied easily for developers to refactor destructor in the future
 */
#define DELETE_EX(_objptr)    \
    do {                      \
        (_objptr)->Destroy(); \
        delete (_objptr);     \
        (_objptr) = NULL;     \
    } while (0)

// used for _objptr need to change to another type
#define DELETE_EX_TYPE(_objptr, _type)  \
    do {                                \
        ((_type*)(_objptr))->Destroy(); \
        delete (_type*)(_objptr);       \
        (_objptr) = NULL;               \
    } while (0)

#define DELETE_EX2(_objptr)   \
    do {                      \
        if ((_objptr) != nullptr) {    \
            delete (_objptr);          \
            (_objptr) = NULL;          \
        }                              \
    } while (0)

#endif /* !FRONTEND_PARSER */
#endif /* PALLOC_H */
