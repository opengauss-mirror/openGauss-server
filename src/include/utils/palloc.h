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

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.	Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData* MemoryContext;

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
#define MemoryContextAlloc(context, size) MemoryAllocFromContext(context, size, __FILE__, __LINE__)
#else
#define MemoryContextAlloc(context, size) MemoryContextAllocDebug(context, size, __FILE__, __LINE__)
#endif
#define MemoryContextAllocZero(context, size) MemoryContextAllocZeroDebug(context, size, __FILE__, __LINE__)
#define MemoryContextAllocZeroAligned(context, size) \
    MemoryContextAllocZeroAlignedDebug(context, size, __FILE__, __LINE__)
#define MemoryContextStrdup(context, size) MemoryContextStrdupDebug(context, size, __FILE__, __LINE__)
#define repalloc(pointer, size) repallocDebug(pointer, size, __FILE__, __LINE__)
#define repalloc_noexcept(pointer, size) repalloc_noexcept_Debug(pointer, size, __FILE__, __LINE__)
#define pnstrdup(in, len) pnstrdupDebug(in, len, __FILE__, __LINE__)

#define INSTANCE_GET_MEM_CXT_GROUP  g_instance.mcxt_group->GetMemCxtGroup
#define THREAD_GET_MEM_CXT_GROUP    t_thrd.mcxt_group->GetMemCxtGroup
#define SESS_GET_MEM_CXT_GROUP      u_sess->mcxt_group->GetMemCxtGroup

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void* MemoryAllocFromContext(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocHugeDebug(MemoryContext context, Size size, const char* file, int line);
extern void* repallocHugeDebug(void* pointer, Size size, const char* file, int line);
extern void* MemoryContextAllocZeroDebug(MemoryContext context, Size size, const char* file, int line);
extern void* MemoryContextAllocZeroAlignedDebug(MemoryContext context, Size size, const char* file, int line);
extern char* MemoryContextStrdupDebug(MemoryContext context, const char* string, const char* file, int line);
extern void* MemoryContextMemalignAllocDebug(MemoryContext context, Size align, Size size, const char* file, int line);
extern void MemoryContextMemalignFree(MemoryContext context, void* pointer);
#ifndef FRONTEND
extern void* palloc_extended(Size size, int flags);
extern void* palloc0_noexcept(Size size);
#define palloc(sz) MemoryContextAlloc(CurrentMemoryContext, (sz))
#endif
#define palloc0(sz) MemoryContextAllocZero(CurrentMemoryContext, (sz))

#define palloc_huge(context, size) MemoryContextAllocHugeDebug(context, size, __FILE__, __LINE__)

#define repalloc_huge(pointer, size) repallocHugeDebug(pointer, size, __FILE__, __LINE__)

#define selfpalloc(sz) MemoryContextAlloc(SelfMemoryContext, (sz))

#define selfpalloc0(sz) MemoryContextAllocZero(SelfMemoryContext, (sz))

#define selfpfree(ptr) pfree((ptr))

#define selfrepalloc(ptr, sz) repalloc((ptr), (sz))

#define selfpstrdup(str) MemoryContextStrdup(SelfMemoryContext, (str))

extern THR_LOCAL MemoryContext AlignMemoryContext;
#define mem_align_alloc(align, size) \
    MemoryContextMemalignAllocDebug(AlignMemoryContext, (align), (size), __FILE__, __LINE__)
#define mem_align_free(ptr) MemoryContextMemalignFree(AlignMemoryContext, (ptr))

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

extern void pfree(void* pointer);

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
extern void* repalloc_noexcept_Debug(void* pointer, Size size, const char* file, int line);

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

#define pstrdup(str) MemoryContextStrdup(CurrentMemoryContext, (str))

#define pstrdup_ext(str) \
    (((str) == NULL) ? NULL : (pstrdup((const char*)(str))))

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
