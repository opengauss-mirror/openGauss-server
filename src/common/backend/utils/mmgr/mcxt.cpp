/* -------------------------------------------------------------------------
 *
 * mcxt.c
 *	  openGauss memory context management code.
 *
 * This module handles context management operations that are independent
 * of the particular kind of context being operated on.  It calls
 * context-type-specific operations via the function pointers in a
 * context's MemoryContextMethods struct.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/mcxt.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/memtrace.h"
#include "pgxc/pgxc.h"

#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/cstore/cstore_mem_alloc.h"
#include "threadpool/threadpool.h"
#include "tcop/tcopprot.h"
#include "workload/workload.h"
#include "pgstat.h"

/*****************************************************************************
 *	  GLOBAL MEMORY															 *
 *****************************************************************************/
/*
 * CurrentMemoryContext
 *		Default memory context for allocations.
 */
THR_LOCAL MemoryContext CurrentMemoryContext = NULL;

/* This memory context is at process level. So any allocation without free
 * against this memory context will stay as far as the process lives. So be
 * careful to use this memory context.
 */
MemoryContext AdioSharedContext = NULL;
MemoryContext ProcSubXidCacheContext = NULL;
MemoryContext PmTopMemoryContext = NULL;  // save the topm of postmaster thread
/* Shared memory context for stream thread connection info. */
MemoryContext StreamInfoContext = NULL;

/*
 * Standard top-level contexts. For a description of the purpose of each
 * of these contexts, refer to src/backend/utils/mmgr/README
 */
THR_LOCAL MemoryContext ErrorContext = NULL;
THR_LOCAL MemoryContext SelfMemoryContext = NULL;
THR_LOCAL MemoryContext TopMemoryContext = NULL;
THR_LOCAL MemoryContext AlignMemoryContext = NULL;

static void MemoryContextStatsInternal(MemoryContext context, int level);
static void FreeMemoryContextList(List* context_list);

#ifdef PGXC
void* allocTopCxt(size_t s);
#endif

static McxtAllocationMethods StdMcxtAllocMtd = {
    MemoryAllocFromContext,
    MemoryContextAllocDebug,
    MemoryContextAllocZeroDebug,
    MemoryContextAllocZeroAlignedDebug,
    MemoryContextAllocHugeDebug,
    MemoryContextAllocHugeZeroDebug,
    MemoryContextAllocExtendedDebug,
    MemoryContextStrdupDebug,
    std_palloc_extended,
    std_palloc0_noexcept
};

static McxtOperationMethods StdMcxtOpMtd = {
    std_MemoryContextReset,
    std_MemoryContextDelete,
    std_MemoryContextDeleteChildren,
    std_MemoryContextDestroyAtThreadExit,
    std_MemoryContextResetAndDeleteChildren,
    std_MemoryContextSetParent
#ifdef MEMORY_CONTEXT_CHECKING
    ,
    std_MemoryContextCheck
#endif
};

/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/
static inline void InsertMemoryAllocInfo(const void* pointer, MemoryContext context,
                                         const char* file, int line, Size size)
{
    if (unlikely(g_instance.stat_cxt.track_memory_inited)) {
        if ((unlikely(strncmp(context->name, "Track MemoryInfo hash", NAMEDATALEN) != 0)) &&
            unlikely(MemoryContextShouldTrack(context->name))) {
            /*
             * Get lock failed means there is some thread get write lock,
             * we could not insert track memory info now, so pass it
             */
            if (likely(pthread_rwlock_tryrdlock(&g_instance.stat_cxt.track_memory_lock) == 0)) {
                InsertTrackMemoryInfo(pointer, context, file, line, size);
                (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
            }
        }
    }
}

static inline void RemoveMemoryAllocInfo(const void* pointer, MemoryContext context)
{
    if (unlikely(g_instance.stat_cxt.track_memory_inited) &&
        unlikely(MemoryContextShouldTrack(context->name))) {
        /* Do not use tryrdlock because it may cause use-after-free */
        if (likely(pthread_rwlock_rdlock(&g_instance.stat_cxt.track_memory_lock) == 0)) {
            RemoveTrackMemoryInfo(pointer);
            (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
        }
    }
}

static inline void RemoveMemoryContextInfo(MemoryContext context)
{
    if (unlikely(g_instance.stat_cxt.track_memory_inited) &&
        unlikely(MemoryContextShouldTrack(context->name))) {
        /* Do not use tryrdlock because it may cause use-after-free */
        if (likely(pthread_rwlock_rdlock(&g_instance.stat_cxt.track_memory_lock) == 0)) {
            RemoveTrackMemoryContext(context);
            (void)pthread_rwlock_unlock(&g_instance.stat_cxt.track_memory_lock);
        }
    }
}

/*
 * MemoryContextInit
 *		Start up the memory-context subsystem.
 *
 * This must be called before creating contexts or allocating memory in
 * contexts.  t_thrd.top_mem_cxt and ErrorContext are initialized here;
 * other contexts must be created afterwards.
 *
 * In normal multi-backend operation, this is called once during
 * postmaster startup, and not at all by individual backend startup
 * (since the backends inherit an already-initialized context subsystem
 * by virtue of being forked off the postmaster).  But in an EXEC_BACKEND
 * build, each process must do this for itself.
 *
 * In a standalone backend this must be called during backend startup.
 */
void MemoryContextInit(void)
{
    CStoreMemAlloc::Init();
    if (TopMemoryContext != NULL) {
        return;
    }

    /* init the thread memory track object */
    t_thrd.utils_cxt.trackedMemChunks = 0;
    t_thrd.utils_cxt.trackedBytes = 0;
    t_thrd.utils_cxt.peakedBytesInQueryLifeCycle = 0;
    t_thrd.utils_cxt.basedBytesInQueryLifeCycle = 0;


    /*
     * Initialize t_thrd.top_mem_cxt as an AllocSetContext with slow growth rate
     * --- we don't really expect much to be allocated in it.
     *
     * (There is special-case code in MemoryContextCreate() for this call.)
     */
    t_thrd.top_mem_cxt = AllocSetContextCreate((MemoryContext)NULL, "ThreadTopMemoryContext", 0, 8 * 1024, 8 * 1024);
    TopMemoryContext = t_thrd.top_mem_cxt;

    /*
     * Not having any other place to point CurrentMemoryContext, make it point
     * to t_thrd.top_mem_cxt.  Caller should change this soon!
     */
    CurrentMemoryContext = t_thrd.top_mem_cxt;

    /*
     * Initialize ErrorContext as an AllocSetContext with slow growth rate ---
     * we don't really expect much to be allocated in it. More to the point,
     * require it to contain at least 8K at all times. This is the only case
     * where retained memory in a context is *essential* --- we want to be
     * sure ErrorContext still has some memory even if we've run out
     * elsewhere!
     *
     * This should be the last step in this function, as elog.c assumes memory
     * management works once ErrorContext is non-null.
     */
    ErrorContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "ErrorContext", 8 * 1024, 8 * 1024, 8 * 1024);

    AlignMemoryContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "AlignContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        MEMALIGN_CONTEXT);

    t_thrd.mem_cxt.profile_log_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Profile Logging",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
}

static inline void PreventActionOnSealedContext(MemoryContext context)
{
#ifdef MEMORY_CONTEXT_CHECKING
	if(unlikely(context->is_sealed))
		ereport(PANIC,
		        (errcode(ERRCODE_INVALID_OPERATION),
		         errmsg("invalid operation on memory context"),
		         errdetail("Failed on operation on sealed context %s",  context->name)));
#else
	if(unlikely(context->is_sealed))
	  ereport(ERROR,
				(errcode(ERRCODE_INVALID_OPERATION),
				 errmsg("invalid operation on memory context"),
				 errdetail("Failed on operation on sealed context %s",  context->name)));
#endif

}

/*
 * std_MemoryContextReset
 *		Release all space allocated within a context and its descendants,
 *		but don't delete the contexts themselves.
 *
 * The type-specific reset routine handles the context itself, but we
 * have to do the recursion for the children.
 */
void std_MemoryContextReset(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

#ifdef MEMORY_CONTEXT_CHECKING
    PreventActionOnSealedContext(context);
#endif

    if (MemoryContextIsShared(context))
        MemoryContextLock(context);

    /* save a function call in common case where there are no children */
    if (context->firstchild != NULL)
        MemoryContextResetChildren(context);

    if (MemoryContextIsShared(context))
        MemoryContextUnlock(context);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Memory Context Checking */
    MemoryContextCheck(context, context->session_id > 0);
#endif

    /* Nothing to do if no pallocs since startup or last reset */
    if (!context->isReset) {
        RemoveMemoryContextInfo(context);
        (*context->methods->reset)(context);
        context->isReset = true;
    }
}

/*
 * MemoryContextResetChildren
 *		Release all space allocated within a context's descendants,
 *		but don't delete the contexts themselves.  The named context
 *		itself is not touched.
 */
void MemoryContextResetChildren(MemoryContext context)
{
    MemoryContext child;

    AssertArg(MemoryContextIsValid(context));

    for (child = context->firstchild; child != NULL; child = child->nextchild)
        MemoryContextReset(child);
}

static inline void TopMemCxtUnSeal()
{
    /* Enable memory opeation on top memory context. */
    MemoryContextUnSeal(t_thrd.top_mem_cxt);
    if (u_sess != NULL && u_sess->top_mem_cxt != NULL) {
        MemoryContextUnSeal(u_sess->top_mem_cxt);
    }
}

static inline void TopMemCxtSeal()
{
    /* Prevent memory opeation on top memory context. */
    MemoryContextSeal(t_thrd.top_mem_cxt);
    if (u_sess != NULL && u_sess->top_mem_cxt != NULL) {
        MemoryContextSeal(u_sess->top_mem_cxt);
    }
}

static inline bool IsTopMemCxt(const MemoryContext mcxt)
{
    bool is_sess_top = (u_sess == NULL) ? false : (mcxt == u_sess->top_mem_cxt);
    return (mcxt == g_instance.instance_context) || (mcxt == t_thrd.top_mem_cxt) || is_sess_top;
}

/*
 * MemoryContextDeleteInternal
 *		Delete a context and its descendants, and release all space
 *		allocated therein.
 *
 * The type-specific delete routine removes all subsidiary storage
 * for the context, but we have to delete the context node itself,
 * as well as recurse to get the children.	We must also delink the
 * node from its parent, if it has one.
 */
static void MemoryContextDeleteInternal(MemoryContext context, bool parent_locked,
    List* context_list)
{
    AssertArg(MemoryContextIsValid(context));
    /* We had better not be deleting t_thrd.top_mem_cxt ... */
    Assert(context != t_thrd.top_mem_cxt);
    /* And not CurrentMemoryContext, either */
    Assert(context != CurrentMemoryContext);

    MemoryContextDeleteChildren(context, context_list);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Memory Context Checking */
    MemoryContextCheck(context, context->session_id > 0);
#endif

    MemoryContext parent = context->parent;


    PG_TRY();
    {
        HOLD_INTERRUPTS();
        if (context->session_id > 0) {
            (void)syscalllockAcquire(&u_sess->utils_cxt.deleMemContextMutex);
        } else if (t_thrd.proc != NULL && t_thrd.proc->topmcxt != NULL) {
            (void)syscalllockAcquire(&t_thrd.proc->deleMemContextMutex);
        }

        /*
         * If the parent context is shared and is already locked by the caller,
         * no need to relock again. In fact, that's not the right thing to do
         * since it will lead to a self-deadlock
         */
        if (parent && MemoryContextIsShared(parent) && (!parent_locked))
            MemoryContextLock(parent);
        /*
         * We delink the context from its parent before deleting it, so that if
         * there's an error we won't have deleted/busted contexts still attached
         * to the context tree.  Better a leak than a crash.
         */
        TopMemCxtUnSeal();
        MemoryContextSetParent(context, NULL);
        if (parent != NULL && MemoryContextIsShared(parent) && (parent_locked == false))
            MemoryContextUnlock(parent);

        RemoveMemoryContextInfo(context);
        (*context->methods->delete_context)(context);
        (void)lappend2(context_list, &context->cell);

        if (context->session_id > 0) {
            (void)syscalllockRelease(&u_sess->utils_cxt.deleMemContextMutex);
        } else if (t_thrd.proc != NULL && t_thrd.proc->topmcxt != NULL) {
            (void)syscalllockRelease(&t_thrd.proc->deleMemContextMutex);
        }
        TopMemCxtSeal();
        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (context->session_id > 0) {
            (void)syscalllockRelease(&u_sess->utils_cxt.deleMemContextMutex);
        } else if (t_thrd.proc != NULL && t_thrd.proc->topmcxt != NULL) {
            (void)syscalllockRelease(&t_thrd.proc->deleMemContextMutex);
        }
        TopMemCxtSeal();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void std_MemoryContextDelete(MemoryContext context)
{
    List context_list = {T_List, 0, NULL, NULL};;

    if (!IsTopMemCxt(context)) {
        PreventActionOnSealedContext(context);
    } else {
#ifdef MEMORY_CONTEXT_CHECKING
        /* before delete top memcxt, you should close lsc */
        if (EnableGlobalSysCache() && context == t_thrd.top_mem_cxt && t_thrd.lsc_cxt.lsc != NULL) {
            Assert(t_thrd.lsc_cxt.lsc->is_closed);
        }
#endif
    }

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.top_mem_cxt);

    MemoryContextDeleteInternal(context, false, &context_list);

    (void)MemoryContextSwitchTo(old_context);

    /* u_sess->top_mem_cxt may be reused by other threads, set it null before the memory it points to be freed. */
    if (u_sess != NULL && context == u_sess->top_mem_cxt) {
        u_sess->top_mem_cxt = NULL;
    }

    FreeMemoryContextList(&context_list);
}

/*
 * std_MemoryContextDeleteChildren
 *		Delete all the descendants of the named context and release all
 *		space allocated therein.  The named context itself is not touched.
 */
void std_MemoryContextDeleteChildren(MemoryContext context, List* context_list)
{
    AssertArg(MemoryContextIsValid(context));
    List res_list = {T_List, 0, NULL, NULL};
    if (context_list == NULL) {
        context_list = &res_list;
    }

    if (MemoryContextIsShared(context))
        MemoryContextLock(context);

    /*
     * MemoryContextDelete will delink the child from me, so just iterate as
     * long as there is a child.
     */
    while (context->firstchild != NULL)
        MemoryContextDeleteInternal(context->firstchild, true, context_list);

    if (MemoryContextIsShared(context))
        MemoryContextUnlock(context);

    if (context_list == &res_list) {
        FreeMemoryContextList(&res_list);
    }
}

/*
 * std_MemoryContextResetAndDeleteChildren
 *		Release all space allocated within a context and delete all
 *		its descendants.
 *
 * This is a common combination case where we want to preserve the
 * specific context but get rid of absolutely everything under it.
 */
void std_MemoryContextResetAndDeleteChildren(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));
    if (!IsTopMemCxt(context)) {
        PreventActionOnSealedContext(context);
    } else {
#ifdef MEMORY_CONTEXT_CHECKING
        /* before delete top memcxt,  you should close lsc */
        if (EnableGlobalSysCache() && context == t_thrd.top_mem_cxt && t_thrd.lsc_cxt.lsc != NULL) {
            Assert(t_thrd.lsc_cxt.lsc->is_closed);
        }
#endif
    }

    List context_list = {T_List, 0, NULL, NULL};
    
    MemoryContextDeleteChildren(context, &context_list);

    FreeMemoryContextList(&context_list);
    MemoryContextReset(context);
}

/*
 * std_MemoryContextSetParent
 *		Change a context to belong to a new parent (or no parent).
 *
 * We provide this as an API function because it is sometimes useful to
 * change a context's lifespan after creation.  For example, a context
 * might be created underneath a transient context, filled with data,
 * and then reparented underneath u_sess->cache_mem_cxt to make it long-lived.
 * In this way no special effort is needed to get rid of the context in case
 * a failure occurs before its contents are completely set up.
 *
 * Callers often assume that this function cannot fail, so don't put any
 * elog(ERROR) calls in it.
 *
 * A possible caller error is to reparent a context under itself, creating
 * a loop in the context graph.  We assert here that context != new_parent,
 * but checking for multi-level loops seems more trouble than it's worth.
 */
void std_MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
{
    AssertArg(MemoryContextIsValid(context));
    AssertArg(context != new_parent);
    PreventActionOnSealedContext(context);

    if (new_parent != NULL) {
        if (context->session_id != new_parent->session_id)
            ereport(PANIC,
                (errmsg("We can not set memory context parent with different session number")));
    }

    /* Delink from existing parent, if any */
    if (context->parent) {
        MemoryContext parent = context->parent;

        if (context->prevchild != NULL)
            context->prevchild->nextchild = context->nextchild;
        else {
            Assert(parent->firstchild == context);
            parent->firstchild = context->nextchild;
        }

        if (context->nextchild != NULL)
            context->nextchild->prevchild = context->prevchild;
    }

    /* And relink */
    if (new_parent) {
        AssertArg(MemoryContextIsValid(new_parent));
        context->parent = new_parent;
        context->prevchild = NULL;
        context->nextchild = new_parent->firstchild;
        if (new_parent->firstchild != NULL)
            new_parent->firstchild->prevchild = context;
        new_parent->firstchild = context;
    } else {
        context->parent = NULL;
        context->prevchild = NULL;
        context->nextchild = NULL;
    }
}

/*
 * MemoryContextAllowInCriticalSection
 *      Allow/disallow allocations in this memory context within a critical
 *      section.
 *
 * Normally, memory allocations are not allowed within a critical section,
 * because a failure would lead to PANIC.  There are a few exceptions to
 * that, like allocations related to debugging code that is not supposed to
 * be enabled in production.  This function can be used to exempt specific
 * memory contexts from the assertion in palloc().
 */
void MemoryContextAllowInCriticalSection(MemoryContext context, bool allow)
{
    AssertArg(MemoryContextIsValid(context));
    context->allowInCritSection = allow;
}

/*
 * GetMemoryChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 *
 * This is useful for measuring the total space occupied by a set of
 * allocated chunks.
 */
Size GetMemoryChunkSpace(void* pointer)
{
#ifndef ENABLE_MEMORY_CHECK
    StandardChunkHeader* header = NULL;

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    /*
     * OK, it's probably safe to look at the chunk header.
     */
    header = (StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE);

    AssertArg(MemoryContextIsValid(header->context));

    return (*header->context->methods->get_chunk_space)(header->context, pointer);
#else
    return AsanMemoryAllocator::AllocSetGetChunkSpace(NULL, pointer);
#endif
}

/*
 * GetMemoryChunkContext
 *		Given a currently-allocated chunk, determine the context
 *		it belongs to.
 */
MemoryContext GetMemoryChunkContext(void* pointer)
{
#ifndef ENABLE_MEMORY_CHECK
    StandardChunkHeader* header = NULL;

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    /*
     * OK, it's probably safe to look at the chunk header.
     */
    header = (StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE);

    AssertArg(MemoryContextIsValid(header->context));

    return header->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    MemoryContext context = &(block->aset->header);
    return context;
#endif
}

/*
 * MemoryContextGetParent
 *		Get the parent context (if any) of the specified context
 */
MemoryContext MemoryContextGetParent(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    return context->parent;
}

/*
 * MemoryContextIsEmpty
 *		Is a memory context empty of any allocated space?
 */
bool MemoryContextIsEmpty(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /*
     * For now, we consider a memory context nonempty if it has any children;
     * perhaps this should be changed later.
     */
    if (context->firstchild != NULL)
        return false;
    /* Otherwise use the type-specific inquiry */
    return (*context->methods->is_empty)(context);
}

/*
 * MemoryContextStats
 *		Print statistics about the named context and all its descendants.
 *
 * This is just a debugging utility, so it's not fancy.  The statistics
 * are merely sent to stderr.
 */
void MemoryContextStats(MemoryContext context)
{
    MemoryContextStatsInternal(context, 0);
}

void MemoryContextSeal(MemoryContext context)
{
	context->is_sealed = true;
}

void MemoryContextUnSeal(MemoryContext context)
{
	context->is_sealed = false;
}

void MemoryContextUnSealChildren(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));
	context->is_sealed = false;
    for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild)
        MemoryContextUnSealChildren(child);
}

static void MemoryContextStatsInternal(MemoryContext context, int level)
{
    MemoryContext child;

    AssertArg(MemoryContextIsValid(context));

    (*context->methods->stats)(context, level);
    for (child = context->firstchild; child != NULL; child = child->nextchild)
        MemoryContextStatsInternal(child, level + 1);
}

/*
 * std_MemoryContextCheck
 *		Check all chunks in the named context.
 *
 * This is just a debugging utility, so it's not fancy.
 */
#ifdef MEMORY_CONTEXT_CHECKING
void std_MemoryContextCheck(MemoryContext context, bool own_by_session)
{
    MemoryContext child;

    if (!g_instance.attr.attr_memory.enable_memory_context_check_debug) {
        return;
    }
    if (unlikely(context == NULL)) {
        elog(PANIC, "Switch to Invalid memory context");
    }
    AssertArg(MemoryContextIsValid(context));

    uint64 id = 0;
    uint64 childId = 0;

    if (!context->is_shared) {
        if (own_by_session)
            id = context->session_id;
        else {
            id = context->thread_id;
            Assert(context->session_id == 0);
        }
    }

    (*context->methods->check)(context);
    for (child = context->firstchild; child != NULL; child = child->nextchild) {
        if (!child->is_shared) {
            if (own_by_session)
                childId = child->session_id;
            else {
                childId = child->thread_id;
                Assert(child->session_id == 0);
            }
            if (id == 0)
                id = childId;

            Assert(id == childId);
        }

        MemoryContextCheck(child, own_by_session);
    }
}
#endif

/*
 * MemoryContextContains
 *		Detect whether an allocated chunk of memory belongs to a given
 *		context or not.
 *
 * Caution: this test is reliable as long as 'pointer' does point to
 * a chunk of memory allocated from *some* context.  If 'pointer' points
 * at memory obtained in some other way, there is a small chance of a
 * false-positive result, since the bits right before it might look like
 * a valid chunk header by chance.
 */
bool MemoryContextContains(MemoryContext context, void* pointer)
{
#ifndef ENABLE_MEMORY_CHECK
    StandardChunkHeader* header = NULL;

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    if (pointer == NULL || pointer != (void*)MAXALIGN(pointer))
        return false;

    /*
     * OK, it's probably safe to look at the chunk header.
     */
    header = (StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE);

    /*
     * If the context link doesn't match then we certainly have a non-member
     * chunk.  Also check for a reasonable-looking size as extra guard against
     * being fooled by bogus pointers.
     */
    if (header->context == context && AllocSizeIsValid(header->size))
        return true;
    return false;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    MemoryContext pointerContext = &(block->aset->header);
    if (pointerContext == context && AllocSizeIsValid(MAXALIGN(sizeof(AsanBlockData)) + MAXALIGN(block->requestSize)))
        return true;
    return false;
#endif
}

static MemoryContext ChooseRootContext(NodeTag tag, MemoryContext parent)
{
    MemoryContext root = NULL;

    if (tag == T_SharedAllocSetContext || tag == T_MemalignSharedAllocSetContext) {
        root = g_instance.instance_context;
    } else if (parent) {
        if (parent->type == T_SharedAllocSetContext || parent->type == T_MemalignSharedAllocSetContext) {
            root = g_instance.instance_context;
        } else if (parent->session_id > 0) {
            Assert(u_sess->session_id == parent->session_id);
            root = u_sess->top_mem_cxt;
        } else {
            root = t_thrd.top_mem_cxt;
        }
    } else {
        root = t_thrd.top_mem_cxt;
    }

    return root;
}

static void TagMemoryContextSessionId(MemoryContext node, MemoryContext parent)
{
    uint64 sessId = 0;
    if (parent) {
        if (parent->session_id > 0) {
            if (u_sess && u_sess->session_id > 0) {
                Assert(parent->is_shared || parent->session_id == u_sess->session_id);
            }
            sessId = parent->session_id;
        } else {
            Assert(parent->is_shared || parent->thread_id == gs_thread_self());
        }
    } else {
        if (u_sess && u_sess->session_id)
            sessId = u_sess->session_id;
    }

    node->session_id = sessId;
}

/* --------------------
 * MemoryContextCreate
 *		Context-type-independent part of context creation.
 *
 * This is only intended to be called by context-type-specific
 * context creation routines, not by the unwashed masses.
 *
 * The context creation procedure is a little bit tricky because
 * we want to be sure that we don't leave the context tree invalid
 * in case of failure (such as insufficient memory to allocate the
 * context node itself).  The procedure goes like this:
 *	1.	Context-type-specific routine first calls MemoryContextCreate(),
 *		passing the appropriate tag/size/methods values (the methods
 *		pointer will ordinarily point to statically allocated data).
 *		The parent and name parameters usually come from the caller.
 *	2.	MemoryContextCreate() attempts to allocate the context node,
 *		plus space for the name.  If this fails we can ereport() with no
 *		damage done.
 *	3.	We fill in all of the type-independent MemoryContext fields.
 *	4.	We call the type-specific init routine (using the methods pointer).
 *		The init routine is required to make the node minimally valid
 *		with zero chance of failure --- it can't allocate more memory,
 *		for example.
 *	5.	Now we have a minimally valid node that can behave correctly
 *		when told to reset or delete itself.  We link the node to its
 *		parent (if any), making the node part of the context tree.
 *	6.	We return to the context-type-specific routine, which finishes
 *		up type-specific initialization.  This routine can now do things
 *		that might fail (like allocate more memory), so long as it's
 *		sure the node is left in a state that delete will handle.
 *
 * This protocol doesn't prevent us from leaking memory if step 6 fails
 * during creation of a top-level context, since there's no parent link
 * in that case.  However, if you run out of memory while you're building
 * a top-level context, you might as well go home anyway...
 *
 * Normally, the context node and the name are allocated from
 * t_thrd.top_mem_cxt (NOT from the parent context, since the node must
 * survive resets of its parent context!).	However, this routine is itself
 * used to create t_thrd.top_mem_cxt!  If we see that t_thrd.top_mem_cxt is NULL,
 * we assume we are creating t_thrd.top_mem_cxt and use malloc() to allocate
 * the node.
 *
 * Note that the name field of a MemoryContext does not point to
 * separately-allocated storage, so it should not be freed at context
 * deletion.
 * --------------------
 */
MemoryContext MemoryContextCreate(
    NodeTag tag, Size size, MemoryContext parent, const char* name, const char* file, int line)
{
    MemoryContext node;
    Size needed = size + sizeof(MemoryContextMethods) + strlen(name) + 1;
    MemoryContext root = ChooseRootContext(tag, parent);
    errno_t rc = EOK;

    /* Get space for node and name */
    if (root != NULL) {
        /* Normal case: allocate the node in Root MemoryContext */
        node = (MemoryContext)MemoryAllocFromContext(root, needed, file, line);
    } else {
        /* Special case for startup: use good ol' malloc */
        node = (MemoryContext)malloc(needed);
        if (node == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("out of memory"),
                    errdetail("Failed on request of size %lu in %s:%d.", (unsigned long)needed, file, line)));
    }

    /* Initialize the node as best we can */
    rc = memset_s(node, size, 0, size);
    securec_check(rc, "\0", "\0");
    node->type = tag;
    node->parent = NULL; /* for the moment */
    node->firstchild = NULL;
    node->prevchild = NULL;
    node->nextchild = NULL;
    node->isReset = true;
    node->is_sealed = false;
    node->methods = (MemoryContextMethods*)(((char*)node) + size);
    node->alloc_methods = &StdMcxtAllocMtd;
    node->mcxt_methods = &StdMcxtOpMtd;
    node->name = ((char*)node) + size + sizeof(MemoryContextMethods);
    node->cell.data.ptr_value = (void*)node;
    node->cell.next = NULL;
    rc = strcpy_s(node->name, strlen(name) + 1, name);
    securec_check_c(rc, "\0", "\0");
    node->thread_id = gs_thread_self();
    TagMemoryContextSessionId(node, parent);

    /*
     * Lock the parent context if the it is shared and must be made thread-safe
     */
    if ((parent != NULL) && (MemoryContextIsShared(parent)))
        MemoryContextLock(parent);

    /* OK to link node to parent (if any) */
    /* Could use MemoryContextSetParent here, but doesn't seem worthwhile */
    if (parent) {
        node->parent = parent;
        node->nextchild = parent->firstchild;
        if (parent->firstchild != NULL)
            parent->firstchild->prevchild = node;
        parent->firstchild = node;

        node->level = parent->level + 1;
    } else
        node->level = 0;

    if ((parent != NULL) && (MemoryContextIsShared(parent)))
        MemoryContextUnlock(parent);

    /* Return to type-specific creation routine to finish up */
    return node;
}

/* --------------------
 * MemoryContextCreate
 *		Context-type-independent part of context creation.
 * This funciton is an interface for PosgGIS and has the same
 * structure with PG's MemoryContextCreate.
 */
MemoryContext MemoryContextCreate(
    NodeTag tag, Size size, MemoryContextMethods* methods, MemoryContext parent, const char* name)
{
    MemoryContext context;
    context = MemoryContextCreate(tag, size, parent, name, __FILE__, __LINE__);

    context->methods = methods;
    return context;
}

/* check if the memory context is out of control */
#define MEMORY_CONTEXT_CONTROL_LEVEL (ENABLE_THREAD_POOL ? 5: 3)  // ExecutorState
void MemoryContextCheckMaxSize(MemoryContext context, Size size, const char* file, int line)
{
#ifndef ENABLE_MEMORY_CHECK
    AllocSet set = (AllocSet)context;
#else
    AsanSet set = (AsanSet)context;
#endif
    /* check if it is beyond the limitation */
    if (ENABLE_MEMORY_CONTEXT_CONTROL && IS_PGXC_DATANODE && set->totalSpace > set->maxSpaceSize) {
        if (context->level >= MEMORY_CONTEXT_CONTROL_LEVEL && !t_thrd.int_cxt.CritSectionCount &&
            !(AmPostmasterProcess()) && IsNormalProcessingMode()) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                    errmsg("memory is temporarily unavailable"),
                    errdetail("Allocated size %ld is beyond to the memory context '%s' limitation."
                              "The maxSize of MemoryContext is %ld.[file:%s,line:%d]",
                        (unsigned long)size,
                        context->name,
                        set->maxSpaceSize,
                        file,
                        line)));
        }
    }
}

/* check if the session memory is beyond the limitation */
void MemoryContextCheckSessionMemory(MemoryContext context, Size size, const char* file, int line)
{
    /* libcomm permanent thread don't need to check session memory */
    if ((t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL) &&
        (t_thrd.comm_cxt.LibcommThreadType == LIBCOMM_NONE) && (context->level >= MEMORY_CONTEXT_CONTROL_LEVEL) &&
        !t_thrd.int_cxt.CritSectionCount && !(AmPostmasterProcess()) && IsNormalProcessingMode()) {
        int used = (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks << (chunkSizeInBits - BITS_IN_MB)) 
            << BITS_IN_KB;
        if (u_sess->attr.attr_sql.statement_max_mem < used)
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                    errmsg(
                        "Session used memory %d Kbytes is beyond the limitation %d Kbytes.", used,
                        u_sess->attr.attr_sql.statement_max_mem),
                    errdetail("Session estimated memory is %d Mbytes and MemoryContext %s request of size %lu "
                              "bytes.[file:%s,line:%d]",
                        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory,
                        context->name,
                        size,
                        file,
                        line)));
    }
}

void* MemoryAllocFromContext(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d.", (unsigned long)size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }

    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}
/*
 * MemoryContextAlloc
 *		Allocate space within the specified context.
 *
 * This could be turned into a macro, but we'd have to import
 * nodes/memnodes.h into postgres.h which seems a bad idea.
 */
void* MemoryContextAllocDebug(MemoryContext context, Size size, const char* file, int line)
{
    AssertArg(MemoryContextIsValid(context));
    PreventActionOnSealedContext(context);

    return MemoryAllocFromContext(context, size, file, line);
}
/*
 * MemoryContextAllocZero
 *		Like MemoryContextAlloc, but clears allocated memory
 *
 *	We could just call MemoryContextAlloc then clear the memory, but this
 *	is a very common combination, so we provide the combined operation.
 */
void* MemoryContextAllocZeroDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(context));
#ifdef MEMORY_CONTEXT_CHECKING
    PreventActionOnSealedContext(context);
#endif
    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    MemSetAligned(ret, 0, size);

    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * MemoryContextAllocZeroAligned
 *		MemoryContextAllocZero where length is suitable for MemSetLoop
 *
 *	This might seem overly specialized, but it's not because newNode()
 *	is so often called with compile-time-constant sizes.
 */
void* MemoryContextAllocZeroAlignedDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(context));

    PreventActionOnSealedContext(context);

    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    MemSetLoop(ret, 0, size);

    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * MemoryContextAllocExtended
 *	  Allocate space within the specified context using the given flags.
 *   
 *    This method supports all three memory allocation flags which makes it   
 *    suitable for almost all circumstances.
 */
void* MemoryContextAllocExtendedDebug(MemoryContext context, Size size, int flags, const char* file, int line)
{
    void* ret = NULL;
    bool allocsz_is_valid = false;

	Assert(MemoryContextIsValid(context));
#ifdef MEMORY_CONTEXT_CHECKING
    PreventActionOnSealedContext(context);
#endif

    /* Make sure memory allocation size is valid. */
	if ((flags & MCXT_ALLOC_HUGE) != 0) {
        allocsz_is_valid = AllocHugeSizeIsValid(size);
    } else {
        allocsz_is_valid = AllocSizeIsValid(size);
    }

    if (!allocsz_is_valid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

	context->isReset = false;

    /* Invoke memory allocator */
    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if ((flags & MCXT_ALLOC_NO_OOM) != 0) {
        /* Do nothing */
    } else if (unlikely(ret == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY), errmsg("memory is temporarily unavailable"),
                        errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                                  (unsigned long)size, u_sess->debug_query_id, file, line)));
    }

    /* Set aligned if MCXT_ALLOC_ZERO */
    if ((flags & MCXT_ALLOC_ZERO) != 0) {
        MemSetAligned(ret, 0, size);
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * std_palloc_extended
 *    palloc with flags, it will return NULL while OOM happend.
 */
void* std_palloc_extended(Size size, int flags)
{
    /* duplicates MemoryContextAllocExtended to avoid increased overhead */
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(CurrentMemoryContext));
#ifdef MEMORY_CONTEXT_CHECKING
    PreventActionOnSealedContext(CurrentMemoryContext);
#endif
    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu", (unsigned long)size)));
    }

    CurrentMemoryContext->isReset = false;

    ret = (*CurrentMemoryContext->methods->alloc)(CurrentMemoryContext, 0, size, __FILE__, __LINE__);
    if (ret == NULL) {
        /*
         * If flag has not MCXT_ALLOC_NO_OOM, we must ereport ERROR here
         */
        return NULL;
    }

    if ((flags & MCXT_ALLOC_ZERO) != 0)
        MemSetAligned(ret, 0, size);

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(CurrentMemoryContext, size, __FILE__, __LINE__);
    }
    InsertMemoryAllocInfo(ret, CurrentMemoryContext, __FILE__, __LINE__, size);

    return ret;
}

/*
 * std_palloc0_noexcept
 *    palloc without exception, it will return NULL while OOM happend.
 *    the memory will reset 0 if alloc successful.
 */
void* std_palloc0_noexcept(Size size)
{
    return palloc_extended(size, MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
}

/*
 * pfree
 *		Release an allocated chunk.
 */
void pfree(void* pointer)
{
    MemoryContext context = NULL;

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

#ifndef ENABLE_MEMORY_CHECK
    /*
     * OK, it's probably safe to look at the chunk header.
     */
    context = ((StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE))->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    context = &(block->aset->header);
#endif

    if (IsOptAllocSetContext(context)) {
        opt_pfree(pointer);
        return;
    }

    AssertArg(MemoryContextIsValid(context));
#ifdef MEMORY_CONTEXT_CHECKING
    if (!IsTopMemCxt(context)) {
        /* No prevent on top memory context as they may be sealed. */
        PreventActionOnSealedContext(context);
    }
#endif
    RemoveMemoryAllocInfo(pointer, context);

    (*context->methods->free_p)(context, pointer);
}

void* repalloc_noexcept_Debug(void* pointer, Size size, const char* file, int line)
{
    MemoryContext context;
    void* ret = NULL;

#ifndef ENABLE_MEMORY_CHECK
    /*
     * OK, it's probably safe to look at the chunk header.
     */
    context = ((StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE))->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    context = &(block->aset->header);
#endif

    if (IsOptAllocSetContext(context)) {
        return opt_repalloc_noexcept_Debug(pointer, size, file, line);
    }

    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    AssertArg(MemoryContextIsValid(context));

    /* isReset must be false already */
    Assert(!context->isReset);

    RemoveMemoryAllocInfo(pointer, context);

    ret = (*context->methods->realloc)(context, pointer, 0, size, file, line);

    if (ret == NULL) {
        return NULL;
    }
    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * repalloc
 *		Adjust the size of a previously allocated chunk.
 */
void* repallocDebug(void* pointer, Size size, const char* file, int line)
{
    MemoryContext context;
    void* ret = NULL;

#ifndef ENABLE_MEMORY_CHECK
    /*
     * OK, it's probably safe to look at the chunk header.
     */
    context = ((StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE))->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    context = &(block->aset->header);
#endif

    if (IsOptAllocSetContext(context)) {
        return opt_repallocDebug(pointer, size, file, line);
    }

    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    AssertArg(MemoryContextIsValid(context));
    PreventActionOnSealedContext(context);
    /* isReset must be false already */
    Assert(!context->isReset);

    RemoveMemoryAllocInfo(pointer, context);

    ret = (*context->methods->realloc)(context, pointer, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * MemoryContextMemalignAllocDebug
 *		Adjust the size of a previously allocated chunk.
 */
void* MemoryContextMemalignAllocDebug(MemoryContext context, Size align, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(context));

    if (!AllocSizeIsValid(size)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", (unsigned long)size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, align, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/**
 * @Description: Allocate (possibly-expansive) space within the specified context.
 *				See considerations in comment at MaxAllocHugeSize.
 * @in context - the pointer of memory context
 * @in size - the allocated size
 * @in file - which file are allocating memory
 * @in line - which line are allocating memory
 */
void* MemoryContextAllocHugeDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(context));

    if (!AllocHugeSizeIsValid(size)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                errmsg("invalid memory alloc request size %zu in %s:%d", size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if (unlikely(ret == NULL))
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/**
 * @Description: Allocate (possibly-expansive) space within the specified context.
 *				See considerations in comment at MaxAllocHugeSize.
 * @in context - the pointer of memory context
 * @in size - the allocated size
 * @in file - which file are allocating memory
 * @in line - which line are allocating memory
 */
void* MemoryContextAllocHugeZeroDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(MemoryContextIsValid(context));

    if (!AllocHugeSizeIsValid(size)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                errmsg("invalid memory alloc request size %zu in %s:%d", size, file, line)));
    }

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }
    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/**
 * @Description: Adjust the size of a previously allocated chunk, permitting a large
 *				value.  The previous allocation need not have been "huge".
 * @in pointer -  the pointer to the orignal memory region
 * @in size - the allocated size
 * @in file - which file are allocating memory
 * @in line - which line are allocating memory
 */
void* repallocHugeDebug(void* pointer, Size size, const char* file, int line)
{
    MemoryContext context;
    void* ret = NULL;

#ifndef ENABLE_MEMORY_CHECK
    /*
     * OK, it's probably safe to look at the chunk header.
     */
    context = ((StandardChunkHeader*)((char*)pointer - STANDARDCHUNKHEADERSIZE))->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(pointer)) - ASAN_BLOCKHDRSZ));
    context = &(block->aset->header);
#endif

    if (IsOptAllocSetContext(context)) {
        return opt_repallocDebug(pointer, size, file, line);
    }

    if (!AllocHugeSizeIsValid(size)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid memory alloc request size %lu in %s:%d", size, file, line)));
    }

    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    AssertArg(MemoryContextIsValid(context));

    /* isReset must be false already */
    Assert(!context->isReset);

    RemoveMemoryAllocInfo(pointer, context);

    ret = (*context->methods->realloc)(context, pointer, 0, size, file, line);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

#ifdef MEMORY_CONTEXT_CHECKING
    /* check if the memory context is out of control */
    MemoryContextCheckMaxSize(context, size, file, line);
#endif

    /* check if the session used memory is beyond the limitation */
    if (unlikely(STATEMENT_MAX_MEM)) {
        MemoryContextCheckSessionMemory(context, size, file, line);
    }

    InsertMemoryAllocInfo(ret, context, file, line, size);

    return ret;
}

/*
 * MemoryContextMemalignFree
 *		Release an allocated chunk.
 */
void MemoryContextMemalignFree(MemoryContext context, void* pointer)
{
    /*
     * Try to detect bogus pointers handed to us, poorly though we can.
     * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
     * allocated chunk.
     */
    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));
    AssertArg(MemoryContextIsValid(context));

    RemoveMemoryAllocInfo(pointer, context);

    (*context->methods->free_p)(context, (char*)pointer);
}

/*
 * MemoryContextSwitchTo
 *		Returns the current context; installs the given context.
 *
 * palloc.h defines an inline version of this function if allowed by the
 * compiler; in which case the definition below is skipped.
 */
#ifndef USE_INLINE

MemoryContext MemoryContextSwitchTo(MemoryContext context)
{
    MemoryContext old;

    AssertArg(MemoryContextIsValid(context));

    old = CurrentMemoryContext;
    CurrentMemoryContext = context;
    return old;
}
#endif /* ! USE_INLINE */

/*
 * MemoryContextStrdup
 *		Like strdup(), but allocate from the specified context
 */
char* MemoryContextStrdupDebug(MemoryContext context, const char* string, const char* file, int line)
{
    char* nstr = NULL;
    Size len;
    errno_t rc;

    if (string == NULL)
        return NULL; /* nothing to do */

    len = strlen(string) + 1;

    nstr = (char*)MemoryContextAllocDebug(context, len, file, line);

    rc = memcpy_s(nstr, len, string, len);
    securec_check(rc, "\0", "\0");

    return nstr;
}

/*
 * pnstrdup
 *		Like pstrdup(), but append null byte to a
 *		not-necessarily-null-terminated input string.
 */
char* pnstrdupDebug(const char* in, Size len, const char* file, int line)
{
    char* out = (char*)MemoryContextAllocDebug(CurrentMemoryContext, len + 1, file, line);

    errno_t rc = memcpy_s(out, len + 1, in, len);
    securec_check(rc, "\0", "\0");

    out[len] = '\0';
    return out;
}

#if defined(WIN32) || defined(__CYGWIN__)
/*
 *	Memory support routines for libpgport on Win32
 *
 *	Win32 can't load a library that PGDLLIMPORTs a variable
 *	if the link object files also PGDLLIMPORT the same variable.
 *	For this reason, libpgport can't reference CurrentMemoryContext
 *	in the palloc macro calls.
 *
 *	To fix this, we create several functions here that allow us to
 *	manage memory without doing the inline in libpgport.
 */
void* pgport_palloc(Size sz)
{
    return palloc(sz);
}

char* pgport_pstrdup(const char* str)
{
    return pstrdup(str);
}

/* Doesn't reference a PGDLLIMPORT variable, but here for completeness. */
void pgport_pfree(void* pointer)
{
    pfree(pointer);
}

#endif

#ifdef PGXC
#include "gen_alloc.h"

void* current_memcontext(void);

void* current_memcontext()
{
    return ((void*)CurrentMemoryContext);
}

void* allocTopCxt(size_t s)
{
    return MemoryContextAlloc(t_thrd.top_mem_cxt, (Size)s);
}

void* gen_alloc(void* context, size_t s)
{
    return MemoryContextAlloc((MemoryContext)context, (Size)s);
}

void* gen_alloc0(void* context, size_t s)
{
    return MemoryContextAllocZero((MemoryContext)context, (Size)s);
}

void* gen_repalloc(void* ptr, size_t s)
{
    return repalloc(ptr, (Size)s);
}

Gen_Alloc genAlloc_class = {(void* (*)(void*, size_t))gen_alloc,
    (void* (*)(void*, size_t))gen_alloc0,
    (void* (*)(void*, size_t))gen_repalloc,
    (void (*)(void*))pfree,
    (void* (*)())current_memcontext,
    (void* (*)(size_t))allocTopCxt};

#endif

void std_MemoryContextDestroyAtThreadExit(MemoryContext context)
{
    MemoryContext pContext = context;
    if (!IsTopMemCxt(context)) {
        PreventActionOnSealedContext(context);
    } else {
#ifdef MEMORY_CONTEXT_CHECKING
        /* before delete top memcxt,  you should close lsc */
        if (EnableGlobalSysCache() && context == t_thrd.top_mem_cxt && t_thrd.lsc_cxt.lsc != NULL) {
            Assert(t_thrd.lsc_cxt.lsc->is_closed);
        }
#endif
    }

    if (pContext != NULL) {
        /* To avoid delete current context */
        MemoryContextSwitchTo(pContext);

        /* Delete all its decendents */
        Assert(!pContext->parent);
        MemoryContextDeleteChildren(pContext, NULL);

        /* Delete the top context itself */
        RemoveMemoryContextInfo(pContext);
        (*pContext->methods->delete_context)(pContext);

        if (pContext != t_thrd.top_mem_cxt)
            return;

        /*
         *	Lock this thread's delete MemoryContext.
         *	Because pv_session_memory_detail view will recursive MemoryContext tree.
         *	Relate to pgstatfuncs.c:pvSessionMemoryDetail().
         */
        if (t_thrd.proc != NULL && t_thrd.proc->topmcxt != NULL) {
            (void)syscalllockAcquire(&t_thrd.proc->deleMemContextMutex);
            free(pContext);
            (void)syscalllockRelease(&t_thrd.proc->deleMemContextMutex);
        } else {
            free(pContext);
        }
    }
}

MemoryContext MemoryContextOriginal(const char* node)
{
    MemoryContext context;
#ifndef ENABLE_MEMORY_CHECK
    context = ((StandardChunkHeader*)((char*)node - STANDARDCHUNKHEADERSIZE))->context;
#else
    AsanBlock block = ((AsanBlock)(((char*)(node)) - ASAN_BLOCKHDRSZ));
    context = &(block->aset->header);
#endif
    AssertArg(MemoryContextIsValid(context));
    return context;
}

static void FreeMemoryContextList(List* context_list)
{
    ListCell *cell = list_head(context_list);
    void* context = NULL;
    while (cell != NULL) {
        context = lfirst(cell);
        cell = cell->next;
        pfree_ext(context);
    }
}
