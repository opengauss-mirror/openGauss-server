/* -------------------------------------------------------------------------
 *
 * std_mcxt.cpp
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

void* opt_MemoryAllocFromContext(MemoryContext context, Size size)
{
    void* ret = NULL;

    context->isReset = false;

    ret = (*context->methods->alloc)(context, 0, size, __FILE__, __LINE__);
    if (ret == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu.",
                    (unsigned long)size,
                    u_sess->debug_query_id)));

    return ret;
}

static void opt_TagMemoryContextSessionId(MemoryContext node, MemoryContext parent)
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
MemoryContext opt_MemoryContextCreate(MemoryContext node, NodeTag tag,
    MemoryContextMethods *methods, MemoryContext parent, const char* name)
{
    node->type = tag;
    node->parent = NULL; /* for the moment */
    node->firstchild = NULL;
    node->prevchild = NULL;
    node->nextchild = NULL;
    node->isReset = true;
    node->is_sealed = false;
    node->is_shared = false;
    node->methods = methods;
    node->name = (char *)name;
    node->cell.data.ptr_value = (void*)node;
    node->cell.next = NULL;
    node->thread_id = gs_thread_self();
    opt_TagMemoryContextSessionId(node, parent);

    /* OK to link node to parent (if any) */
    /* Could use MemoryContextSetParent here, but doesn't seem worthwhile */
    if (parent) {
        node->parent = parent;
        node->nextchild = parent->firstchild;
        if (parent->firstchild != NULL)
            parent->firstchild->prevchild = node;
        parent->firstchild = node;

        node->level = parent->level + 1;
    } else {
        node->nextchild = NULL;
        node->level = 0;
    }

    /* Return to type-specific creation routine to finish up */
    return node;
}

void* opt_MemoryContextAllocZeroAlignedDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(IsOptAllocSetContext(context));

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

    MemSetLoop(ret, 0, size);

    return ret;
}

void* opt_MemoryContextAllocZeroDebug(MemoryContext context, Size size, const char* file, int line)
{
    void* ret = NULL;

    AssertArg(IsOptAllocSetContext(context));

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

    MemSetAligned(ret, 0, size);

    return ret;
}

void opt_MemoryContextSetParent(MemoryContext context, MemoryContext new_parent)
{
    AssertArg(IsOptAllocSetContext(context));
    AssertArg(context != new_parent);

    if (new_parent == context->parent)
        return;

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
        AssertArg(IsOptAllocSetContext(new_parent));
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

void opt_MemoryContextDeleteInternal(MemoryContext context, List *context_list)
{
    AssertArg(IsOptAllocSetContext(context));

    /* save a function call in common case where there are no children */
    if (context->firstchild != NULL)
        MemoryContextDeleteChildren(context, context_list);

    opt_MemoryContextSetParent(context, NULL);

    context->methods->delete_context(context);
}

void opt_MemoryContextReset(MemoryContext context)
{
    AssertArg(IsOptAllocSetContext(context));

    /* save a function call in common case where there are no children */
    if (context->firstchild != NULL)
        MemoryContextResetChildren(context);

    /* Nothing to do if no pallocs since startup or last reset */
    if (!context->isReset) {
        (*context->methods->reset)(context);
        context->isReset = true;
    }
}

/*
 * pfree
 *		Release an allocated chunk.
 */
void
opt_pfree(void *pointer)
{
    MemoryContext context = GetMemoryChunkContext(pointer);

    context->methods->free_p(context, pointer);
}

void*
opt_repallocDebug(void* pointer, Size size, const char *file, int line)
{
    MemoryContext context;
    void* ret = NULL;

    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    context = GetMemoryChunkContext(pointer);

    /* isReset must be false already */
    Assert(!context->isReset);

    ret = (*context->methods->realloc)(context, pointer, 0, size, file, line);
    if (unlikely(ret == NULL))
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory is temporarily unavailable"),
                errdetail("Failed on request of size %lu bytes under queryid %lu in %s:%d.",
                    (unsigned long)size,
                    u_sess->debug_query_id,
                    file,
                    line)));

    return ret;
}

void*
opt_repalloc_noexcept_Debug(void* pointer, Size size, const char *file, int line)
{
    MemoryContext context;
    void* ret = NULL;

    Assert(pointer != NULL);
    Assert(pointer == (void*)MAXALIGN(pointer));

    context = GetMemoryChunkContext(pointer);

    /* isReset must be false already */
    Assert(!context->isReset);

    ret = (*context->methods->realloc)(context, pointer, 0, size, file, line);

    return ret;
}
