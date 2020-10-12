/* -------------------------------------------------------------------------
 *
 * dsm.c
 * manage dynamic shared memory segments
 *
 * This file provides a set of services to make programming with dynamic
 * shared memory segments more convenient.  Unlike the low-level
 * facilities provided by dsm_impl.h and dsm_impl.c, mappings and segments
 * created using this module will be cleaned up automatically.  Mappings
 * will be removed when the resource owner under which they were created
 * is cleaned up, unless dsm_pin_mapping() is used, in which case they
 * have session lifespan.  Segments will be removed when there are no
 * remaining mappings, or at postmaster shutdown in any case.  After a
 * hard postmaster crash, remaining segments will be removed, if they
 * still exist, at the next postmaster startup.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/storage/ipc/dsm.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "storage/dsm.h"
#include "knl/knl_session.h"
#include "utils/memutils.h"
#include "postmaster/bgworker_internals.h"

/* Backend-local tracking for on-detach callbacks. */
typedef struct __dsm_segment_detach_callback {
    on_dsm_detach_callback function;
    Datum arg;
    slist_node node;
} dsm_segment_detach_callback;

#ifdef __USE_NUMA
static void RestoreCpuAffinity(cpu_set_t *cpuset)
{
    /* Resotre CPU affinity after parallel query is done. */
    if (cpuset != NULL) {
        int rc = pthread_setaffinity_np(t_thrd.proc->pid, sizeof(cpu_set_t), cpuset);
        if (rc != 0) {
            ereport(WARNING, (errmsg("pthread_setaffinity_np failed:%d", rc)));
        }
    }
}
#endif

/*
 * newMemCtx means whether the memctx is new created or not.
 * When newMemCtx is true, we just need to call pfree to free the mem,
 * or we can call MemoryContextDelete to jsut delete the whole new created
 * memctx.
 */
void dsm_detach(void **seg, bool newMemCtx)
{
    Assert(seg != NULL);
    Assert(*seg != NULL);
    knl_u_parallel_context *ctx = (knl_u_parallel_context *)*seg;
    /*
     * Invoke registered callbacks.  Just in case one of those callbacks
     * throws a further error that brings us back here, pop the callback
     * before invoking it, to avoid infinite error recursion.
     */
    while (!slist_is_empty(&ctx->on_detach)) {
        slist_node *node = slist_pop_head_node(&ctx->on_detach);
        dsm_segment_detach_callback *cb = slist_container(dsm_segment_detach_callback, node, node);
        on_dsm_detach_callback function = cb->function;
        Datum arg = cb->arg;
        pfree(cb);

        function(seg, arg);
    }

    if (newMemCtx) {
#ifdef __USE_NUMA
        RestoreCpuAffinity(ctx->pwCtx->cpuset);
#endif
        MemoryContextDelete(ctx->memCtx);
        ctx->memCtx = NULL;
        ctx->pwCtx = NULL;
        ctx->used = false;
    } else {
        pfree(ctx->pwCtx);
        pfree(ctx);
    }
}

void *dsm_create(void)
{
    for (int i = 0; i < DSM_MAX_ITEM_PER_QUERY; i++) {
        if (u_sess->parallel_ctx[i].used == false) {
            u_sess->parallel_ctx[i].memCtx = AllocSetContextCreate(u_sess->top_mem_cxt, "parallel query",
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);

            MemoryContext oldContext = MemoryContextSwitchTo(u_sess->parallel_ctx[i].memCtx);
            u_sess->parallel_ctx[i].pwCtx = (ParallelInfoContext *)palloc0(sizeof(ParallelInfoContext));
            (void)MemoryContextSwitchTo(oldContext);

            u_sess->parallel_ctx[i].used = true;
            slist_init(&u_sess->parallel_ctx[i].on_detach);
            return &(u_sess->parallel_ctx[i]);
        }
    }

    ereport(WARNING, (errmsg("too many dynamic shared memory segments")));
    return NULL;
}

/*
 * Register an on-detach callback for a dynamic shared memory segment.
 */
void on_dsm_detach(void *seg, on_dsm_detach_callback function, Datum arg)
{
    dsm_segment_detach_callback *cb =
        (dsm_segment_detach_callback*)MemoryContextAlloc(TopMemoryContext, sizeof(dsm_segment_detach_callback));
    cb->function = function;
    cb->arg = arg;
    knl_u_parallel_context *ctx = (knl_u_parallel_context *)seg;
    slist_push_head(&ctx->on_detach, &cb->node);
}
