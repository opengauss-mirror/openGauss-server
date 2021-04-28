/*-------------------------------------------------------------------------
 *
 * resowner.c
 *	  POSTGRES resource owner management code.
 *
 * Query-lifespan resources are tracked by associating them with
 * ResourceOwner objects.  This provides a simple mechanism for ensuring
 * that such resources are freed at the right time.
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/resowner/resowner.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "storage/cucache_mgr.h"
#include "executor/executor.h"
#include "catalog/pg_hashbucket_fn.h"
/*
 * ResourceOwner objects look like this
 */
typedef struct ResourceOwnerData {
    ResourceOwner parent;     /* NULL if no parent (toplevel owner) */
    ResourceOwner firstchild; /* head of linked list of children */
    ResourceOwner nextchild;  /* next child of same parent */
    const char* name;         /* name (just for debugging) */

    /* We have built-in support for remembering owned buffers */
    int nbuffers;    /* number of owned buffer pins */
    Buffer* buffers; /* dynamically allocated array */
    int maxbuffers;  /* currently allocated array size */

    /* We have built-in support for remembering catcache references */
    int ncatrefs;       /* number of owned catcache pins */
    HeapTuple* catrefs; /* dynamically allocated array */
    int maxcatrefs;     /* currently allocated array size */

    int ncatlistrefs;       /* number of owned catcache-list pins */
    CatCList** catlistrefs; /* dynamically allocated array */
    int maxcatlistrefs;     /* currently allocated array size */

    /* We have built-in support for remembering relcache references */
    int nrelrefs;      /* number of owned relcache pins */
    Relation* relrefs; /* dynamically allocated array */
    int maxrelrefs;    /* currently allocated array size */
    /* We have built-in support for remembering partcache references */
    int npartrefs;       /* number of owned partcache pins */
    Partition* partrefs; /* dynamically allocated array */
    int maxpartrefs;     /* currently allocated array size */

    /* We have built-in support for remembering  references */
    int nfakerelrefs;      /* number of owned relcache pins */
    dlist_head fakerelrefs_list; /* a list of fakeRelation */
    
    /* We have built-in support for remembering  references */
    int nfakepartrefs;       /* number of owned partcache pins */
    Partition* fakepartrefs; /* dynamically allocated array */
    int maxfakepartrefs;     /* currently allocated array size */
    /* We have built-in support for remembering plancache references */
    int nplanrefs;         /* number of owned plancache pins */
    CachedPlan** planrefs; /* dynamically allocated array */
    int maxplanrefs;       /* currently allocated array size */

    /* We have built-in support for remembering tupdesc references */
    int ntupdescs;       /* number of owned tupdesc references */
    TupleDesc* tupdescs; /* dynamically allocated array */
    int maxtupdescs;     /* currently allocated array size */

    /* We have built-in support for remembering snapshot references */
    int nsnapshots;      /* number of owned snapshot references */
    Snapshot* snapshots; /* dynamically allocated array */
    int maxsnapshots;    /* currently allocated array size */

    /* We have built-in support for remembering open temporary files */
    int nfiles;   /* number of owned temporary files */
    File* files;  /* dynamically allocated array */
    int maxfiles; /* currently allocated array size */

    /* We have built-in support for remembering owned cache slots in CStore and hdfs */
    int nDataCacheSlots;           /* number of owned data cache block pins */
    CacheSlotId_t* dataCacheSlots; /* dynamically allocated array */
    int maxDataCacheSlots;         /* currently allocated array size */

    int nMetaCacheSlots;           /* number of owned meta cache block pins */
    CacheSlotId_t* metaCacheSlots; /* dynamically allocated array */
    int maxMetaCacheSlots;         /* currently allocated array size */

    // We have built-in support for remembering pthread_mutex
    //
    int nPthreadMutex;
    pthread_mutex_t** pThdMutexs;
    int maxPThdMutexs;

    /* We have built-in support for remembering partition map references */
    int npartmaprefs;
    PartitionMap** partmaprefs;
    int maxpartmaprefs;

    /* track global memory context */
    int nglobalMemContext;
    MemoryContext* globalMemContexts;
    int maxGlobalMemContexts;
} ResourceOwnerData;

THR_LOCAL ResourceOwner IsolatedResourceOwner = NULL;

/*
 * List of add-on callbacks for resource releasing
 */
typedef struct ResourceReleaseCallbackItem {
    struct ResourceReleaseCallbackItem* next;
    ResourceReleaseCallback callback;
    void* arg;
} ResourceReleaseCallbackItem;

/* Internal routines */
static void ResourceOwnerReleaseInternal(
    ResourceOwner owner, ResourceReleasePhase phase, bool isCommit, bool isTopLevel);
static void PrintRelCacheLeakWarning(Relation rel);
static void PrintPartCacheLeakWarning(Partition part);
static void PrintFakePartLeakWarning(Partition fakepart);
static void PrintFakeRelLeakWarning(Relation fakerel);
static void PrintPlanCacheLeakWarning(CachedPlan* plan);
static void PrintTupleDescLeakWarning(TupleDesc tupdesc);
static void PrintSnapshotLeakWarning(Snapshot snapshot);
static void PrintFileLeakWarning(File file);

extern void PrintMetaCacheBlockLeakWarning(CacheSlotId_t slot);
extern void ReleaseMetaBlock(CacheSlotId_t slot);

/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/
/*
 * ResourceOwnerCreate
 *		Create an empty ResourceOwner.
 *
 * All ResourceOwner objects are kept in t_thrd.top_mem_cxt, since they should
 * only be freed explicitly.
 */
ResourceOwner ResourceOwnerCreate(ResourceOwner parent, const char* name, MemoryGroupType memGroup)
{
    ResourceOwner owner;

    owner = (ResourceOwner)MemoryContextAllocZero(
        THREAD_GET_MEM_CXT_GROUP(memGroup), sizeof(ResourceOwnerData));
    owner->name = name;

    if (parent) {
        owner->parent = parent;
        owner->nextchild = parent->firstchild;
        parent->firstchild = owner;
    }

    if (parent == NULL && strcmp(name, "TopTransaction") != 0)
        IsolatedResourceOwner = owner;

    return owner;
}

/*
 * ResourceOwnerRelease
 *		Release all resources owned by a ResourceOwner and its descendants,
 *		but don't delete the owner objects themselves.
 *
 * Note that this executes just one phase of release, and so typically
 * must be called three times.	We do it this way because (a) we want to
 * do all the recursion separately for each phase, thereby preserving
 * the needed order of operations; and (b) xact.c may have other operations
 * to do between the phases.
 *
 * phase: release phase to execute
 * isCommit: true for successful completion of a query or transaction,
 *			false for unsuccessful
 * isTopLevel: true if completing a main transaction, else false
 *
 * isCommit is passed because some modules may expect that their resources
 * were all released already if the transaction or portal finished normally.
 * If so it is reasonable to give a warning (NOT an error) should any
 * unreleased resources be present.  When isCommit is false, such warnings
 * are generally inappropriate.
 *
 * isTopLevel is passed when we are releasing TopTransactionResourceOwner
 * at completion of a main transaction.  This generally means that *all*
 * resources will be released, and so we can optimize things a bit.
 */
void ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool isCommit, bool isTopLevel)
{
    /* Rather than PG_TRY at every level of recursion, set it up once */
    ResourceOwner save;

    save = t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        ResourceOwnerReleaseInternal(owner, phase, isCommit, isTopLevel);
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = save;
        PG_RE_THROW();
    }
    PG_END_TRY();
    t_thrd.utils_cxt.CurrentResourceOwner = save;
}

static void ResourceOwnerReleaseInternal(
    ResourceOwner owner, ResourceReleasePhase phase, bool isCommit, bool isTopLevel)
{
    ResourceOwner child;
    ResourceOwner save;
    ResourceReleaseCallbackItem* item = NULL;

    /* Recurse to handle descendants */
    for (child = owner->firstchild; child != NULL; child = child->nextchild)
        ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);

    /*
     * Make CurrentResourceOwner point to me, so that ReleaseBuffer etc don't
     * get confused.  We needn't PG_TRY here because the outermost level will
     * fix it on error abort.
     */
    save = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = owner;

    if (phase == RESOURCE_RELEASE_BEFORE_LOCKS) {
        /*
         * Release buffer pins.  Note that ReleaseBuffer will remove the
         * buffer entry from my list, so I just have to iterate till there are
         * none.
         *
         * During a commit, there shouldn't be any remaining pins --- that
         * would indicate failure to clean up the executor correctly --- so
         * issue warnings.	In the abort case, just clean up quietly.
         *
         * We are careful to do the releasing back-to-front, so as to avoid
         * O(N^2) behavior in ResourceOwnerForgetBuffer().
         */
        while (owner->nbuffers > 0) {
            if (isCommit)
                PrintBufferLeakWarning(owner->buffers[owner->nbuffers - 1]);
            ReleaseBuffer(owner->buffers[owner->nbuffers - 1]);
        }
        while (owner->nDataCacheSlots > 0) {
            if (isCommit)
                CUCache->PrintDataCacheSlotLeakWarning(owner->dataCacheSlots[owner->nDataCacheSlots - 1]);
            CUCache->UnPinDataBlock(owner->dataCacheSlots[owner->nDataCacheSlots - 1]);
        }
        while (owner->nMetaCacheSlots > 0) {
            if (isCommit)
                PrintMetaCacheBlockLeakWarning(owner->metaCacheSlots[owner->nMetaCacheSlots - 1]);
            ReleaseMetaBlock(owner->metaCacheSlots[owner->nMetaCacheSlots - 1]);
        }

        while (owner->nfakerelrefs > 0) {
            dlist_node *tail_node = dlist_tail_node(&(owner->fakerelrefs_list));
            Relation nfakerelref = (Relation)dlist_container(struct RelationData, node, tail_node);
            if (isCommit) {
                PrintFakeRelLeakWarning(nfakerelref);
            }
            releaseDummyRelation(&nfakerelref);
        }

        while (owner->nfakepartrefs > 0) {
            if (isCommit)
                PrintFakePartLeakWarning(owner->fakepartrefs[owner->nfakepartrefs - 1]);
            bucketClosePartition(owner->fakepartrefs[owner->nfakepartrefs - 1]);
        }

        /* release partition map ref count */
        while (owner->npartmaprefs > 0) {
            if (isCommit)
                ereport(WARNING, (errmsg("partition map reference leak")));
            decre_partmap_refcount(owner->partmaprefs[owner->npartmaprefs - 1]);
        }

        /*
         * Release relcache references.  Note that RelationClose will remove
         * the relref entry from my list, so I just have to iterate till there
         * are none.
         *
         * As with buffer pins, warn if any are left at commit time, and
         * release back-to-front for speed.
         */
        while (owner->nrelrefs > 0) {
            if (isCommit)
                PrintRelCacheLeakWarning(owner->relrefs[owner->nrelrefs - 1]);
            RelationClose(owner->relrefs[owner->nrelrefs - 1]);
        }
        while (owner->npartrefs > 0) {
            if (isCommit)
                PrintPartCacheLeakWarning(owner->partrefs[owner->npartrefs - 1]);
            PartitionClose(owner->partrefs[owner->npartrefs - 1]);
        }
        // Ditto for pthread mutex
        //
        while (owner->nPthreadMutex > 0) {
            if (isCommit)
                PrintPthreadMutexLeakWarning(owner->pThdMutexs[owner->nPthreadMutex - 1]);
            PthreadMutexUnlock(owner, owner->pThdMutexs[owner->nPthreadMutex - 1]);
        }
    } else if (phase == RESOURCE_RELEASE_LOCKS) {
        if (isTopLevel) {
            /*
             * For a top-level xact we are going to release all locks (or at
             * least all non-session locks), so just do a single lmgr call at
             * the top of the recursion.
             */
            if (owner == t_thrd.utils_cxt.TopTransactionResourceOwner) {
                ProcReleaseLocks(isCommit);
                ReleasePredicateLocks(isCommit);
            }
        } else {
            /*
             * Release locks retail.  Note that if we are committing a
             * subtransaction, we do NOT release its locks yet, but transfer
             * them to the parent.
             */
            Assert(owner->parent != NULL);
            if (isCommit)
                LockReassignCurrentOwner();
            else
                LockReleaseCurrentOwner();
        }
    } else if (phase == RESOURCE_RELEASE_AFTER_LOCKS) {
        /*
         * Release catcache references.  Note that ReleaseCatCache will remove
         * the catref entry from my list, so I just have to iterate till there
         * are none.
         *
         * As with buffer pins, warn if any are left at commit time, and
         * release back-to-front for speed.
         */
        while (owner->ncatrefs > 0) {
            if (isCommit)
                PrintCatCacheLeakWarning(owner->catrefs[owner->ncatrefs - 1]);
            ReleaseCatCache(owner->catrefs[owner->ncatrefs - 1]);
        }
        /* Ditto for catcache lists */
        while (owner->ncatlistrefs > 0) {
            if (isCommit)
                PrintCatCacheListLeakWarning(owner->catlistrefs[owner->ncatlistrefs - 1]);
            ReleaseCatCacheList(owner->catlistrefs[owner->ncatlistrefs - 1]);
        }
        /* Ditto for plancache references */
        while (owner->nplanrefs > 0) {
            if (isCommit)
                PrintPlanCacheLeakWarning(owner->planrefs[owner->nplanrefs - 1]);
            ReleaseCachedPlan(owner->planrefs[owner->nplanrefs - 1], true);
        }
        /* Ditto for tupdesc references */
        while (owner->ntupdescs > 0) {
            if (isCommit)
                PrintTupleDescLeakWarning(owner->tupdescs[owner->ntupdescs - 1]);
            DecrTupleDescRefCount(owner->tupdescs[owner->ntupdescs - 1]);
        }
        /* Ditto for snapshot references */
        while (owner->nsnapshots > 0) {
            if (isCommit)
                PrintSnapshotLeakWarning(owner->snapshots[owner->nsnapshots - 1]);
            UnregisterSnapshot(owner->snapshots[owner->nsnapshots - 1]);
        }

        /* Ditto for temporary files */
        while (owner->nfiles > 0) {
            if (isCommit)
                PrintFileLeakWarning(owner->files[owner->nfiles - 1]);
            FileClose(owner->files[owner->nfiles - 1]);
        }        
        /* Ditto for global memory context */
        while (owner->nglobalMemContext > 0) {
            MemoryContext memContext = owner->globalMemContexts[owner->nglobalMemContext - 1];
            if (isCommit)
                PrintGMemContextLeakWarning(memContext);
            MemoryContextDelete(memContext);
            ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, memContext);
        }

        /* Clean up index scans too */
        ReleaseResources_hash();
    }

    /* Let add-on modules get a chance too */
    for (item = t_thrd.utils_cxt.ResourceRelease_callbacks; item; item = item->next)
        (*item->callback)(phase, isCommit, isTopLevel, item->arg);

    t_thrd.utils_cxt.CurrentResourceOwner = save;
}

static void ResourceOwnerFreeOwner(ResourceOwner owner)
{
    if (owner->buffers)
        pfree(owner->buffers);
    if (owner->catrefs)
        pfree(owner->catrefs);
    if (owner->catlistrefs)
        pfree(owner->catlistrefs);
    if (owner->relrefs)
        pfree(owner->relrefs);
    if (owner->partrefs)
        pfree(owner->partrefs);
    if (owner->planrefs)
        pfree(owner->planrefs);
    if (owner->tupdescs)
        pfree(owner->tupdescs);
    if (owner->snapshots)
        pfree(owner->snapshots);
    if (owner->files)
        pfree(owner->files);
    if (owner->dataCacheSlots)
        pfree(owner->dataCacheSlots);
    if (owner->metaCacheSlots)
        pfree(owner->metaCacheSlots);
    if (owner->pThdMutexs)
        pfree(owner->pThdMutexs);
    if (owner->partmaprefs)
        pfree(owner->partmaprefs);
    if (owner->fakepartrefs)
        pfree(owner->fakepartrefs);
    if (owner->globalMemContexts)
        pfree(owner->globalMemContexts);
    pfree(owner);
}

/*
 * ResourceOwnerDelete
 *		Delete an owner object and its descendants.
 *
 * The caller must have already released all resources in the object tree.
 */
void ResourceOwnerDelete(ResourceOwner owner)
{
    /* We had better not be deleting CurrentResourceOwner ... */
    Assert(owner != t_thrd.utils_cxt.CurrentResourceOwner);

    /* And it better not own any resources, either */
    Assert(owner->nbuffers == 0);
    Assert(owner->ncatrefs == 0);
    Assert(owner->ncatlistrefs == 0);
    Assert(owner->nrelrefs == 0);
    Assert(owner->npartrefs == 0);
    Assert(owner->nfakerelrefs == 0);
    Assert(owner->nfakepartrefs == 0);
    Assert(owner->nplanrefs == 0);
    Assert(owner->ntupdescs == 0);
    Assert(owner->nsnapshots == 0);
    Assert(owner->nfiles == 0);
    Assert(owner->nDataCacheSlots == 0);
    Assert(owner->nMetaCacheSlots == 0);
    Assert(owner->nPthreadMutex == 0);

    /*
     * Delete children.  The recursive call will delink the child from me, so
     * just iterate as long as there is a child.
     */
    if (IsolatedResourceOwner == owner)
        IsolatedResourceOwner = NULL;

    while (owner->firstchild != NULL)
        ResourceOwnerDelete(owner->firstchild);

    /*
     * We delink the owner from its parent before deleting it, so that if
     * there's an error we won't have deleted/busted owners still attached to
     * the owner tree.	Better a leak than a crash.
     */
    ResourceOwnerNewParent(owner, NULL);

    if (owner == t_thrd.utils_cxt.STPSavedResourceOwner) {
        return;
    }

    /* And free the object. */
    ResourceOwnerFreeOwner(owner);
}

/*
 * Fetch parent of a ResourceOwner (returns NULL if top-level owner)
 */
ResourceOwner ResourceOwnerGetParent(ResourceOwner owner)
{
    return owner->parent;
}


/*
 * Fetch nextchild of a ResourceOwner (returns)
 */
ResourceOwner ResourceOwnerGetNextChild(ResourceOwner owner)
{
    return owner->nextchild;
}

/*
 * Fetch name of a ResourceOwner (should always has value)
 */
const char* ResourceOwnerGetName(ResourceOwner owner)
{
    return owner->name;
}

/*
 * Fetch firstchild of a ResourceOwner
 */
ResourceOwner ResourceOwnerGetFirstChild(ResourceOwner owner)
{
    return owner->firstchild;
}

/*
 * Reassign a ResourceOwner to have a new parent
 */
void ResourceOwnerNewParent(ResourceOwner owner, ResourceOwner newparent)
{
    ResourceOwner oldparent = owner->parent;

    if (oldparent) {
        if (owner == oldparent->firstchild)
            oldparent->firstchild = owner->nextchild;
        else {
            ResourceOwner child;

            for (child = oldparent->firstchild; child; child = child->nextchild) {
                if (owner == child->nextchild) {
                    child->nextchild = owner->nextchild;
                    break;
                }
            }
        }
    }

    if (newparent) {
        Assert(owner != newparent);
        owner->parent = newparent;
        owner->nextchild = newparent->firstchild;
        newparent->firstchild = owner;
    } else {
        owner->parent = NULL;
        owner->nextchild = NULL;
    }
}

/*
 * Register or deregister callback functions for resource cleanup
 *
 * These functions are intended for use by dynamically loaded modules.
 * For built-in modules we generally just hardwire the appropriate calls.
 *
 * Note that the callback occurs post-commit or post-abort, so the callback
 * functions can only do noncritical cleanup.
 */
void RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void* arg)
{
    ResourceReleaseCallbackItem* item = NULL;

    item = (ResourceReleaseCallbackItem*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(ResourceReleaseCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = t_thrd.utils_cxt.ResourceRelease_callbacks;
    t_thrd.utils_cxt.ResourceRelease_callbacks = item;
}

void UnregisterResourceReleaseCallback(ResourceReleaseCallback callback, const void* arg)
{
    ResourceReleaseCallbackItem* item = NULL;
    ResourceReleaseCallbackItem* prev = NULL;

    prev = NULL;
    for (item = t_thrd.utils_cxt.ResourceRelease_callbacks; item; prev = item, item = item->next) {
        if (item->callback == callback && item->arg == arg) {
            if (prev != NULL)
                prev->next = item->next;
            else
                t_thrd.utils_cxt.ResourceRelease_callbacks = item->next;
            pfree(item);
            break;
        }
    }
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * buffer array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void ResourceOwnerEnlargeBuffers(ResourceOwner owner)
{
    int newmax;

    if (owner == NULL || owner->nbuffers < owner->maxbuffers)
        return; /* nothing to do */

    if (owner->buffers == NULL) {
        newmax = 16;
        owner->buffers = (Buffer*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(Buffer));
        owner->maxbuffers = newmax;
    } else {
        newmax = owner->maxbuffers * 2;
        owner->buffers = (Buffer*)repalloc(owner->buffers, newmax * sizeof(Buffer));
        owner->maxbuffers = newmax;
    }
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * buffer array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void ResourceOwnerEnlargeDataCacheSlot(ResourceOwner owner)
{
    int newmax;

    if (owner == NULL || owner->nDataCacheSlots < owner->maxDataCacheSlots)
        return; /* nothing to do */

    if (owner->dataCacheSlots == NULL) {
        newmax = 16;
        owner->dataCacheSlots = (CacheSlotId_t*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(CacheSlotId_t));
        owner->maxDataCacheSlots = newmax;
    } else {
        newmax = owner->maxDataCacheSlots * 2;
        owner->dataCacheSlots = (Buffer*)repalloc(owner->dataCacheSlots, newmax * sizeof(Buffer));
        owner->maxDataCacheSlots = newmax;
    }
}

/*
 * same as ResourceOwnerEnlargeCacheSlot()
 */
void ResourceOwnerEnlargeMetaCacheSlot(ResourceOwner owner)
{
    int newmax;

    if (owner == NULL || owner->nMetaCacheSlots < owner->maxMetaCacheSlots)
        return; /* nothing to do */

    if (owner->metaCacheSlots == NULL) {
        newmax = 16;
        owner->metaCacheSlots = (CacheSlotId_t*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(CacheSlotId_t));
        owner->maxMetaCacheSlots = newmax;
    } else {
        newmax = owner->maxMetaCacheSlots * 2;
        owner->metaCacheSlots = (Buffer*)repalloc(owner->metaCacheSlots, newmax * sizeof(Buffer));
        owner->maxMetaCacheSlots = newmax;
    }
}

/*
 * Remember that a buffer pin is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeBuffers()
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
FORCE_INLINE
void ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer buffer)
{
    if (owner != NULL) {
        Assert(owner->nbuffers < owner->maxbuffers);
        owner->buffers[owner->nbuffers] = buffer;
        owner->nbuffers++;
    }
}

/*
 * Remember that a data cache block  pin is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeCUSlots()
 *
 * We allow the case owner == NULL because the DataCacheMgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void ResourceOwnerRememberDataCacheSlot(ResourceOwner owner, CacheSlotId_t slotid)
{
    if (owner != NULL) {
        Assert(owner->nDataCacheSlots < owner->maxDataCacheSlots);
        owner->dataCacheSlots[owner->nDataCacheSlots] = slotid;
        owner->nDataCacheSlots++;
    }
}

/*
 * same as ResourceOwnerRememberDataCacheSlot()
 */
void ResourceOwnerRememberMetaCacheSlot(ResourceOwner owner, CacheSlotId_t slotid)
{
    if (owner != NULL) {
        Assert(owner->nMetaCacheSlots < owner->maxMetaCacheSlots);
        owner->metaCacheSlots[owner->nMetaCacheSlots] = slotid;
        owner->nMetaCacheSlots++;
    }
}

/*
 * Forget that a data cache block pin is owned by a ResourceOwner
 *
 * We allow the case owner == NULL because the DataCacheMgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void ResourceOwnerForgetDataCacheSlot(ResourceOwner owner, CacheSlotId_t slotid)
{
    if (owner != NULL) {
        CacheSlotId_t* dataCacheSlots = owner->dataCacheSlots;
        int nb1 = owner->nDataCacheSlots - 1;
        int i;

        /*
         * Scan back-to-front because it's more likely we are releasing a
         * recently pinned buffer.	This isn't always the case of course, but
         * it's the way to bet.
         */
        for (i = nb1; i >= 0; i--) {
            if (dataCacheSlots[i] == slotid) {
                while (i < nb1) {
                    dataCacheSlots[i] = dataCacheSlots[i + 1];
                    i++;
                }
                owner->nDataCacheSlots = nb1;
                return;
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                errmsg("data cache block %d is not owned by resource owner %s", slotid, owner->name)));
    }
}

/*
 * same as ResourceOwnerForgetDataCacheSlot()
 */
void ResourceOwnerForgetMetaCacheSlot(ResourceOwner owner, CacheSlotId_t slotid)
{
    if (owner != NULL) {
        CacheSlotId_t* metaCacheSlots = owner->metaCacheSlots;
        int nb1 = owner->nMetaCacheSlots - 1;
        int i;

        /*
         * Scan back-to-front because it's more likely we are releasing a
         * recently pinned buffer.	This isn't always the case of course, but
         * it's the way to bet.
         */
        for (i = nb1; i >= 0; i--) {
            if (metaCacheSlots[i] == slotid) {
                while (i < nb1) {
                    metaCacheSlots[i] = metaCacheSlots[i + 1];
                    i++;
                }
                owner->nMetaCacheSlots = nb1;
                return;
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                errmsg("meta cache block %d is not owned by resource owner %s", slotid, owner->name)));
    }
}

/*
 * Forget that a buffer pin is owned by a ResourceOwner
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer)
{
    if (owner != NULL) {
        Buffer* buffers = owner->buffers;
        int nb1 = owner->nbuffers - 1;
        int i;

        /*
         * Scan back-to-front because it's more likely we are releasing a
         * recently pinned buffer.	This isn't always the case of course, but
         * it's the way to bet.
         */
        for (i = nb1; i >= 0; i--) {
            if (buffers[i] == buffer) {
                while (i < nb1) {
                    buffers[i] = buffers[i + 1];
                    i++;
                }
                owner->nbuffers = nb1;
                return;
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                errmsg("buffer %d is not owned by resource owner %s", buffer, owner->name)));
    }
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * catcache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeCatCacheRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->ncatrefs < owner->maxcatrefs)
        return; /* nothing to do */

    if (owner->catrefs == NULL) {
        newmax = 16;
        owner->catrefs = (HeapTuple*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(HeapTuple));
        owner->maxcatrefs = newmax;
    } else {
        newmax = owner->maxcatrefs * 2;
        owner->catrefs = (HeapTuple*)repalloc(owner->catrefs, newmax * sizeof(HeapTuple));
        owner->maxcatrefs = newmax;
    }
}

/*
 * Remember that a catcache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeCatCacheRefs()
 */
void ResourceOwnerRememberCatCacheRef(ResourceOwner owner, HeapTuple tuple)
{
    Assert(owner->ncatrefs < owner->maxcatrefs);
    owner->catrefs[owner->ncatrefs] = tuple;
    owner->ncatrefs++;
}

/*
 * Forget that a catcache reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetCatCacheRef(ResourceOwner owner, HeapTuple tuple)
{
    HeapTuple* catrefs = owner->catrefs;
    int nc1 = owner->ncatrefs - 1;
    int i;

    for (i = nc1; i >= 0; i--) {
        if (catrefs[i] == tuple) {
            while (i < nc1) {
                catrefs[i] = catrefs[i + 1];
                i++;
            }
            owner->ncatrefs = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("catcache is not owned by resource owner %s", owner->name)));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * catcache-list reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeCatCacheListRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->ncatlistrefs < owner->maxcatlistrefs)
        return; /* nothing to do */

    if (owner->catlistrefs == NULL) {
        newmax = 16;
        owner->catlistrefs = (CatCList**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(CatCList*));
        owner->maxcatlistrefs = newmax;
    } else {
        newmax = owner->maxcatlistrefs * 2;
        owner->catlistrefs = (CatCList**)repalloc(owner->catlistrefs, newmax * sizeof(CatCList*));
        owner->maxcatlistrefs = newmax;
    }
}

/*
 * Remember that a catcache-list reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeCatCacheListRefs()
 */
void ResourceOwnerRememberCatCacheListRef(ResourceOwner owner, CatCList* list)
{
    Assert(owner->ncatlistrefs < owner->maxcatlistrefs);
    owner->catlistrefs[owner->ncatlistrefs] = list;
    owner->ncatlistrefs++;
}

/*
 * Forget that a catcache-list reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetCatCacheListRef(ResourceOwner owner, CatCList* list)
{
    CatCList** catlistrefs = owner->catlistrefs;
    int nc1 = owner->ncatlistrefs - 1;
    int i;

    for (i = nc1; i >= 0; i--) {
        if (catlistrefs[i] == list) {
            while (i < nc1) {
                catlistrefs[i] = catlistrefs[i + 1];
                i++;
            }
            owner->ncatlistrefs = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("catcache list is not owned by resource owner %s", owner->name)));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * relcache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeRelationRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->nrelrefs < owner->maxrelrefs)
        return; /* nothing to do */

    if (owner->relrefs == NULL) {
        newmax = 16;
        owner->relrefs = (Relation*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(Relation));
        owner->maxrelrefs = newmax;
    } else {
        newmax = owner->maxrelrefs * 2;
        owner->relrefs = (Relation*)repalloc(owner->relrefs, newmax * sizeof(Relation));
        owner->maxrelrefs = newmax;
    }
}

/*
 * Remember that a relcache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeRelationRefs()
 */
void ResourceOwnerRememberRelationRef(ResourceOwner owner, Relation rel)
{
    Assert(owner->nrelrefs < owner->maxrelrefs);
    owner->relrefs[owner->nrelrefs] = rel;
    owner->nrelrefs++;
}

/*
 * Forget that a relcache reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetRelationRef(ResourceOwner owner, Relation rel)
{
    Relation* relrefs = owner->relrefs;
    int nr1 = owner->nrelrefs - 1;
    int i;

    for (i = nr1; i >= 0; i--) {
        if (relrefs[i] == rel) {
            while (i < nr1) {
                relrefs[i] = relrefs[i + 1];
                i++;
            }
            owner->nrelrefs = nr1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg(
                "relcache reference %s is not owned by resource owner %s", RelationGetRelationName(rel), owner->name)));
}

/*
 * Debugging subroutine
 */
static void PrintRelCacheLeakWarning(Relation rel)
{
    ereport(WARNING, (errmsg("relcache reference leak: relation \"%s\" not closed", RelationGetRelationName(rel))));
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Make sure there is room for at least one more entry in
 *			: a ResourceOwner's partcache reference array.
 * Description	: This is separate from actually inserting an entry because
 *			: if we run out of memory, it's critical to do so *before*
 *			: acquiring the resource.
 * Notes		:
 */
void ResourceOwnerEnlargePartitionRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->npartrefs < owner->maxpartrefs)
        return; /* nothing to do */

    if (owner->partrefs == NULL) {
        newmax = 16;
        owner->partrefs = (Partition*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(Partition));
        owner->maxpartrefs = newmax;
    } else {
        newmax = owner->maxpartrefs * 2;
        owner->partrefs = (Partition*)repalloc(owner->partrefs, newmax * sizeof(Partition));
        owner->maxpartrefs = newmax;
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Remember that a partcache reference is owned by a ResourceOwner
 * Description	:
 * Notes		:  Caller must have previously done ResourceOwnerEnlargePartitionRefs()
 */
void ResourceOwnerRememberPartitionRef(ResourceOwner owner, Partition part)
{
    Assert(owner->npartrefs < owner->maxpartrefs);
    owner->partrefs[owner->npartrefs] = part;
    owner->npartrefs++;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Forget that a relcache reference is owned by a ResourceOwner
 * Description	:
 * Notes		:
 */
void ResourceOwnerForgetPartitionRef(ResourceOwner owner, Partition part)
{
    Partition* partrefs = owner->partrefs;
    int nr1 = owner->npartrefs - 1;
    int i;

    for (i = nr1; i >= 0; i--) {
        if (partrefs[i] == part) {
            while (i < nr1) {
                partrefs[i] = partrefs[(i + 1)];
                i++;
            }
            owner->npartrefs = nr1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("partcache reference %s is not owned by resource owner %s",
                PartitionGetPartitionName(part),
                owner->name)));
}

void ResourceOwnerRememberFakerelRef(ResourceOwner owner, Relation fakerel)
{
    dlist_push_tail(&(owner->fakerelrefs_list), &(fakerel->node));
    owner->nfakerelrefs++;
}

void ResourceOwnerForgetFakerelRef(ResourceOwner owner, Relation fakerel)
{    
    if (fakerel->node.next != NULL && fakerel->node.prev != NULL) {
        dlist_delete(&(fakerel->node));
        DListNodeInit(&(fakerel->node));
        owner->nfakerelrefs--;
        return;
    }

    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("fakerel reference %s is not owned by resource owner %s",
                RelationGetRelationName(fakerel),
                owner->name)));
}

void ResourceOwnerEnlargeFakepartRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->nfakepartrefs < owner->maxfakepartrefs)
        return; /* nothing to do */

    if (owner->fakepartrefs == NULL) {
        newmax = 512;
        owner->fakepartrefs = (Partition*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(Partition));
        owner->maxfakepartrefs = newmax;
    } else {
        newmax = owner->maxfakepartrefs * 2;
        owner->fakepartrefs = (Partition*)repalloc(owner->fakepartrefs, newmax * sizeof(Partition));
        owner->maxfakepartrefs = newmax;
    }
}

void ResourceOwnerRememberFakepartRef(ResourceOwner owner, Partition fakepart)
{
    ResourceOwnerEnlargeFakepartRefs(owner);
    Assert(owner->nfakepartrefs < owner->maxfakepartrefs);
    owner->fakepartrefs[owner->nfakepartrefs] = fakepart;
    owner->nfakepartrefs++;
}

void ResourceOwnerForgetFakepartRef(ResourceOwner owner, Partition fakepart)
{
    Partition* fakepartrefs = owner->fakepartrefs;
    int nr1 = owner->nfakepartrefs - 1;
    int i;

    for (i = nr1; i >= 0; i--) {
        if (fakepartrefs[i] == fakepart) {
            while (i < nr1) {
                fakepartrefs[i] = fakepartrefs[(i + 1)];
                i++;
            }
            owner->nfakepartrefs = nr1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("fakepart reference %u is not owned by resource owner %s", fakepart->pd_id, owner->name)));
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Debugging subroutine
 * Description	:
 * Notes		:
 */
static void PrintPartCacheLeakWarning(Partition part)
{
    ereport(
        WARNING, (errmsg("partcache reference leak: partition \"%s\" not closed", PartitionGetPartitionName(part))));
}

static void PrintFakeRelLeakWarning(Relation fakerel)
{
    ereport(WARNING, (errmsg("fakerel reference leak: fakerel \"%s\" not closed", RelationGetRelationName(fakerel))));
}

static void PrintFakePartLeakWarning(Partition fakepart)
{
    ereport(WARNING, (errmsg("fakepart reference leak: fakepart \"%u\" not closed", (fakepart->pd_id))));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * plancache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargePlanCacheRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->nplanrefs < owner->maxplanrefs)
        return; /* nothing to do */

    if (owner->planrefs == NULL) {
        newmax = 16;
        owner->planrefs = (CachedPlan**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(CachedPlan*));
        owner->maxplanrefs = newmax;
    } else {
        newmax = owner->maxplanrefs * 2;
        owner->planrefs = (CachedPlan**)repalloc(owner->planrefs, newmax * sizeof(CachedPlan*));
        owner->maxplanrefs = newmax;
    }
}

/*
 * Remember that a plancache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargePlanCacheRefs()
 */
void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner, CachedPlan* plan)
{
    Assert(owner->nplanrefs < owner->maxplanrefs);
    owner->planrefs[owner->nplanrefs] = plan;
    owner->nplanrefs++;
}

/*
 * Forget that a plancache reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetPlanCacheRef(ResourceOwner owner, CachedPlan* plan)
{
    CachedPlan** planrefs = owner->planrefs;
    int np1 = owner->nplanrefs - 1;
    int i;

    for (i = np1; i >= 0; i--) {
        if (planrefs[i] == plan) {
            while (i < np1) {
                planrefs[i] = planrefs[i + 1];
                i++;
            }
            owner->nplanrefs = np1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("plancache reference is not owned by resource owner %s", owner->name)));
}

/*
 * Debugging subroutine
 */
static void PrintPlanCacheLeakWarning(CachedPlan* plan)
{
    ereport(WARNING, (errmsg("plancache reference leak: plan not closed")));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * tupdesc reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeTupleDescs(ResourceOwner owner)
{
    int newmax;

    if (owner->ntupdescs < owner->maxtupdescs)
        return; /* nothing to do */

    if (owner->tupdescs == NULL) {
        newmax = 16;
        owner->tupdescs = (TupleDesc*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(TupleDesc));
        owner->maxtupdescs = newmax;
    } else {
        newmax = owner->maxtupdescs * 2;
        owner->tupdescs = (TupleDesc*)repalloc(owner->tupdescs, newmax * sizeof(TupleDesc));
        owner->maxtupdescs = newmax;
    }
}

/*
 * Remember that a tupdesc reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeTupleDescs()
 */
void ResourceOwnerRememberTupleDesc(ResourceOwner owner, TupleDesc tupdesc)
{
    Assert(owner->ntupdescs < owner->maxtupdescs);
    owner->tupdescs[owner->ntupdescs] = tupdesc;
    owner->ntupdescs++;
}

/*
 * Forget that a tupdesc reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetTupleDesc(ResourceOwner owner, TupleDesc tupdesc)
{
    TupleDesc* tupdescs = owner->tupdescs;
    int nt1 = owner->ntupdescs - 1;
    int i;

    for (i = nt1; i >= 0; i--) {
        if (tupdescs[i] == tupdesc) {
            while (i < nt1) {
                tupdescs[i] = tupdescs[i + 1];
                i++;
            }
            owner->ntupdescs = nt1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("tupdesc is not owned by resource owner %s", owner->name)));
}

/*
 * Debugging subroutine
 */
static void PrintTupleDescLeakWarning(TupleDesc tupdesc)
{
    ereport(WARNING,
        (errmsg("TupleDesc reference leak: TupleDesc (%u,%d) still referenced",
            tupdesc->tdtypeid,
            tupdesc->tdtypmod)));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * snapshot reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeSnapshots(ResourceOwner owner)
{
    int newmax;

    if (owner->nsnapshots < owner->maxsnapshots)
        return; /* nothing to do */

    if (owner->snapshots == NULL) {
        newmax = 16;
        owner->snapshots = (Snapshot*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(Snapshot));
        owner->maxsnapshots = newmax;
    } else {
        newmax = owner->maxsnapshots * 2;
        owner->snapshots = (Snapshot*)repalloc(owner->snapshots, newmax * sizeof(Snapshot));
        owner->maxsnapshots = newmax;
    }
}

/*
 * Remember that a snapshot reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeSnapshots()
 */
void ResourceOwnerRememberSnapshot(ResourceOwner owner, Snapshot snapshot)
{
    Assert(owner->nsnapshots < owner->maxsnapshots);
    owner->snapshots[owner->nsnapshots] = snapshot;
    owner->nsnapshots++;
}

/*
 * Forget that a snapshot reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snapshot)
{
    Snapshot* snapshots = owner->snapshots;
    int ns1 = owner->nsnapshots - 1;
    int i;

    for (i = ns1; i >= 0; i--) {
        if (snapshots[i] == snapshot) {
            while (i < ns1) {
                snapshots[i] = snapshots[i + 1];
                i++;
            }
            owner->nsnapshots = ns1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("snapshot is not owned by resource owner %s", owner->name)));
}

/*
 * This function is used to clean up the snapshots.
 * It will be called by PreCommit_Portals and AtAbort_Portals.
 */
void ResourceOwnerDecrementNsnapshots(ResourceOwner owner, void *queryDesc)
{
    QueryDesc *queryDesc_temp = (QueryDesc *)queryDesc;
    while(owner->nsnapshots > 0) {
        if(queryDesc_temp) {
            // check if owner's snapshot is same as queryDesc's snapshot, need to set queryDesc
            // snapshot to null, because this function will clean up those snapshots.
            if(owner->snapshots[owner->nsnapshots - 1] == queryDesc_temp->estate->es_snapshot) {
                queryDesc_temp->estate->es_snapshot = NULL;
            }
               
            if(owner->snapshots[owner->nsnapshots - 1] == queryDesc_temp->estate->es_crosscheck_snapshot) {
                queryDesc_temp->estate->es_crosscheck_snapshot = NULL;
            }

            if(owner->snapshots[owner->nsnapshots - 1] == queryDesc_temp->snapshot) {
                queryDesc_temp->snapshot = NULL;
            }

            if(owner->snapshots[owner->nsnapshots - 1] == queryDesc_temp->crosscheck_snapshot) {
                queryDesc_temp->crosscheck_snapshot = NULL;
            }
        }
        UnregisterSnapshotFromOwner(owner->snapshots[owner->nsnapshots - 1], owner);
   }
}

/*
 * This function is used to clean up the cached plan.
 * It will be called by CommitTransaction
 */
void ResourceOwnerDecrementNPlanRefs(ResourceOwner owner, bool useResOwner)
{
    if(!owner) {
        return;
    }
       
    while(owner->nplanrefs > 0) {
       ReleaseCachedPlan(owner->planrefs[owner->nplanrefs - 1], useResOwner);
    }
}

/*
 * Debugging subroutine
 */
static void PrintSnapshotLeakWarning(Snapshot snapshot)
{
    ereport(WARNING, (errmsg("Snapshot reference leak: Snapshot still referenced")));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * files reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargeFiles(ResourceOwner owner)
{
    int newmax;

    if (owner->nfiles < owner->maxfiles)
        return; /* nothing to do */

    if (owner->files == NULL) {
        newmax = 16;
        owner->files = (File*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(File));
        owner->maxfiles = newmax;
    } else {
        newmax = owner->maxfiles * 2;
        owner->files = (File*)repalloc(owner->files, newmax * sizeof(File));
        owner->maxfiles = newmax;
    }
}

/*
 * Remember that a temporary file is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeFiles()
 */
void ResourceOwnerRememberFile(ResourceOwner owner, File file)
{
    Assert(owner->nfiles < owner->maxfiles);
    owner->files[owner->nfiles] = file;
    owner->nfiles++;
}

/*
 * Forget that a temporary file is owned by a ResourceOwner
 */
void ResourceOwnerForgetFile(ResourceOwner owner, File file)
{
    File* files = owner->files;
    int ns1 = owner->nfiles - 1;
    int i;

    for (i = ns1; i >= 0; i--) {
        if (files[i] == file) {
            while (i < ns1) {
                files[i] = files[i + 1];
                i++;
            }
            owner->nfiles = ns1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("temporery file %d is not owned by resource owner %s", file, owner->name)));
}

/*
 * Debugging subroutine
 */
static void PrintFileLeakWarning(File file)
{
    ereport(WARNING, (errmsg("temporary file leak: File %d still referenced", file)));
}

//
// Make sure there is room for at least one more entry in a ResourceOwner's
// pthread mutex array.
// This is separate from actually inserting an entry because if we run out
// of memory, it's critical to do so *before* acquiring the resource.
//
void ResourceOwnerEnlargePthreadMutex(ResourceOwner owner)
{
    int newmax;

    if (owner->nPthreadMutex < owner->maxPThdMutexs)
        return; /* nothing to do */

    if (owner->pThdMutexs == NULL) {
        newmax = 16;
        owner->pThdMutexs = (pthread_mutex_t**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(pthread_mutex_t*));
        owner->maxPThdMutexs = newmax;
    } else {
        newmax = owner->maxPThdMutexs * 2;
        owner->pThdMutexs = (pthread_mutex_t**)repalloc(owner->pThdMutexs, newmax * sizeof(pthread_mutex_t*));
        owner->maxPThdMutexs = newmax;
    }
}

// ResourceOwnerRememberPthreadMutex
// Remember that a pthread mutex is owned by a ResourceOwner
// Caller must have previously done ResourceOwnerEnlargePthreadMutex()
//
void ResourceOwnerRememberPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex)
{
    Assert(owner->nPthreadMutex < owner->maxPThdMutexs);
    owner->pThdMutexs[owner->nPthreadMutex] = pMutex;
    owner->nPthreadMutex++;
}

// ResourceOwnerForgetPthreadMutex
// Forget that a pthread mutex is owned by a ResourceOwner
//
void ResourceOwnerForgetPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex)
{
    pthread_mutex_t** mutexs = owner->pThdMutexs;
    int ns1 = owner->nPthreadMutex - 1;
    int i;

    for (i = ns1; i >= 0; i--) {
        if (mutexs[i] == pMutex) {
            while (i < ns1) {
                mutexs[i] = mutexs[i + 1];
                i++;
            }
            owner->nPthreadMutex = ns1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("pthread mutex is not owned by resource owner %s", owner->name)));
}

/*
 * Debugging subroutine
 */
void PrintPthreadMutexLeakWarning(pthread_mutex_t* pMutex)
{
    ereport(WARNING, (errmsg("pthread mutex leak: pthread mutex still been locked")));
}

/*
 *  * Debugging subroutine
 *   */
void PrintResourceOwnerLeakWarning()
{
    if (IsolatedResourceOwner != NULL)
        ereport(WARNING, (errmsg("resource owner \"%s\" may leak", IsolatedResourceOwner->name)));
}

void ResourceOwnerReleasePthreadMutex()
{
    ResourceOwner owner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    ResourceOwner child;

    if (owner) {
        for (child = owner->firstchild; child != NULL; child = child->nextchild) {
            // Ditto for pthread mutex
            //
            while (child->nPthreadMutex > 0)
                PthreadMutexUnlock(child, child->pThdMutexs[child->nPthreadMutex - 1]);
        }

        while (owner->nPthreadMutex > 0)
            PthreadMutexUnlock(owner, owner->pThdMutexs[owner->nPthreadMutex - 1]);
    }
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * partition map reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargePartitionMapRefs(ResourceOwner owner)
{
    int newmax;

    if (owner->npartmaprefs < owner->maxpartmaprefs)
        return; /* nothing to do */

    if (owner->partmaprefs == NULL) {
        newmax = 16;
        owner->partmaprefs = (PartitionMap**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newmax * sizeof(PartitionMap*));
        owner->maxpartmaprefs = newmax;
    } else {
        newmax = owner->maxpartmaprefs * 2;
        owner->partmaprefs = (PartitionMap**)repalloc(owner->partmaprefs, newmax * sizeof(PartitionMap*));
        owner->maxpartmaprefs = newmax;
    }
}

/*
 * Remember that a partition map reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargePartitionMapRefs()
 */
void ResourceOwnerRememberPartitionMapRef(ResourceOwner owner, PartitionMap* partmap)
{
    Assert(owner->npartmaprefs < owner->maxpartmaprefs);
    owner->partmaprefs[owner->npartmaprefs] = partmap;
    owner->npartmaprefs++;
}

/*
 * Forget that a partition map reference is owned by a ResourceOwner
 */
void ResourceOwnerForgetPartitionMapRef(ResourceOwner owner, PartitionMap* partmap)
{
    PartitionMap** partmaprefs = owner->partmaprefs;
    int nr1 = owner->npartmaprefs - 1;
    int i;

    for (i = nr1; i >= 0; i--) {
        if (partmaprefs[i] == partmap) {
            while (i < nr1) {
                partmaprefs[i] = partmaprefs[i + 1];
                i++;
            }
            owner->npartmaprefs = nr1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("partition map reference is not owned by resource owner %s", owner->name)));
}

void ResourceOwnerRememberGMemContext(ResourceOwner owner, MemoryContext memcontext)
{
    Assert(owner->nglobalMemContext < owner->maxGlobalMemContexts);
    owner->globalMemContexts[owner->nglobalMemContext] = memcontext;
    owner->nglobalMemContext++;
}

void ResourceOwnerForgetGMemContext(ResourceOwner owner, MemoryContext memcontext)
{
    MemoryContext* gMemContexts = owner->globalMemContexts;
    int num = owner->nglobalMemContext - 1;

    for (int i = num; i >= 0; i--) {
        if (memcontext == gMemContexts[i]) {
            int j = i;
            while (j < num) {
                gMemContexts[j] = gMemContexts[j + 1];
                j++;
            }
            owner->nglobalMemContext = num;
            return;
        }
    }
    return;
}

void ResourceOwnerEnlargeGMemContext(ResourceOwner owner)
{
    int newmax;

    if (owner->nglobalMemContext < owner->maxGlobalMemContexts)
        return; /* nothing to do */

    if (owner->globalMemContexts == NULL) {
        newmax = 2;
        owner->globalMemContexts = (MemoryContext*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), newmax * sizeof(MemoryContext));
    } else {
        newmax = owner->maxGlobalMemContexts * 2;
        owner->globalMemContexts = (MemoryContext*)repalloc(owner->globalMemContexts, newmax * sizeof(MemoryContext));
    }    
    owner->maxGlobalMemContexts = newmax;
}

void PrintGMemContextLeakWarning(MemoryContext memcontext)
{   
    char *name = memcontext->name;
    ereport(WARNING, (errmsg("global memory context: %s leak", name)));
}
