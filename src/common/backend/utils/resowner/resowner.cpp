/*-------------------------------------------------------------------------
 *
 * resowner.c
 *	  openGauss resource owner management code.
 *
 * Query-lifespan resources are tracked by associating them with
 * ResourceOwner objects.  This provides a simple mechanism for ensuring
 * that such resources are freed at the right time.
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
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
#include "access/ustore/knl_uundovec.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/smgr/segment.h"
#include "utils/knl_partcache.h"
#include "utils/knl_relcache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "storage/cucache_mgr.h"
#include "executor/executor.h"
#include "catalog/pg_hashbucket_fn.h"

/*
 * ResourceOwner objects look like this. When tracking new types of resource,
 * you must at least add the 'Remember' interface for that resource and adapt
 * the 'ResourceOwnerConcat' function.
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


    int nlocalcatclist;
    LocalCatCList** localcatclists;
    int maxlocalcatclists;

    int nlocalcatctup;
    LocalCatCTup** localcatctups;
    int maxlocalcatctups;

    int nglobalcatctup;
    GlobalCatCTup** globalcatctups;
    int maxglobalcatctups;

    int nglobalcatclist;
    GlobalCatCList** globalcatclists;
    int maxglobalcatclist;

    int nglobalbaseentry;
    GlobalBaseEntry** globalbaseentries;
    int maxglobalbaseentry;

    int nglobaldbentry;
    GlobalSysDBCacheEntry** globaldbentries;
    int maxglobaldbentry;

    int nglobalisexclusive;
    volatile uint32** globalisexclusives;
    int maxglobalisexclusive;

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

    /* We have built-in support for remembering pthread_rwlock */
    int nPthreadRWlock;
    pthread_rwlock_t** pThdRWlocks;
    int maxPThdRWlocks;

    /* We have built-in support for remembering partition map references */
    int npartmaprefs;
    PartitionMap** partmaprefs;
    int maxpartmaprefs;

    /* track global memory context */
    int nglobalMemContext;
    MemoryContext* globalMemContexts;
    int maxGlobalMemContexts;

    MemoryContext memCxt;

    /* whether this is a complete one. FALSE is setted while its transaction finishes. */
    bool valid;
} ResourceOwnerData;

THR_LOCAL ResourceOwner IsolatedResourceOwner = NULL;
#ifdef MEMORY_CONTEXT_CHECKING
#define PrintGlobalSysCacheLeakWarning(owner, strinfo) \
do { \
    if (EnableLocalSysCache()) { \
        ereport(WARNING, (errmsg("global syscache reference leak %s %s %d", strinfo, __FILE__, __LINE__))); \
    } \
} while(0)
#else
#define PrintGlobalSysCacheLeakWarning(owner, strinfo)
#endif

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
ResourceOwner ResourceOwnerCreate(ResourceOwner parent, const char* name, MemoryContext memCxt)
{
    ResourceOwner owner;

    MemoryContext context = AllocSetContextCreate(memCxt,
                                                  "ResourceOwnerCxt",
                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
    owner = (ResourceOwner)MemoryContextAllocZero(context, sizeof(ResourceOwnerData));
    owner->name = name;
    owner->memCxt = context;
    owner->valid = true;
    if (parent) {
        owner->parent = parent;
        owner->nextchild = parent->firstchild;
        parent->firstchild = owner;
    }

    if (parent == NULL && strcmp(name, "TopTransaction") != 0 && strcmp(name, "InitLocalSysCache") != 0)
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

    /* Recurse to handle descendants */
    for (child = owner->firstchild; child != NULL; child = child->nextchild) {
        ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);
    }

    /*
     * Make CurrentResourceOwner point to me, so that ReleaseBuffer etc don't
     * get confused.  We needn't PG_TRY here because the outermost level will
     * fix it on error abort.
     */
    save = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = owner;

    if (phase == RESOURCE_RELEASE_BEFORE_LOCKS) {
        if (owner == t_thrd.utils_cxt.TopTransactionResourceOwner) {
            if (t_thrd.ustore_cxt.urecvec) {
                t_thrd.ustore_cxt.urecvec->Reset(false);
            }
            ReleaseUndoBuffers();
            undo::ReleaseSlotBuffer();
        }

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
        ResourceOwnerReleaseRelationRef(owner, isCommit);
        ResourceOwnerReleasePartitionRef(owner, isCommit);
        // Ditto for pthread mutex
        //
        while (owner->nPthreadMutex > 0) {
            if (isCommit)
                PrintPthreadMutexLeakWarning(owner->pThdMutexs[owner->nPthreadMutex - 1]);
            PthreadMutexUnlock(owner, owner->pThdMutexs[owner->nPthreadMutex - 1]);
        }
        ResourceOwnerReleaseRWLock(owner, isCommit);
        ResourceOwnerReleaseGlobalCatCList(owner, isCommit);
        ResourceOwnerReleaseGlobalCatCTup(owner, isCommit);
        ResourceOwnerReleaseGlobalBaseEntry(owner, isCommit);
        ResourceOwnerReleaseGlobalDBEntry(owner, isCommit);
        ResourceOwnerReleaseGlobalIsExclusive(owner, isCommit);
    } else if (phase == RESOURCE_RELEASE_LOCKS) {
        if (isTopLevel) {
            /*
             * For a top-level xact we are going to release all locks (or at
             * least all non-session locks), so just do a single lmgr call at
             * the top of the recursion.
             */
            if (owner == t_thrd.utils_cxt.TopTransactionResourceOwner) {
                if (!CanPerformUndoActions())
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
            else if (!CanPerformUndoActions())
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
            Assert(!EnableLocalSysCache());
            if (isCommit)
                PrintCatCacheLeakWarning(owner->catrefs[owner->ncatrefs - 1]);
            ReleaseCatCache(owner->catrefs[owner->ncatrefs - 1]);
        }
        ResourceOwnerReleaseLocalCatCList(owner, isCommit);
        ResourceOwnerReleaseLocalCatCTup(owner, isCommit);
        /* Ditto for catcache lists */
        while (owner->ncatlistrefs > 0) {
            Assert(!EnableLocalSysCache());
            if (isCommit)
                PrintCatCacheListLeakWarning(owner->catlistrefs[owner->ncatlistrefs - 1]);
            ReleaseSysCacheList(owner->catlistrefs[owner->ncatlistrefs - 1]);
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
    }

    t_thrd.utils_cxt.CurrentResourceOwner = save;
}

static void ResourceOwnerFreeOwner(ResourceOwner owner, bool whole)
{
    if (owner->valid) {
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
        pfree_ext(owner->localcatclists);
        pfree_ext(owner->localcatctups);
        pfree_ext(owner->globalcatctups);
        pfree_ext(owner->globalcatclists);
        pfree_ext(owner->globalbaseentries);
        pfree_ext(owner->globaldbentries);
        pfree_ext(owner->globalisexclusives);
        pfree_ext(owner->pThdRWlocks);
    }
    if (whole && owner->memCxt)
        MemoryContextDelete(owner->memCxt);
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
    Assert(CurrentResourceOwnerIsEmpty(owner));

    /*
     * Delete children.  The recursive call will delink the child from me, so
     * just iterate as long as there is a child.
     */
    if (IsolatedResourceOwner == owner)
        IsolatedResourceOwner = NULL;
    Assert(t_thrd.lsc_cxt.local_sysdb_resowner != owner);

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
    ResourceOwnerFreeOwner(owner, true);
}

/*
 * Part 1 of 'ResourceOwnerConcat'. Concatenate the top 12 resources of two owners.
 */
static void ResourceOwnerConcatPart1(ResourceOwner target, ResourceOwner source)
{
    while (source->nbuffers > 0) {
        ResourceOwnerEnlargeBuffers(target);
        ResourceOwnerRememberBuffer(target, source->buffers[--source->nbuffers]);
    }

    while (source->nlocalcatclist > 0) {
        ResourceOwnerEnlargeLocalCatCList(target);
        ResourceOwnerRememberLocalCatCList(target, source->localcatclists[--source->nlocalcatclist]);
    }

    while (source->nlocalcatctup > 0) {
        ResourceOwnerEnlargeLocalCatCTup(target);
        ResourceOwnerRememberLocalCatCTup(target, source->localcatctups[--source->nlocalcatctup]);
    }

    while (source->nglobalcatctup > 0) {
        ResourceOwnerEnlargeGlobalCatCTup(target);
        ResourceOwnerRememberGlobalCatCTup(target, source->globalcatctups[--source->nglobalcatctup]);
    }

    while (source->nglobalcatclist > 0) {
        ResourceOwnerEnlargeGlobalCatCList(target);
        ResourceOwnerRememberGlobalCatCList(target, source->globalcatclists[--source->nglobalcatclist]);
    }

    while (source->nglobalbaseentry > 0) {
        ResourceOwnerEnlargeGlobalBaseEntry(target);
        ResourceOwnerRememberGlobalBaseEntry(target, source->globalbaseentries[--source->nglobalbaseentry]);
    }

    while (source->nglobaldbentry > 0) {
        ResourceOwnerEnlargeGlobalDBEntry(target);
        ResourceOwnerRememberGlobalDBEntry(target, source->globaldbentries[--source->nglobaldbentry]);
    }

    while (source->nglobalisexclusive > 0) {
        ResourceOwnerEnlargeGlobalIsExclusive(target);
        ResourceOwnerRememberGlobalIsExclusive(target, source->globalisexclusives[--source->nglobalisexclusive]);
    }

    while (source->ncatrefs > 0) {
        ResourceOwnerEnlargeCatCacheRefs(target);
        ResourceOwnerRememberCatCacheRef(target, source->catrefs[--source->ncatrefs]);
    }

    while (source->ncatlistrefs > 0) {
        ResourceOwnerEnlargeCatCacheListRefs(target);
        ResourceOwnerRememberCatCacheListRef(target, source->catlistrefs[--source->ncatlistrefs]);
    }

    while (source->nrelrefs > 0) {
        ResourceOwnerEnlargeRelationRefs(target);
        ResourceOwnerRememberRelationRef(target, source->relrefs[--source->nrelrefs]);
    }

    while (source->npartrefs > 0) {
        ResourceOwnerEnlargePartitionRefs(target);
        ResourceOwnerRememberPartitionRef(target, source->partrefs[--source->npartrefs]);
    }
}

/*
 * Part 2 of 'ResourceOwnerConcat'. Concatenate the remaining resources of two owners.
 */
static void ResourceOwnerConcatPart2(ResourceOwner target, ResourceOwner source)
{
    while (source->nfakerelrefs > 0) {
        dlist_push_tail(&(target->fakerelrefs_list), dlist_pop_head_node(&(source->fakerelrefs_list)));
        target->nfakerelrefs++;
        source->nfakerelrefs--;
    }

    while (source->nfakepartrefs > 0) {
        ResourceOwnerEnlargeFakepartRefs(target);
        ResourceOwnerRememberFakepartRef(target, source->fakepartrefs[--source->nfakepartrefs]);
    }

    while (source->nplanrefs > 0) {
        ResourceOwnerEnlargePlanCacheRefs(target);
        ResourceOwnerRememberPlanCacheRef(target, source->planrefs[--source->nplanrefs]);
    }

    while (source->ntupdescs > 0) {
        ResourceOwnerEnlargeTupleDescs(target);
        ResourceOwnerRememberTupleDesc(target, source->tupdescs[--source->ntupdescs]);
    }

    while (source->nsnapshots > 0) {
        ResourceOwnerEnlargeSnapshots(target);
        ResourceOwnerRememberSnapshot(target, source->snapshots[--source->nsnapshots]);
    }

    while (source->nfiles > 0) {
        ResourceOwnerEnlargeFiles(target);
        ResourceOwnerRememberFile(target, source->files[--source->nfiles]);
    }

    while (source->nDataCacheSlots > 0) {
        ResourceOwnerEnlargeDataCacheSlot(target);
        ResourceOwnerRememberDataCacheSlot(target, source->dataCacheSlots[--source->nDataCacheSlots]);
    }

    while (source->nMetaCacheSlots > 0) {
        ResourceOwnerEnlargeMetaCacheSlot(target);
        ResourceOwnerRememberMetaCacheSlot(target, source->metaCacheSlots[--source->nMetaCacheSlots]);
    }

    while (source->nPthreadMutex > 0) {
        ResourceOwnerEnlargePthreadMutex(target);
        ResourceOwnerRememberPthreadMutex(target, source->pThdMutexs[--source->nPthreadMutex]);
    }

    while (source->nPthreadRWlock > 0) {
        ResourceOwnerEnlargePthreadRWlock(target);
        ResourceOwnerRememberPthreadRWlock(target, source->pThdRWlocks[--source->nPthreadRWlock]);
    }

    while (source->npartmaprefs > 0) {
        ResourceOwnerEnlargePartitionMapRefs(target);
        ResourceOwnerRememberPartitionMapRef(target, source->partmaprefs[--source->npartmaprefs]);
    }

    while (source->nglobalMemContext > 0) {
        ResourceOwnerEnlargeGMemContext(target);
        ResourceOwnerRememberGMemContext(target, source->globalMemContexts[--source->nglobalMemContext]);
    }
}

/* ResourceOwnerConcat
 *              Concatenate two owners.
 *
 * The resources traced by the 'source' are placed in the 'target' for tracing.
 * The advantage is that the memory occupied by the 'source' owner can be released
 * to reduce the memory consumed by tracing resources. When using a stream-plan,
 * this is useful for preventing "memory is temporarily unavailable" error when
 * executing a large number of SQLs in a single transaction/procedure.
 *
 * Note: After the invoking is complete, the memory of the 'source' should be release.
 */
void ResourceOwnerConcat(ResourceOwner target, ResourceOwner source)
{
    ResourceOwner child;

    Assert(target && source);
    /*
     * When modifying the structure of ResourceOwnerData, note that the ResourceOwnerConcat
     * function needs to be adapted when tracing new types of resources.
     */
    Assert(sizeof(ResourceOwnerData) == 448); /* The current size of ResourceOwnerData is 448 */

    /* Recurse to handle descendants */
    for (child = source->firstchild; child != NULL; child = child->nextchild) {
        ResourceOwnerConcat(target, child);
    }

    /*
     * ResourceOwner traces too many resources. To reduce cyclomatic complexity,
     * the Concatenate operation is divided into two parts.
     */
    ResourceOwnerConcatPart1(target, source);
    ResourceOwnerConcatPart2(target, source);
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
 * Fetch memory context of a ResourceOwner
 */
MemoryContext ResourceOwnerGetMemCxt(ResourceOwner owner)
{
    return owner->memCxt;
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
        owner->buffers = (Buffer*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(Buffer));
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
        owner->dataCacheSlots = (CacheSlotId_t*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(CacheSlotId_t));
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
            owner->memCxt, newmax * sizeof(CacheSlotId_t));
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
    if (owner != NULL && owner->valid) {
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
    if (owner != NULL && owner->valid) {
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
    if (owner != NULL && owner->valid) {
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
    if (owner != NULL && owner->valid) {
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
        owner->catrefs = (HeapTuple*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(HeapTuple));
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

    if (!owner->valid)
        return;

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
        owner->catlistrefs = (CatCList**)MemoryContextAlloc(owner->memCxt, newmax * sizeof(CatCList*));
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

    if (!owner->valid)
        return;

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
        owner->relrefs = (Relation*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(Relation));
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

    if (!owner->valid)
        return;

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
        owner->partrefs = (Partition*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(Partition));
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

    if (!owner->valid)
        return;

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
    if (!owner->valid)
        return;

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
        owner->fakepartrefs = (Partition*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(Partition));
        owner->maxfakepartrefs = newmax;
    } else {
        newmax = owner->maxfakepartrefs * 2;
        owner->fakepartrefs = (Partition*)repalloc(owner->fakepartrefs, newmax * sizeof(Partition));
        owner->maxfakepartrefs = newmax;
    }
}

void ResourceOwnerRememberFakepartRef(ResourceOwner owner, Partition fakepart)
{
    Assert(owner->nfakepartrefs < owner->maxfakepartrefs);
    owner->fakepartrefs[owner->nfakepartrefs] = fakepart;
    owner->nfakepartrefs++;
}

void ResourceOwnerForgetFakepartRef(ResourceOwner owner, Partition fakepart)
{
    Partition* fakepartrefs = owner->fakepartrefs;
    int nr1 = owner->nfakepartrefs - 1;
    int i;

    if (!owner->valid)
        return;

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
        owner->planrefs = (CachedPlan**)MemoryContextAlloc(owner->memCxt, newmax * sizeof(CachedPlan*));
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

    if (!owner->valid) {
        return;
    }

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
    int elevel = t_thrd.proc_cxt.proc_exit_inprogress ? WARNING : ERROR;
    ereport(elevel,
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
        owner->tupdescs = (TupleDesc*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(TupleDesc));
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
bool ResourceOwnerForgetTupleDesc(ResourceOwner owner, TupleDesc tupdesc)
{
    TupleDesc* tupdescs = owner->tupdescs;
    int nt1 = owner->ntupdescs - 1;
    int i;

    if (!owner->valid)
        return false;

    for (i = nt1; i >= 0; i--) {
        if (tupdescs[i] == tupdesc) {
            while (i < nt1) {
                tupdescs[i] = tupdescs[i + 1];
                i++;
            }
            owner->ntupdescs = nt1;
            return true;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("tupdesc is not owned by resource owner %s", owner->name)));

    return false;
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
        owner->snapshots = (Snapshot*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(Snapshot));
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
bool ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snapshot, bool ereport)
{
    Snapshot* snapshots = owner->snapshots;
    int ns1 = owner->nsnapshots - 1;
    int i;

    if (!owner->valid)
        return false;

    for (i = ns1; i >= 0; i--) {
        if (snapshots[i] == snapshot) {
            while (i < ns1) {
                snapshots[i] = snapshots[i + 1];
                i++;
            }
            owner->nsnapshots = ns1;
            return true;
        }
    }

    for (ResourceOwner child = owner->firstchild; child != NULL; child = child->nextchild) {
        if (ResourceOwnerForgetSnapshot(child, snapshot, false)) {
            return true;
        }
    }

    if (ereport && u_sess->plsql_cxt.spi_xact_context != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
                errmsg("snapshot is not owned by resource owner %s", owner->name)));
    }

    return false;
}

/*
 * This function is used to clean up the snapshots.
 * It will be called by PreCommit_Portals and AtAbort_Portals.
 */
void ResourceOwnerDecrementNsnapshots(ResourceOwner owner, void *queryDesc)
{
    QueryDesc *queryDesc_temp = (QueryDesc *)queryDesc;
    while(owner->valid && owner->nsnapshots > 0) {
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
    if(!owner || !owner->valid) {
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
        owner->files = (File*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(File));
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

    if (!owner->valid)
        return;

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
        owner->pThdMutexs = (pthread_mutex_t**)MemoryContextAlloc(owner->memCxt, newmax * sizeof(pthread_mutex_t*));
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

    if (!owner->valid)
        return;

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

void ResourceOwnerReleaseAllXactPthreadMutex()
{
    ResourceOwner owner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    ResourceOwner child;

    if (owner && owner->valid) {
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
        owner->partmaprefs = (PartitionMap**)MemoryContextAlloc(owner->memCxt, newmax * sizeof(PartitionMap*));
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

    if (!owner->valid)
        return;

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

    if (!owner->valid)
        return;

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
        owner->globalMemContexts = (MemoryContext*)MemoryContextAlloc(owner->memCxt, newmax * sizeof(MemoryContext));
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

/*
 * Mark this ResourceOwner and its children as incomplete ones.
 */
void ResourceOwnerMarkInvalid(ResourceOwner owner)
{
    if (owner->valid) {
        for (ResourceOwner child = owner->firstchild; child != NULL; child = child->nextchild) {
            ResourceOwnerMarkInvalid(child);
        }

        ResourceOwnerFreeOwner(owner, false);
        owner->valid = false;
    }
}

/*
 * whether this ResourceOwner is a complete one.
 *
 * NOTE: it doesn't take child ResourceOwner into consideration.
 */
bool ResourceOwnerIsValid(ResourceOwner owner)
{
    return owner->valid;
}

/* Make sure there is room for at least one more entry in a ResourceOwner's
 * pthread rwlock array.
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void ResourceOwnerEnlargePthreadRWlock(ResourceOwner owner)
{
    int newmax;
    if (owner->nPthreadRWlock < owner->maxPThdRWlocks)
        return; /* nothing to do */
    if (owner->pThdRWlocks == NULL) {
        newmax = 16;
        owner->pThdRWlocks = (pthread_rwlock_t**)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(pthread_rwlock_t*));
        owner->maxPThdRWlocks = newmax;
    } else {
        newmax = owner->maxPThdRWlocks * 2;
        owner->pThdRWlocks = (pthread_rwlock_t**)repalloc(owner->pThdRWlocks, newmax * sizeof(pthread_rwlock_t*));
        owner->maxPThdRWlocks = newmax;
    }
}

/* ResourceOwnerRememberPthreadRWlock
 * Remember that a pthread rwlock is owned by a ResourceOwner
 */
void ResourceOwnerRememberPthreadRWlock(ResourceOwner owner, pthread_rwlock_t* pRWlock)
{
    Assert(owner->nPthreadRWlock < owner->maxPThdRWlocks);
    owner->pThdRWlocks[owner->nPthreadRWlock] = pRWlock;
    owner->nPthreadRWlock++;
}
/* ResourceOwnerForgetPthreadRWlock
 * Forget that a pthread mutex is owned by a ResourceOwner
 */
void ResourceOwnerForgetPthreadRWlock(ResourceOwner owner, pthread_rwlock_t* pRWlock)
{
    pthread_rwlock_t** rwlocks = owner->pThdRWlocks;
    int ns1 = owner->nPthreadRWlock - 1;
    int i;
    for (i = ns1; i >= 0; i--) {
        if (rwlocks[i] == pRWlock) {
            while (i < ns1) {
                rwlocks[i] = rwlocks[i + 1];
                i++;
            }
            owner->nPthreadRWlock = ns1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("pthread rwlock is not owned by resource owner %s", owner->name)));
}

int ResourceOwnerForgetIfExistPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex, bool trace)
{
    pthread_mutex_t** mutexs = owner->pThdMutexs;
    int ns1 = owner->nPthreadMutex - 1;
 
    if (!owner->valid) {
        return 0;
    }
 
    for (int i = ns1; i >= 0; i--) {
        if (mutexs[i] == pMutex) {
            return PthreadMutexUnlock(owner, pMutex, trace);
        }
    }
    return 0;
}

void ResourceOwnerEnlargeLocalCatCList(ResourceOwner owner)
{
    int newmax;
    if (owner->nlocalcatclist < owner->maxlocalcatclists)
        return; /* nothing to do */
    if (owner->localcatclists == NULL) {
        newmax = 16;
        owner->localcatclists = (LocalCatCList**)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(LocalCatCList*));
        owner->maxlocalcatclists = newmax;
    } else {
        newmax = owner->maxlocalcatclists * 2;
        owner->localcatclists = (LocalCatCList**)repalloc(owner->localcatclists, newmax * sizeof(LocalCatCList*));
        owner->maxlocalcatclists = newmax;
    }
}

void ResourceOwnerRememberLocalCatCList(ResourceOwner owner, LocalCatCList* list)
{
    Assert(owner->nlocalcatclist < owner->maxlocalcatclists);
    owner->localcatclists[owner->nlocalcatclist] = list;
    owner->nlocalcatclist++;
}
void ResourceOwnerForgetLocalCatCList(ResourceOwner owner, LocalCatCList* list)
{
    LocalCatCList** localcatclist = owner->localcatclists;
    int nc1 = owner->nlocalcatclist - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (localcatclist[i] == list) {
            while (i < nc1) {
                localcatclist[i] = localcatclist[i + 1];
                i++;
            }
            owner->nlocalcatclist = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("localcatcache list is not owned by resource owner %s", owner->name)));
}
void ResourceOwnerEnlargeLocalCatCTup(ResourceOwner owner)
{
    int newmax;
    if (owner->nlocalcatctup < owner->maxlocalcatctups)
        return; /* nothing to do */
    if (owner->localcatctups == NULL) {
        newmax = 16;
        owner->localcatctups = (LocalCatCTup**)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(LocalCatCTup*));
        owner->maxlocalcatctups = newmax;
    } else {
        newmax = owner->maxlocalcatctups * 2;
        owner->localcatctups = (LocalCatCTup**)repalloc(owner->localcatctups, newmax * sizeof(LocalCatCTup*));
        owner->maxlocalcatctups = newmax;
    }
}
void ResourceOwnerRememberLocalCatCTup(ResourceOwner owner, LocalCatCTup* tup)
{
    Assert(owner->nlocalcatctup < owner->maxlocalcatctups);
    owner->localcatctups[owner->nlocalcatctup] = tup;
    owner->nlocalcatctup++;
}
LocalCatCTup* ResourceOwnerForgetLocalCatCTup(ResourceOwner owner, HeapTuple tup)
{
    LocalCatCTup** localcatctup = owner->localcatctups;
    LocalCatCTup* find = NULL;
    int nc1 = owner->nlocalcatctup - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (&localcatctup[i]->global_ct->tuple == tup) {
            find = localcatctup[i];
            while (i < nc1) {
                localcatctup[i] = localcatctup[i + 1];
                i++;
            }
            owner->nlocalcatctup = nc1;
            return find;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("localcatcache tuple is not owned by resource owner %s", owner->name)));
    return NULL; /* keep compiler quiet */
}

void ResourceOwnerEnlargeGlobalCatCTup(ResourceOwner owner)
{
    int newmax;
    if (owner->nglobalcatctup < owner->maxglobalcatctups)
        return; /* nothing to do */
    if (owner->globalcatctups == NULL) {
        newmax = 16;
        owner->globalcatctups = (GlobalCatCTup**)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(GlobalCatCTup*));
        owner->maxglobalcatctups = newmax;
    } else {
        newmax = owner->maxglobalcatctups * 2;
        owner->globalcatctups = (GlobalCatCTup**)repalloc(owner->globalcatctups, newmax * sizeof(GlobalCatCTup*));
        owner->maxglobalcatctups = newmax;
    }
}
void ResourceOwnerRememberGlobalCatCTup(ResourceOwner owner, GlobalCatCTup* tup)
{
    Assert(owner->nglobalcatctup < owner->maxglobalcatctups);
    owner->globalcatctups[owner->nglobalcatctup] = tup;
    owner->nglobalcatctup++;
}
void ResourceOwnerForgetGlobalCatCTup(ResourceOwner owner, GlobalCatCTup* tup)
{
    GlobalCatCTup** global_cts = owner->globalcatctups;
    int nc1 = owner->nglobalcatctup - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (global_cts[i] == tup) {
            while (i < nc1) {
                global_cts[i] = global_cts[i + 1];
                i++;
            }
            owner->nglobalcatctup = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("bad global tuple is not owned by resource owner %s", owner->name)));
}
void ResourceOwnerEnlargeGlobalCatCList(ResourceOwner owner)
{
    int newmax;
    if (owner->nglobalcatclist < owner->maxglobalcatclist)
        return; /* nothing to do */
    if (owner->globalcatclists == NULL) {
        newmax = 16;
        owner->globalcatclists = (GlobalCatCList**)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(GlobalCatCList*));
        owner->maxglobalcatclist = newmax;
    } else {
        newmax = owner->maxglobalcatclist * 2;
        owner->globalcatclists = (GlobalCatCList**)repalloc(owner->globalcatclists, newmax * sizeof(GlobalCatCList*));
        owner->maxglobalcatclist = newmax;
    }
}
void ResourceOwnerRememberGlobalCatCList(ResourceOwner owner, GlobalCatCList* list)
{
    Assert(owner->nglobalcatclist < owner->maxglobalcatclist);
    owner->globalcatclists[owner->nglobalcatclist] = list;
    owner->nglobalcatclist++;
}
void ResourceOwnerForgetGlobalCatCList(ResourceOwner owner, GlobalCatCList* list)
{
    GlobalCatCList** global_lists = owner->globalcatclists;
    int nc1 = owner->nglobalcatclist - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (global_lists[i] == list) {
            while (i < nc1) {
                global_lists[i] = global_lists[i + 1];
                i++;
            }
            owner->nglobalcatclist = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("bad global list is not owned by resource owner %s", owner->name)));
}
void ResourceOwnerEnlargeGlobalBaseEntry(ResourceOwner owner)
{
    int newmax;
    if (owner->nglobalbaseentry < owner->maxglobalbaseentry)
        return; /* nothing to do */
    if (owner->globalbaseentries == NULL) {
        newmax = 16;
        owner->globalbaseentries = (GlobalBaseEntry **)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(GlobalBaseEntry *));
        owner->maxglobalbaseentry = newmax;
    } else {
        newmax = owner->maxglobalbaseentry * 2;
        owner->globalbaseentries =
            (GlobalBaseEntry **)repalloc(owner->globalbaseentries, newmax * sizeof(GlobalBaseEntry *));
        owner->maxglobalbaseentry = newmax;
    }
}
void ResourceOwnerRememberGlobalBaseEntry(ResourceOwner owner, GlobalBaseEntry* entry)
{
    Assert(owner->nglobalbaseentry < owner->maxglobalbaseentry);
    owner->globalbaseentries[owner->nglobalbaseentry] = entry;
    owner->nglobalbaseentry++;
}
void ResourceOwnerForgetGlobalBaseEntry(ResourceOwner owner, GlobalBaseEntry* entry)
{
    GlobalBaseEntry** global_entries = owner->globalbaseentries;
    int nc1 = owner->nglobalbaseentry - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (global_entries[i] == entry) {
            while (i < nc1) {
                global_entries[i] = global_entries[i + 1];
                i++;
            }
            owner->nglobalbaseentry = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("the global base entry is not owned by resource owner %s", owner->name)));
}

void ResourceOwnerReleasePthreadMutex(ResourceOwner owner, bool isCommit)
{
    while (owner->nPthreadMutex > 0) {
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "MutexLock");
        }
        /* unlock do -- */
        PthreadMutexUnlock(owner, owner->pThdMutexs[owner->nPthreadMutex - 1]);
    }
}

void ResourceOwnerReleaseRWLock(ResourceOwner owner, bool isCommit)
{
    while (owner->nPthreadRWlock > 0) {
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "RWLock");
        }
        /* unlock do -- */
        PthreadRWlockUnlock(owner, owner->pThdRWlocks[owner->nPthreadRWlock - 1]);
    }
}

void ResourceOwnerReleaseLocalCatCTup(ResourceOwner owner, bool isCommit)
{
    while (owner->nlocalcatctup > 0) {
        LocalCatCTup *ct = owner->localcatctups[owner->nlocalcatctup - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "LocalCatCTup");
        }
        ct->Release();
        owner->nlocalcatctup--;
    }
}

void ResourceOwnerReleaseLocalCatCList(ResourceOwner owner, bool isCommit)
{
    while (owner->nlocalcatclist > 0) {
        LocalCatCList *cl = owner->localcatclists[owner->nlocalcatclist - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "LocalCatCList");
        }
        cl->Release();
        owner->nlocalcatclist--;
    }
}

void ResourceOwnerReleaseRelationRef(ResourceOwner owner, bool isCommit)
{
    while (owner->nrelrefs > 0) {
        Relation rel = owner->relrefs[owner->nrelrefs - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "Relation");
            PrintRelCacheLeakWarning(rel);
        }
        /* close do -- */
        RelationClose(rel);
    }
}

void ResourceOwnerReleasePartitionRef(ResourceOwner owner, bool isCommit)
{
    while (owner->npartrefs > 0) {
        Partition part = owner->partrefs[owner->npartrefs - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "Partition");
            PrintPartCacheLeakWarning(part);
        }
        /* close do -- */
        PartitionClose(part);
    }
}

void ResourceOwnerReleaseGlobalCatCTup(ResourceOwner owner, bool isCommit)
{
    while (owner->nglobalcatctup > 0) {
        GlobalCatCTup* global_ct = owner->globalcatctups[owner->nglobalcatctup - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "GlobalCatCTup");
        }
        global_ct->Release();
        owner->nglobalcatctup--;
    }
}

void ResourceOwnerReleaseGlobalCatCList(ResourceOwner owner, bool isCommit)
{
    while (owner->nglobalcatclist > 0) {
        GlobalCatCList* global_cl = owner->globalcatclists[owner->nglobalcatclist - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "GlobalCatCList");
        }
        global_cl->Release();
        owner->nglobalcatclist--;
}
}

void ResourceOwnerReleaseGlobalBaseEntry(ResourceOwner owner, bool isCommit)
{
    while (owner->nglobalbaseentry > 0) {
        GlobalBaseEntry *entry = owner->globalbaseentries[owner->nglobalbaseentry - 1];
        if (isCommit) {
            PrintGlobalSysCacheLeakWarning(owner, "GlobalBaseEntry");
        }
        if (unlikely(entry->refcount == 0)) {
            /* palloc fail */
            entry->FreeError();
        } else {
            entry->Release();
        }
        owner->nglobalbaseentry--;
    }
}

void ResourceOwnerEnlargeGlobalDBEntry(ResourceOwner owner)
{
    int newmax;
    if (owner->nglobaldbentry < owner->maxglobaldbentry)
        return; /* nothing to do */
    if (owner->globaldbentries == NULL) {
        newmax = 16;
        owner->globaldbentries = (GlobalSysDBCacheEntry **)MemoryContextAlloc(owner->memCxt,
            newmax * sizeof(GlobalSysDBCacheEntry *));
        owner->maxglobaldbentry = newmax;
    } else {
        newmax = owner->maxglobaldbentry * 2;
        owner->globaldbentries =
            (GlobalSysDBCacheEntry **)repalloc(owner->globaldbentries, newmax * sizeof(GlobalSysDBCacheEntry *));
        owner->maxglobaldbentry = newmax;
    }
}

extern void ResourceOwnerRememberGlobalDBEntry(ResourceOwner owner, GlobalSysDBCacheEntry* entry)
{
    Assert(owner->nglobaldbentry < owner->maxglobaldbentry);
    owner->globaldbentries[owner->nglobaldbentry] = entry;
    owner->nglobaldbentry++;
}

extern void ResourceOwnerForgetGlobalDBEntry(ResourceOwner owner, GlobalSysDBCacheEntry* entry)
{
    Assert(entry->m_dbOid!= InvalidOid);
    GlobalSysDBCacheEntry** global_entries = owner->globaldbentries;
    int nc1 = owner->nglobaldbentry - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (global_entries[i] == entry) {
            while (i < nc1) {
                global_entries[i] = global_entries[i + 1];
                i++;
            }
            owner->nglobaldbentry = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("the global rel entry is not owned by resource owner %s", owner->name)));
}

extern void ResourceOwnerReleaseGlobalDBEntry(ResourceOwner owner, bool isCommit)
{
    Assert(owner->nglobaldbentry <= 1);
    while (owner->nglobaldbentry > 0) {
        GlobalSysDBCacheEntry *entry = owner->globaldbentries[owner->nglobaldbentry - 1];
        if (isCommit) {
            /* print some debug info */
            PrintGlobalSysCacheLeakWarning(owner, "GlobalDBEntry");
        }
        if (unlikely(entry->m_refcount == 0)) {
            // palloc failed entry
            entry->Free(entry);
        } else {
            entry->Release();
        }
        owner->nglobaldbentry--;
    }
}

void ResourceOwnerEnlargeGlobalIsExclusive(ResourceOwner owner)
{
    int newmax;
    if (owner->nglobalisexclusive < owner->maxglobalisexclusive)
        return; /* nothing to do */
    if (owner->globalisexclusives == NULL) {
        newmax = 16;
        owner->globalisexclusives = (volatile uint32 **)MemoryContextAlloc(owner->memCxt, newmax * sizeof(uint32 *));
        owner->maxglobalisexclusive = newmax;
    } else {
        newmax = owner->maxglobalisexclusive * 2;
        owner->globalisexclusives =
            (volatile uint32 **)repalloc(owner->globalisexclusives, newmax * sizeof(uint32 *));
        owner->maxglobalisexclusive = newmax;
    }
}

extern void ResourceOwnerRememberGlobalIsExclusive(ResourceOwner owner, volatile uint32 *isexclusive)
{
    Assert(owner->nglobalisexclusive < owner->maxglobalisexclusive);
    Assert(*isexclusive == 1);
    owner->globalisexclusives[owner->nglobalisexclusive] = isexclusive;
    owner->nglobalisexclusive++;
}

extern void ResourceOwnerForgetGlobalIsExclusive(ResourceOwner owner, volatile uint32 *isexclusive)
{
    volatile uint32 **global_isexclusives = owner->globalisexclusives;
    int nc1 = owner->nglobalisexclusive - 1;
    int i;
    for (i = nc1; i >= 0; i--) {
        if (global_isexclusives[i] == isexclusive) {
            while (i < nc1) {
                global_isexclusives[i] = global_isexclusives[i + 1];
                i++;
            }
            owner->nglobalisexclusive = nc1;
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
            errmsg("the global isexclusive is not owned by resource owner %s", owner->name)));
}

extern void ResourceOwnerReleaseGlobalIsExclusive(ResourceOwner owner, bool isCommit)
{
    Assert(owner->nglobalisexclusive <= 1);
    while (owner->nglobalisexclusive > 0) {
        volatile uint32 *isexclusive = owner->globalisexclusives[owner->nglobalisexclusive - 1];
        if (isCommit) {
            /* print some debug info */
            PrintGlobalSysCacheLeakWarning(owner, "Global IsExclusive");
        }
        Assert(*isexclusive == 1);
        atomic_compare_exchange_u32(isexclusive, 1, 0);
        owner->nglobalisexclusive--;
    }
}

bool CurrentResourceOwnerIsEmpty(ResourceOwner owner)
{
    if (owner == NULL || !owner->valid) {
        return true;
    }
    Assert(owner->nbuffers == 0);
    Assert(owner->nlocalcatclist == 0);
    Assert(owner->nlocalcatctup == 0);
    Assert(owner->nglobalcatctup == 0);
    Assert(owner->nglobalcatclist == 0);
    Assert(owner->nglobalbaseentry == 0);
    Assert(owner->nglobaldbentry == 0);
    Assert(owner->nglobalisexclusive == 0);
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
    Assert(owner->nPthreadRWlock == 0);
    Assert(owner->npartmaprefs == 0);
    Assert(owner->nglobalMemContext == 0);
    return true;
}
/*
 * ResourceOwnerReleaseAllPlanCacheRefs
 *              Release the plancache references (only) held by this owner.
 *
 * We might eventually add similar functions for other resource types,
 * but for now, only this is needed.
 */
void ResourceOwnerReleaseAllPlanCacheRefs(ResourceOwner owner)
{
    ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = owner;
    ResourceOwnerDecrementNPlanRefs(owner, true);
    t_thrd.utils_cxt.CurrentResourceOwner = save;
}
void ReleaseResownerOutOfTransaction()
{
    if (likely(t_thrd.utils_cxt.CurrentResourceOwner == NULL)) {
        return;
    }
    if (unlikely(IsTransactionOrTransactionBlock())) {
        return;
    }
    ResourceOwner root = t_thrd.utils_cxt.CurrentResourceOwner;
    while (root->parent != NULL) {
        root = root->parent;
    }
    Assert(t_thrd.utils_cxt.TopTransactionResourceOwner != root);
    if (unlikely(t_thrd.utils_cxt.TopTransactionResourceOwner == root)) {
        return;
    }
    Assert(strcmp(root->name, "TopTransaction") != 0);
    if (unlikely(strcmp(root->name, "TopTransaction") == 0)) {
        return;
    }

    ResourceOwnerRelease(root, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    ResourceOwnerRelease(root, RESOURCE_RELEASE_LOCKS, false, true);
    ResourceOwnerRelease(root, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
}

FORCE_INLINE
Buffer ResourceOwnerGetBuffer(ResourceOwner owner)
{
    if (owner != NULL && owner->nbuffers > 0) {
        return owner->buffers[owner->nbuffers - 1];
    }
    return 0;
}
