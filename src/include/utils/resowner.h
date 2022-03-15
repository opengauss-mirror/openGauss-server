/* -------------------------------------------------------------------------
 *
 * resowner.h
 *	  openGauss resource owner definitions.
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
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/utils/resowner.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RESOWNER_H
#define RESOWNER_H

#include "storage/smgr/fd.h"
#include "utils/catcache.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/knl_localsystupcache.h"
#include "utils/plancache.h"
#include "utils/snapshot.h"
#include "utils/partcache.h"
#include "storage/cucache_mgr.h"
/*
 * ResourceOwner objects are an opaque data structure known only within
 * resowner.c.
 */
typedef struct ResourceOwnerData* ResourceOwner;

extern THR_LOCAL PGDLLIMPORT ResourceOwner IsolatedResourceOwner;

/*
 * Resource releasing is done in three phases: pre-locks, locks, and
 * post-locks.	The pre-lock phase must release any resources that are
 * visible to other backends (such as pinned buffers); this ensures that
 * when we release a lock that another backend may be waiting on, it will
 * see us as being fully out of our transaction.  The post-lock phase
 * should be used for backend-internal cleanup.
 */
typedef enum {
    RESOURCE_RELEASE_BEFORE_LOCKS,
    RESOURCE_RELEASE_LOCKS,
    RESOURCE_RELEASE_AFTER_LOCKS
} ResourceReleasePhase;

/*
 *	Dynamically loaded modules can get control during ResourceOwnerRelease
 *	by providing a callback of this form.
 */
typedef void (*ResourceReleaseCallback)(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void* arg);

/*
 * Functions in resowner.c
 */

/* generic routines */
extern ResourceOwner ResourceOwnerCreate(ResourceOwner parent, const char* name, MemoryContext memCxt);
extern void ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool isCommit, bool isTopLevel);
extern void ResourceOwnerDelete(ResourceOwner owner);
extern void ResourceOwnerConcat(ResourceOwner target, ResourceOwner source);
extern ResourceOwner ResourceOwnerGetParent(ResourceOwner owner);
extern ResourceOwner ResourceOwnerGetNextChild(ResourceOwner owner);
extern const char * ResourceOwnerGetName(ResourceOwner owner);
extern ResourceOwner ResourceOwnerGetFirstChild(ResourceOwner owner);
extern MemoryContext ResourceOwnerGetMemCxt(ResourceOwner owner);
extern void ResourceOwnerNewParent(ResourceOwner owner, ResourceOwner newparent);

/* support for buffer refcount management */
extern void ResourceOwnerEnlargeBuffers(ResourceOwner owner);
extern void ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer buffer);
extern void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer);

/* support for segment head buffer refcount management */
extern void ResourceOwnerEnlargeSegmentBuffers(ResourceOwner owner);
extern void ResourceOwnerRememberSegmentBuffer(ResourceOwner owner, Buffer buffer, SegmentDesc *seg_desc);
extern void ResourceOwnerForgetSegmentBuffer(ResourceOwner owner, SegmentDesc *seg_desc);

/* support for catcache refcount management */
extern void ResourceOwnerEnlargeCatCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheRef(ResourceOwner owner, HeapTuple tuple);
extern void ResourceOwnerForgetCatCacheRef(ResourceOwner owner, HeapTuple tuple);
extern void ResourceOwnerEnlargeCatCacheListRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheListRef(ResourceOwner owner, CatCList* list);
extern void ResourceOwnerForgetCatCacheListRef(ResourceOwner owner, CatCList* list);

/* support for relcache refcount management */
extern void ResourceOwnerEnlargeRelationRefs(ResourceOwner owner);
extern void ResourceOwnerRememberRelationRef(ResourceOwner owner, Relation rel);
extern void ResourceOwnerForgetRelationRef(ResourceOwner owner, Relation rel);
/* support for partcache refcount management */
extern void ResourceOwnerEnlargePartitionRefs(ResourceOwner owner);

extern void ResourceOwnerRememberPartitionRef(ResourceOwner owner,
                                                         Partition part);
extern void ResourceOwnerForgetPartitionRef(ResourceOwner owner,
                                                      Partition part);

extern void ResourceOwnerRememberFakerelRef(ResourceOwner owner, Relation fakerel);
extern void ResourceOwnerForgetFakerelRef(ResourceOwner owner, Relation fakerel);

/* support for fakepart refcount management */
extern void ResourceOwnerEnlargeFakepartRefs(ResourceOwner owner);
extern void ResourceOwnerRememberFakepartRef(ResourceOwner owner,
                                                        Partition fakepart);
extern void ResourceOwnerForgetFakepartRef(ResourceOwner owner,
                                                     Partition fakerel);

/* support for plancache refcount management */
extern void ResourceOwnerEnlargePlanCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner, CachedPlan* plan);
extern void ResourceOwnerForgetPlanCacheRef(ResourceOwner owner, CachedPlan* plan);

/* support for tupledesc refcount management */
extern void ResourceOwnerEnlargeTupleDescs(ResourceOwner owner);
extern void ResourceOwnerRememberTupleDesc(ResourceOwner owner, TupleDesc tupdesc);
extern void ResourceOwnerForgetTupleDesc(ResourceOwner owner, TupleDesc tupdesc);

/* support for snapshot refcount management */
extern void ResourceOwnerEnlargeSnapshots(ResourceOwner owner);
extern void ResourceOwnerRememberSnapshot(ResourceOwner owner, Snapshot snapshot);
extern bool ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snapshot, bool ereport);
extern void ResourceOwnerDecrementNsnapshots(ResourceOwner owner, void* queryDesc);
extern void ResourceOwnerDecrementNPlanRefs(ResourceOwner owner, bool useResOwner);

/* support for temporary file management */
extern void ResourceOwnerEnlargeFiles(ResourceOwner owner);
extern void ResourceOwnerRememberFile(ResourceOwner owner, File file);
extern void ResourceOwnerForgetFile(ResourceOwner owner, File file);

/* support for data cache refcount management */
extern void ResourceOwnerEnlargeDataCacheSlot(ResourceOwner owner);
extern void ResourceOwnerRememberDataCacheSlot(ResourceOwner owner, CacheSlotId_t slotid);
extern void ResourceOwnerForgetDataCacheSlot(ResourceOwner owner, CacheSlotId_t slotid);

/* support for meta cache refcount management */
extern void ResourceOwnerEnlargeMetaCacheSlot(ResourceOwner owner);
extern void ResourceOwnerRememberMetaCacheSlot(ResourceOwner owner, CacheSlotId_t slotid);
extern void ResourceOwnerForgetMetaCacheSlot(ResourceOwner owner, CacheSlotId_t slotid);

// support for pthread mutex
//
extern void ResourceOwnerForgetPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex);

extern void ResourceOwnerRememberPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex);

extern void ResourceOwnerEnlargePthreadMutex(ResourceOwner owner);

extern void PrintPthreadMutexLeakWarning(pthread_mutex_t* pMutex);
extern void PrintResourceOwnerLeakWarning();
extern void ResourceOwnerReleasePthreadMutex();

extern void ResourceOwnerEnlargePartitionMapRefs(ResourceOwner owner);
extern void ResourceOwnerRememberPartitionMapRef(ResourceOwner owner, PartitionMap* partmap);
extern void ResourceOwnerForgetPartitionMapRef(ResourceOwner owner, PartitionMap* partmap);

extern void ResourceOwnerEnlargeGMemContext(ResourceOwner owner);
extern void ResourceOwnerRememberGMemContext(ResourceOwner owner, MemoryContext memcontext);
extern void ResourceOwnerForgetGMemContext(ResourceOwner owner, MemoryContext memcontext);
extern void PrintGMemContextLeakWarning(MemoryContext memcontext);

extern void ResourceOwnerMarkInvalid(ResourceOwner owner);
extern bool ResourceOwnerIsValid(ResourceOwner owner);

extern void ResourceOwnerReleaseAllPlanCacheRefs(ResourceOwner owner);

extern void PrintPthreadRWlockLeakWarning(pthread_rwlock_t* pRWlock);
/* support for local catcache tuple/list ref management */
extern void ResourceOwnerEnlargeLocalCatCList(ResourceOwner owner);
extern void ResourceOwnerRememberLocalCatCList(ResourceOwner owner, LocalCatCList* list);
extern void ResourceOwnerForgetLocalCatCList(ResourceOwner owner, LocalCatCList* list);
extern void ResourceOwnerEnlargeLocalCatCTup(ResourceOwner owner);
extern void ResourceOwnerRememberLocalCatCTup(ResourceOwner owner, LocalCatCTup* tuple);
extern LocalCatCTup* ResourceOwnerForgetLocalCatCTup(ResourceOwner owner, HeapTuple tuple);
/* support for global catcache tuple/list ref management */
extern void ResourceOwnerEnlargePthreadRWlock(ResourceOwner owner);
extern void ResourceOwnerForgetPthreadRWlock(ResourceOwner owner, pthread_rwlock_t* pRWlock);
extern void ResourceOwnerRememberPthreadRWlock(ResourceOwner owner, pthread_rwlock_t* pRWlock);
extern void ResourceOwnerEnlargeGlobalCatCTup(ResourceOwner owner);
extern void ResourceOwnerRememberGlobalCatCTup(ResourceOwner owner, GlobalCatCTup* tuple);
extern void ResourceOwnerForgetGlobalCatCTup(ResourceOwner owner, GlobalCatCTup* tuple);
extern void ResourceOwnerEnlargeGlobalCatCList(ResourceOwner owner);
extern void ResourceOwnerRememberGlobalCatCList(ResourceOwner owner, GlobalCatCList* list);
extern void ResourceOwnerForgetGlobalCatCList(ResourceOwner owner, GlobalCatCList* list);
extern void ResourceOwnerEnlargeGlobalBaseEntry(ResourceOwner owner);
extern void ResourceOwnerRememberGlobalBaseEntry(ResourceOwner owner, GlobalBaseEntry* entry);
extern void ResourceOwnerForgetGlobalBaseEntry(ResourceOwner owner, GlobalBaseEntry* entry);
extern void ResourceOwnerEnlargeGlobalDBEntry(ResourceOwner owner);
extern void ResourceOwnerRememberGlobalDBEntry(ResourceOwner owner, GlobalSysDBCacheEntry* entry);
extern void ResourceOwnerForgetGlobalDBEntry(ResourceOwner owner, GlobalSysDBCacheEntry* entry);
extern void ResourceOwnerEnlargeGlobalIsExclusive(ResourceOwner owner);
extern void ResourceOwnerRememberGlobalIsExclusive(ResourceOwner owner, volatile uint32 *isexclusive);
extern void ResourceOwnerForgetGlobalIsExclusive(ResourceOwner owner, volatile uint32 *isexclusive);

extern void ResourceOwnerReleaseRWLock(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseLocalCatCTup(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseLocalCatCList(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseRelationRef(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleasePartitionRef(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseGlobalCatCTup(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseGlobalCatCList(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseGlobalBaseEntry(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseGlobalDBEntry(ResourceOwner owner, bool isCommit);
extern void ResourceOwnerReleaseGlobalIsExclusive(ResourceOwner owner, bool isCommit);
extern bool CurrentResourceOwnerIsEmpty(ResourceOwner owner);
#endif /* RESOWNER_H */
