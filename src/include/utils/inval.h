/* -------------------------------------------------------------------------
 *
 * inval.h
 *	  openGauss cache invalidation dispatcher definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/inval.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef INVAL_H
#define INVAL_H

#include "access/htup.h"
#include "utils/relcache.h"
#include "utils/partcache.h"

typedef void (*SyscacheCallbackFunction)(Datum arg, int cacheid, uint32 hashvalue);
typedef void (*RelcacheCallbackFunction)(Datum arg, Oid relid);
typedef void (*PartcacheCallbackFunction)(Datum arg, Oid partid);

/*
 * Dynamically-registered callback functions.  Current implementation
 * assumes there won't be very many of these at once; could improve if needed.
 */

#define MAX_SYSCACHE_CALLBACKS 30
#define MAX_RELCACHE_CALLBACKS 5
#define MAX_PARTCACHE_CALLBACKS 5

typedef struct SYSCACHECALLBACK {
    int16 id; /* cache number */
    SyscacheCallbackFunction function;
    Datum arg;
} SYSCACHECALLBACK;

typedef struct RELCACHECALLBACK {
    RelcacheCallbackFunction function;
    Datum arg;
} RELCACHECALLBACK;

typedef struct PARTCACHECALLBACK {
    PartcacheCallbackFunction function;
    Datum arg;
} PARTCACHECALLBACK;

extern void AcceptInvalidationMessages(void);

extern void AtStart_Inval(void);

extern void AtSubStart_Inval(void);

extern void AtEOXact_Inval(bool isCommit);

extern void AtEOSubXact_Inval(bool isCommit);

extern void AtPrepare_Inval(void);

extern void PostPrepare_Inval(void);

extern void CommandEndInvalidationMessages(void);

extern void CacheInvalidateHeapTuple(Relation relation, HeapTuple tuple, HeapTuple newtuple);

extern void CacheInvalidateFunction(Oid funcOid, Oid pkgId);

extern void CacheInvalidateCatalog(Oid catalogId);

extern void CacheInvalidateRelcache(Relation relation);

extern void CacheInvalidateRelcacheByTuple(HeapTuple classTuple);

extern void CacheInvalidateRelcacheByRelid(Oid relid);

extern void CacheInvalidateSmgr(RelFileNodeBackend rnode);

extern void CacheInvalidateRelmap(Oid databaseId);

extern void CacheInvalidateHeapTupleInplace(Relation relation, HeapTuple tuple);

extern void inval_twophase_postcommit(TransactionId xid, uint16 info, void* recdata, uint32 len);

extern void CacheInvalidatePartcache(Partition partition);
extern void CacheInvalidatePartcacheByTuple(HeapTuple partitionTuple);
extern void CacheInvalidatePartcacheByPartid(Oid partid);

extern void InvalidateSystemCaches(void);

extern void CacheRegisterThreadSyscacheCallback(int cacheid, SyscacheCallbackFunction func, Datum arg);
extern void CacheRegisterThreadRelcacheCallback(RelcacheCallbackFunction func, Datum arg);
extern void CacheRegisterThreadPartcacheCallback(PartcacheCallbackFunction func, Datum arg);
extern void CacheRegisterSessionSyscacheCallback(int cacheid, SyscacheCallbackFunction func, Datum arg);
extern void CacheRegisterSessionRelcacheCallback(RelcacheCallbackFunction func, Datum arg);
extern void CacheRegisterSessionPartcacheCallback(PartcacheCallbackFunction func, Datum arg);
extern void CallThreadSyscacheCallbacks(int cacheid, uint32 hashvalue);
extern void CallSessionSyscacheCallbacks(int cacheid, uint32 hashvalue);
extern void InvalidateSessionSystemCaches(void);
extern void InvalidateThreadSystemCaches(void);
extern void CacheInvalidateRelcacheAll(void);

#endif /* INVAL_H */
