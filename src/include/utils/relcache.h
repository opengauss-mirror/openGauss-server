/* -------------------------------------------------------------------------
 *
 * relcache.h
 *	  Relation descriptor cache definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/relcache.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RELCACHE_H
#define RELCACHE_H

#include "access/tupdesc.h"
#include "nodes/bitmapset.h"
#include "storage/smgr/relfilenode.h"
#include "utils/hsearch.h"

#define IsValidCatalogParam(catalogDesc) (catalogDesc.oid != InvalidOid)

typedef struct RelationData* Relation;
typedef struct PartitionData* Partition;
struct HeapTupleData;
typedef HeapTupleData* HeapTuple;
typedef int LOCKMODE;

/* ----------------
 *		RelationPtr is used in the executor to support index scans
 *		where we have to keep track of several index relations in an
 *		array.	-cim 9/10/89
 * ----------------
 */
typedef Relation* RelationPtr;

typedef struct CatalogRelationBuildParam {
    Oid oid;
    const char* relationName;
    Oid relationReltype;
    bool isshared;
    bool hasoids;
    int natts;
    const FormData_pg_attribute* attrs;
    bool isnailed;
    bool insertIt;
} CatalogRelationBuildParam;

/*
 * Routines to open (lookup) and close a relcache entry
 */
extern Relation RelationIdGetRelation(Oid relationId);
extern void RelationClose(Relation relation);

/*
 * Routines to compute/retrieve additional cached information
 */
extern List* PartitionGetPartIndexList(Partition part, bool inc_unused = false);
extern List* RelationGetIndexList(Relation relation, bool inc_unused = false);
extern List* RelationGetSpecificKindIndexList(Relation relation, bool isGlobal);
extern List* RelationGetLocalCbiList(Relation relation);
extern List* RelationGetIndexInfoList(Relation relation);
extern int RelationGetIndexNum(Relation relation);
extern Oid RelationGetOidIndex(Relation relation);
extern Oid RelationGetPrimaryKeyIndex(Relation relation);
extern Oid RelationGetReplicaIndex(Relation relation);
extern List* RelationGetIndexExpressions(Relation relation);
extern List* RelationGetIndexExpressions(Relation relation);
extern List* RelationGetDummyIndexExpressions(Relation relation);
extern List* RelationGetIndexPredicate(Relation relation);
extern int16 *relationGetHBucketKey(HeapTuple tuple, int *nColumn);
extern void AtEOXact_FreeTupleDesc();

typedef enum IndexAttrBitmapKind {
    INDEX_ATTR_BITMAP_ALL,
    INDEX_ATTR_BITMAP_KEY,
    INDEX_ATTR_BITMAP_PRIMARY_KEY,
    INDEX_ATTR_BITMAP_IDENTITY_KEY
} IndexAttrBitmapKind;

typedef enum PartitionMetadataStatus {
    PART_METADATA_NOEXIST,   /* partition is not exists */
    PART_METADATA_LIVE,      /* partition is live, normal use */
    PART_METADATA_CREATING,  /* partition is being created */
    PART_METADATA_INVISIBLE  /* partition is invisible, being droped */
} PartStatus;

extern Bitmapset* RelationGetIndexAttrBitmap(Relation relation, IndexAttrBitmapKind keyAttrs);

struct IndexInfo; /* just a statement here */
extern Bitmapset* IndexGetAttrBitmap(Relation relation, struct IndexInfo *indexInfo);

extern void RelationGetExclusionInfo(Relation indexRelation, Oid** operators, Oid** procs, uint16** strategies);

extern void RelationSetIndexList(Relation relation, List* indexIds, Oid oidIndex);

extern void RelationInitIndexAccessInfo(Relation relation, HeapTuple index_tuple = NULL);

/* caller must include pg_publication.h */
struct PublicationActions;
extern struct PublicationActions *GetRelationPublicationActions(Relation relation);

/*
 * Routines for backend startup
 */
extern void RelationCacheInitialize(void);
extern void RelationCacheInitializePhase2(void);
extern void RelationCacheInitializePhase3(void);

/*
 * Routine to create a relcache entry for an about-to-be-created relation
 */
extern Relation RelationBuildLocalRelation(const char* relname, Oid relnamespace, TupleDesc tupDesc, Oid relid,
    Oid relfilenode, Oid reltablespace, bool shared_relation, bool mapped_relation, char relpersistence, char relkind,
    int8 row_compress, Datum reloptions, TableAmType tam_type, int8 relindexsplit = 0, StorageType storage_type = HEAP_DISK,
    Oid accessMethodObjectId = 0);

/*
 * Routine to manage assignment of new relfilenode to a relation
 */
extern void DescTableSetNewRelfilenode(Oid relid, TransactionId freezeXid, bool partition);
extern void DeltaTableSetNewRelfilenode(Oid relid, TransactionId freezeXid, bool partition);
extern void RelationSetNewRelfilenode(Relation relation, TransactionId freezeXid,  MultiXactId minmulti,
    bool isDfsTruncate = false);
extern RelFileNodeBackend CreateNewRelfilenode(Relation relation, TransactionId freezeXid);
extern RelFileNodeBackend CreateNewRelfilenodePart(Relation parent, Partition part);

extern void UpdatePgclass(Relation relation, TransactionId freezeXid, const RelFileNodeBackend *rnode);
extern void UpdatePartition(Relation parent, Partition part, TransactionId freezeXid, const RelFileNodeBackend *newrnode);

/*
 * Routines for flushing/rebuilding relcache entries in various scenarios
 */
extern void RelationForgetRelation(Oid rid);

extern void RelationCacheInvalidateEntry(Oid relationId);

extern void RelationCacheInvalidate(void);

extern void RelationCacheInvalidateBuckets();

extern void RelationCloseSmgrByOid(Oid relationId);
extern Oid  RelationGetBucketOid(Relation relation);

extern void AtEOXact_RelationCache(bool isCommit);
extern void AtEOSubXact_RelationCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);

extern void InvalidateRelationNodeList();
/*
 * Routines to help manage rebuilding of relcache init files
 */
extern bool RelationIdIsInInitFile(Oid relationId);
extern void RelationCacheInitFilePreInvalidate(void);
extern void RelationCacheInitFilePostInvalidate(void);
extern void RelationCacheInitFileRemove(void);

extern TupleDesc BuildHardcodedDescriptor(int natts, const FormData_pg_attribute* attrs, bool hasoids);
extern TupleDesc GetDefaultPgClassDesc(void);
extern TupleDesc GetDefaultPgIndexDesc(void);

extern bool CheckRelationInRedistribution(Oid rel_oid);

extern CatalogRelationBuildParam GetCatalogParam(Oid id);

extern HeapTuple ScanPgRelation(Oid targetRelId, bool indexOK, bool force_non_historic);

/*
 * this function is used for timeseries database, do not call this function directly!!!
 */
extern Relation tuple_get_rel(HeapTuple pg_class_tuple, LOCKMODE lockmode, TupleDesc tuple_desc, HeapTuple pg_indextuple = NULL);
extern THR_LOCAL bool needNewLocalCacheFile;
#endif /* RELCACHE_H */
