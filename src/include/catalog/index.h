/* -------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/index.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"
#include "utils/tuplesort.h"

#define DEFAULT_INDEX_TYPE	"btree"
#define DEFAULT_HASH_INDEX_TYPE "hash"
#define DEFAULT_CSTORE_INDEX_TYPE "psort"
#define DEFAULT_GIST_INDEX_TYPE	"gist"
#define CSTORE_BTREE_INDEX_TYPE "cbtree"
#define DEFAULT_GIN_INDEX_TYPE "gin"
#define CSTORE_GINBTREE_INDEX_TYPE "cgin"
#define DEFAULT_USTORE_INDEX_TYPE "ubtree"
#define DEFAULT_IVFFLAT_INDEX_TYPE "ivfflat"
#define DEFAULT_HNSW_INDEX_TYPE "hnsw"

/* Typedef for callback function for IndexBuildHeapScan */
typedef void (*IndexBuildCallback)(Relation index, HeapTuple htup, Datum *values, const bool *isnull,
                                   bool tupleIsAlive, void *state);

typedef void (*IndexBuildVecBatchScanCallback)(Relation index, ItemPointer tid, Datum *values,
                                               const bool *isnull, void *state);

/* Action code for index_set_state_flags */
typedef enum
{
    INDEX_CREATE_SET_READY,
    INDEX_CREATE_SET_VALID,
    INDEX_DROP_CLEAR_VALID,
    INDEX_DROP_SET_DEAD
} IndexStateFlagsAction;

typedef struct IndexBucketShared {
    Oid heaprelid;
    Oid indexrelid;
    Oid heappartid;
    Oid indexpartid;
    slock_t mutex;
    pg_atomic_uint32 curiter;
    int nparticipants;
    IndexBuildResult indresult;
} IndexBucketShared;

/* */
#define	REINDEX_BTREE_INDEX (1<<1)
#define	REINDEX_HASH_INDEX (1<<2)
#define	REINDEX_GIN_INDEX (1<<3)
#define	REINDEX_GIST_INDEX (1<<4)
#define REINDEX_CGIN_INDEX (1<<5)
#define	REINDEX_ALL_INDEX   (REINDEX_BTREE_INDEX|REINDEX_HASH_INDEX|REINDEX_GIN_INDEX|REINDEX_GIST_INDEX|REINDEX_CGIN_INDEX)


/* state info for validate_index bulkdelete callback */
typedef struct {
    Tuplesortstate* tuplesort; /* for sorting the index TIDs */
    /* statistics (for debug purposes only): */
    double htups, itups, tups_inserted;
} v_i_state;


typedef enum CheckWaitMode 
{
    CHECK_WAIT,
    CHECK_NOWAIT,
} CheckWaitMode;

extern void index_check_primary_key(Relation heapRel, IndexInfo *indexInfo, bool is_alter_table, IndexStmt *stmt,
    bool is_modify_primary = false);

/*
 * Parameter isPartitionedIndex indicates whether the index is a partition index.
 * Parameter isGlobalPartitionedIndex indicates whether the index is a global partition index.
 * -------------------------------------------------------------------------
 * | isPartitionedIndex | isGlobalPartitionedIndex |      Description       |
 * -------------------------------------------------------------------------
 * |        false       |           false          | normal relation index  |
 * -------------------------------------------------------------------------
 * |        true        |           false          | local partition index  |
 * -------------------------------------------------------------------------
 * |        false       |           true           | can not happen         |
 * -------------------------------------------------------------------------
 * |        true        |           true           | global partition index |
 * -------------------------------------------------------------------------
 */
typedef struct
{
    Oid  existingPSortOid;
    bool isPartitionedIndex;
    bool isGlobalPartitionedIndex;
    bool crossBucket;
} IndexCreateExtraArgs;

typedef enum {
    INDEX_CREATE_NONE_PARTITION,
    INDEX_CREATE_LOCAL_PARTITION,
    INDEX_CREATE_GLOBAL_PARTITION
} IndexCreatePartitionType;

typedef enum { ALL_KIND, GLOBAL_INDEX, LOCAL_INDEX } IndexKind;

#define PARTITION_TYPE(extra)                                                                                      \
    (extra->isPartitionedIndex == false ? INDEX_CREATE_NONE_PARTITION                                              \
                                        : (extra->isGlobalPartitionedIndex == false ? INDEX_CREATE_LOCAL_PARTITION \
                                                                                    : INDEX_CREATE_GLOBAL_PARTITION))


extern Oid index_create(Relation heapRelation, const char *indexRelationName, Oid indexRelationId,
                        Oid relFileNode, IndexInfo *indexInfo, List *indexColNames, Oid accessMethodObjectId,
                        Oid tableSpaceId, Oid *collationObjectId, Oid *classObjectId, int16 *coloptions,
                        Datum reloptions, bool isprimary, bool isconstraint, bool deferrable,
                        bool initdeferred, bool allow_system_table_mods, bool skip_build, bool concurrent,
                        IndexCreateExtraArgs *extra, bool useLowLockLevel = false,
                        int8 relindexsplit = 0, bool visible = true, bool isvalidated = true, bool isdisable = false);


extern Oid index_concurrently_create_copy(Relation heapRelation, Oid oldIndexId, Oid oldIndexPartId, const char *newName);
extern void index_concurrently_build(Oid heapRelationId, Oid indexRelationId, bool isPrimary, AdaptMem* memInfo = NULL, bool dbWide = false);
extern void index_concurrently_swap(Oid newIndexId, Oid oldIndexId, const char *oldName);
extern void index_concurrently_set_dead(Oid heapId, Oid indexId);
extern void index_concurrently_part_build(Oid heapRelationId, Oid heapPartitionId, Oid indexRelationId, Oid IndexPartitionId, AdaptMem* memInfo = NULL, bool dbWide = false);
extern void index_concurrently_part_swap(Oid newIndexPartId, Oid oldIndexPartId, const char *oldName);

extern ObjectAddress index_constraint_create(Relation heapRelation, Oid indexRelationId, IndexInfo *indexInfo,
                                    const char *constraintName, char constraintType, bool deferrable,
                                    bool initdeferred, bool mark_as_primary, bool update_pgindex,
                                    bool remove_old_dependencies, bool allow_system_table_mods, 
                                    bool isvalidated = true, bool isdisable = false);

extern void index_drop(Oid indexId, bool concurrent, bool concurrent_lock_mode = false);

extern IndexInfo *BuildIndexInfo(Relation index);
extern IndexInfo *BuildDummyIndexInfo(Relation index);

extern void BuildSpeculativeIndexInfo(Relation index, IndexInfo* ii);

extern void FormIndexDatumForRedis(const TupleTableSlot* slot, Datum* values, bool* isnull);

extern void FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   EState *estate,
			   Datum *values,
			   bool *isnull);
extern void index_build(Relation heapRelation,
			Partition heapPartition,
			Relation indexRelation,
			Partition indexPartition,
			IndexInfo *indexInfo,
			bool isprimary,
			bool isreindex,
			IndexCreatePartitionType partitionType,
			bool parallel = true,
			bool isTruncGTT = false);

extern double IndexBuildHeapScan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, IndexBuildCallback callback, void *callback_state, TableScanDesc scan = NULL,
    BlockNumber startBlkno = 0, BlockNumber numblocks = InvalidBlockNumber);
extern double IndexBuildUHeapScan(Relation heapRelation,
                         Relation indexRelation,
                         IndexInfo *indexInfo,
                         bool allowSync,
                         IndexBuildCallback callback,
                         void *callbackState,
                         TableScanDesc scan /* set as NULL */);
extern double* GlobalIndexBuildHeapScan(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo,
                                 IndexBuildCallback callback, void* callbackState);
extern List* LockAllGlobalIndexes(Relation relation, LOCKMODE lockmode);
extern void ReleaseLockAllGlobalIndexes(List** indexRelList, LOCKMODE lockmode);
extern double IndexBuildHeapScanCrossBucket(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
                                            IndexBuildCallback callback, void *callbackState);
extern double IndexBuildVectorBatchScan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
                                        VectorBatch *vecScanBatch, Snapshot snapshot,
                                        IndexBuildVecBatchScanCallback callback, void *callback_state, 
                                        void *transferFuncs);


extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot, bool isPart = false);
extern void validate_index_heapscan(
    Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state);

extern void index_set_state_flags(Oid indexId, IndexStateFlagsAction action);
extern void reindex_indexpart_internal(Relation heapRelation, 
                                       Relation iRel, 
                                       IndexInfo* indexInfo, 
                                       Oid indexPartId,
                                       void *baseDesc);
extern void reindex_index(Oid indexId, Oid indexPartId,
                          bool skip_constraint_checks, AdaptMem *memInfo,
                          bool dbWide,
                          void *baseDesc = NULL,
                          bool isTruncGTT = false);
extern void ReindexGlobalIndexInternal(Relation heapRelation, Relation iRel, IndexInfo* indexInfo, void* baseDesc);

/* Flag bits for ReindexRelation(): */
#define REINDEX_REL_PROCESS_TOAST		0x01
#define REINDEX_REL_SUPPRESS_INDEX_USE	0x02
#define REINDEX_REL_CHECK_CONSTRAINTS	0x04

extern bool ReindexRelation(Oid relid, int flags, int reindexType,
    void *baseDesc = NULL,
    AdaptMem* memInfo = NULL,
    bool dbWide = false,
    IndexKind indexKind = ALL_KIND,
    bool isTruncGTT = false);
extern bool ReindexIsProcessingHeap(Oid heapOid);
extern bool ReindexIsProcessingIndex(Oid indexOid);
extern Oid IndexGetRelation(Oid indexId, bool missing_ok);

extern Oid PartIndexGetPartition(Oid partIndexId, bool missing_ok);
extern Oid PartIdGetParentId(Oid partIndexId, bool missing_ok);

typedef struct
{
    Oid  existingPSortOid;
    bool crossbucket;
} PartIndexCreateExtraArgs;

extern Oid partition_index_create(const char* partIndexName,
                                  Oid partIndexFileNode,
                                  Partition partition,
                                  Oid tspid,
                                  Relation parentIndex,
                                  Relation partitionedTable,
                                  Relation pg_partition_rel,
                                  IndexInfo *indexInfo,
                                  List *indexColNames,
                                  Datum  indexRelOptions,
                                  bool skipBuild,
                                  PartIndexCreateExtraArgs *extra,
                                  bool isUsable = true);
extern void addIndexForPartition(Relation partitionedRelation, Oid partOid);
extern void dropIndexForPartition(Oid partOid);
extern void index_update_stats(Relation rel, bool hasindex, bool isprimary,
                               Oid reltoastidxid, Oid relcudescidx, double reltuples);
extern void PartitionNameCallbackForIndexPartition(Oid partitionedRelationOid, 
                                                   const char *partitionName, 
                                                   Oid partId, 
                                                   Oid oldPartId, 
                                                   char partition_type, 
                                                   void *arg,
                                                   LOCKMODE callbackobj_lockMode);
extern void reindex_partIndex(Relation heapRel,  Partition heapPart, Relation indexRel , Partition indexPart);
extern bool reindexPartition(Oid relid, Oid partOid, int flags, int reindexType);
extern Oid indexIdAndPartitionIdGetIndexPartitionId(Oid indexId, Oid partOid);
extern void AddGPIForPartition(Oid partTableOid, Oid partOid);
extern void AddGPIForSubPartition(Oid partTableOid, Oid partOid, Oid subPartOid);
void AddCBIForPartition(Relation partTableRel, Relation tempTableRel, const List* indexRelList, 
    const List* indexDestOidList);
extern bool DeleteGPITuplesForPartition(Oid partTableOid, Oid partOid);
extern bool DeleteGPITuplesForSubPartition(Oid partTableOid, Oid partOid, Oid subPartOid);
extern void mergeBTreeIndexes(List* mergingBtreeIndexes, List* srcPartMergeOffset, int2 bktId);
extern bool RecheckIndexTuple(IndexScanDesc scan, TupleTableSlot *slot);
extern void SetIndexCreateExtraArgs(IndexCreateExtraArgs* extra, Oid psortOid, bool isPartition, bool isGlobal,
    bool crossBucket = false);
extern void cbi_set_enable_clean(Relation rel);
void ScanBucketsInsertIndex(Relation rel, const List* idxRelList, const List* idxInfoList);
extern void ScanPartitionInsertIndex(Relation partTableRel, Relation partRel, const List* indexRelList,
                              const List* indexInfoList);
void ScanHeapInsertCBI(Relation parentRel, Relation heapRel, Relation idxRel, Oid tmpPartOid);
#ifdef USE_SPQ
extern Datum spq_get_root_ctid(HeapTuple tuple, Buffer buffer, ExprContext *econtext);
#endif
#endif   /* INDEX_H */
