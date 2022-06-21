/* -------------------------------------------------------------------------
 *
 * heap.h
 *	  prototypes for functions in backend/catalog/heap.c
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/heap.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HEAP_H
#define HEAP_H

#include "parser/parse_node.h"
#include "catalog/indexing.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"

#define PSORT_RESERVE_COLUMN	"tid"
#define CHCHK_PSORT_RESERVE_COLUMN(attname)		(strcmp(PSORT_RESERVE_COLUMN, (attname)) == 0)

typedef struct RawColumnDefault {
    AttrNumber attnum;         /* attribute to attach default to */
    Node      *raw_default;    /* default value (untransformed parse tree) */
    char       generatedCol;   /* generated column setting */
} RawColumnDefault;

typedef struct CookedConstraint {
	ConstrType	contype;         /* CONSTR_DEFAULT or CONSTR_CHECK */
	char	   *name;            /* name, or NULL if none */
	AttrNumber	attnum;          /* which attr (only for DEFAULT) */
	Node	   *expr;            /* transformed default or check expr */
	bool		skip_validation; /* skip validation? (only for CHECK) */
	bool		is_local;        /* constraint has local (non-inherited) def */
	int			inhcount;        /* number of times constraint is inherited */
	bool		is_no_inherit;   /* constraint has local def and cannot be
								 * inherited */
} CookedConstraint;

typedef struct CeHeapInfo {
    int8 alg_type; /* int representation of encryption type RANDOMIZED/DETERMINISTIC */
    Oid orig_typ;
    int32 orig_mod; 
    Oid cek_id;
    NameData column_name;
    int4 attnum;
} CeHeapInfo;

typedef struct HashBucketInfo {
	oidvector  *bucketlist;
	int2vector *bucketcol;
	Oid          bucketOid;
} HashBucketInfo;

typedef struct SliceConstInfo {
    char* sliceName;
    int sliceNum;
    List* sliceBoundary;
    Const* sliceBoundaryValue[RANGE_PARTKEYMAXNUM];
} SliceConstInfo;

extern Relation heap_create(const char *relname,
                                Oid relnamespace,
                                Oid reltablespace,
                                Oid relid,
                                Oid relfilenode,
                                Oid bucketOid,
                                TupleDesc tupDesc,
                                char relkind,
                                char relpersistence,
                                bool partitioned_relation,
                                bool rowMovement,
                                bool shared_relation,
                                bool mapped_relation,
                                bool allow_system_table_mods,
                                int8 row_compress,
                                Datum reloptions,
                                Oid ownerid,
                                bool skip_create_storage,
                                TableAmType tam_type,
                                int8 relindexsplit = 0,
                                StorageType storage_type = HEAP_DISK,
                                bool newcbi = false,
                                Oid accessMethodObjectId = 0);

extern bool heap_is_matview_init_state(Relation rel);

extern Partition
heapCreatePartition(const char* part_name,
                          bool for_partitioned_table,
                          Oid part_tablespace,
                          Oid part_id,
                          Oid partFileNode,
                          Oid bucketOid,
                          Oid ownerid,
                          StorageType storage_type,
                          bool newcbi = false,
                          Datum reloptions = Datum(0));


extern Oid heap_create_with_catalog(const char *relname,
                         Oid relnamespace,
                         Oid reltablespace,
                         Oid relid,
						 Oid reltypeid,
						 Oid reloftypeid,
						 Oid ownerid,
						 TupleDesc tupdesc,
						 List *cooked_constraints,
						 char relkind,
						 char relpersistence,
						 bool shared_relation,
						 bool mapped_relation,
						 bool oidislocal,
						 int oidinhcount,
						 OnCommitAction oncommit,
						 Datum reloptions,
						 bool use_user_acl,
						 bool allow_system_table_mods,
						 PartitionState *partTableState,
						 int8 row_compress,
						 HashBucketInfo *bucketinfo,
						 bool record_dependce = true,
						 List* ceLst = NULL,
						 StorageType storage_type = HEAP_DISK,
						 LOCKMODE partLockMode = AccessExclusiveLock);

extern void heap_create_init_fork(Relation rel);

extern void heap_drop_with_catalog(Oid relid);
extern void heapDropPartition(Relation rel, Partition part);
extern void dropToastTableOnPartition(Oid partId);
extern void dropCuDescTableOnPartition(Oid partId);
extern void dropDeltaTableOnPartition(Oid partId);


extern void heapDropPartitionToastList(List* toastList);
extern void heapDropPartitionList(Relation rel, List* partitionList);
extern Oid heapAddRangePartition(Relation pgPartRel, Oid partTableOid,  Oid partTablespace,
                                 Oid bucketOid, RangePartitionDefState *newPartDef, Oid ownerid, Datum reloptions,
                                 const bool* isTimestamptz, StorageType storage_type, LOCKMODE partLockMode, int2vector* subpartition_key = NULL, bool isSubPartition = false);

extern Oid HeapAddListPartition(Relation pgPartRel, Oid partTableOid,  Oid partTablespace,
                                Oid bucketOid, ListPartitionDefState *newPartDef, Oid ownerid, Datum reloptions,
                                const bool* isTimestamptz, StorageType storage_type, int2vector* subpartition_key = NULL, bool isSubPartition = false);

extern Oid HeapAddHashPartition(Relation pgPartRel, Oid partTableOid,  Oid partTablespace,
                                Oid bucketOid, HashPartitionDefState *newPartDef, Oid ownerid, Datum reloptions,
                                const bool* isTimestamptz, StorageType storage_type, int2vector* subpartition_key = NULL, bool isSubPartition = false);
extern Node *MakeDefaultSubpartition(PartitionState *partitionState, Node *partitionDefState);
extern List *addNewSubPartitionTuplesForPartition(Relation pgPartRel, Oid partTableOid, Oid partTablespace,
    Oid bucketOid, Oid ownerid, Datum reloptions, const bool *isTimestamptz, StorageType storage_type,
    PartitionState *partitionState, Node *partitionDefState, LOCKMODE partLockMode);

extern Oid GetPartTablespaceOidForSubpartition(Oid reltablespace, const char* partTablespacename);

extern void heapDropPartitionIndex(Relation parentIndex, Oid partIndexId);
extern void addNewPartitionTuple(Relation pg_part_desc, Partition new_part_desc, int2vector* pkey, oidvector *intablespace,
    Datum interval, Datum maxValues,  Datum transitionPoint, Datum reloptions);

extern void heap_truncate_one_part(Relation rel , Oid partOid);
extern Oid heapTupleGetPartitionId(Relation rel, void *tuple, bool isDDL = false);
extern Oid heapTupleGetSubPartitionId(Relation rel, void *tuple);
extern void heap_truncate(List *relids);
extern void heap_truncate_one_rel(Relation rel);
extern void heap_truncate_check_FKs(List *relations, bool tempTables);
extern List *heap_truncate_find_FKs(List *relationIds);
extern void InsertTablebucketidAttribute(Oid relOid);
extern void InsertPgAttributeTuple(Relation pg_attribute_rel, Form_pg_attribute new_attribute, CatalogIndexState indstate);

extern void InsertPgClassTuple(Relation pg_class_desc, Relation new_rel_desc, Oid new_rel_oid, Datum relacl,
    Datum reloptions, char relkind, int2vector *bucketcol);

extern List *AddRelationNewConstraints(Relation rel, List *newColDefaults, List *newConstraints, bool allow_merge, bool is_local);

extern List *AddRelClusterConstraints(Relation rel, List *clusterKeys);
extern void StoreAttrDefault(Relation rel, AttrNumber attnum, Node *expr,  char generatedCol);
extern Node *cookDefault(ParseState *pstate, Node *raw_default, Oid atttypid, int32 atttypmod, char *attname,
    char generatedCol);
extern void DeleteRelationTuple(Oid relid);
extern void DeleteAttributeTuples(Oid relid);
extern void DeleteSystemAttributeTuples(Oid relid);
extern void RemoveAttributeById(Oid relid, AttrNumber attnum);
extern void RemoveAttrDefault(Oid relid, AttrNumber attnum, DropBehavior behavior, bool complain, bool internal);
extern void RemoveAttrDefaultById(Oid attrdefId);

template<char starelkind>
extern void RemoveStatistics(Oid relid, AttrNumber attnum);

extern Form_pg_attribute SystemAttributeDefinition(AttrNumber attno, bool relhasoids, bool relhasbucket, bool relhasuids);
extern Form_pg_attribute SystemAttributeByName(const char *attname, bool relhasoids);

extern int GetSysAttLength(bool hasBucketAttr = true);

extern void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind, bool allow_system_table_mods);
extern void CheckAttributeType(const char *attname, Oid atttypid, Oid attcollation, List *containing_rowtypes, bool allow_system_table_mods);

#ifdef PGXC
/* Functions related to distribution data of relations */
extern void AddRelationDistribution(const char *relname, Oid relid, DistributeBy *distributeby, 
	PGXCSubCluster *subcluster, List *parentOids, TupleDesc descriptor, bool isinstallationgroup,
        bool isbucket = false, int bucketmaplen = 0);
extern void GetRelationDistributionItems(Oid relid, DistributeBy *distributeby, TupleDesc descriptor, char *locatortype,
    int *hashalgorithm, int *hashbuckets, AttrNumber *attnum);
extern HashBucketInfo *GetRelationBucketInfo(DistributeBy *distributeby, TupleDesc tupledsc, 
    bool *createbucket, bool enable_createbucket);
extern void TryReuseIndex(Oid oldId, IndexStmt *stmt);
extern void tryReusePartedIndex(Oid oldId, IndexStmt *stmt, Relation rel);
extern Oid *GetRelationDistributionNodes(PGXCSubCluster *subcluster, int *numnodes);
extern Oid *BuildRelationDistributionNodes(List *nodes, int *numnodes);
extern Oid *SortRelationDistributionNodes(Oid *nodeoids, int numnodes);
#endif

extern void SetRelHasClusterKey(Relation rel, bool has);
extern int2vector* buildPartitionKey(List *keys, TupleDesc tupledsc);
/* 
 * @hdfs
 * Check the constraint from pg_constraint.
 */
extern bool FindExistingConstraint(const char *ccname, Relation rel);
/**
 * @Description: Build the column map. Store the column number using
 * bitmap method.
 * @in tuple_desc, A tuple descriptor.
 * @return reutrn the column map.
 */ 
extern char* make_column_map(TupleDesc tuple_desc);

/**
 * @Description: check whether the partition keys has timestampwithzone type.
 * @input: partTableRel, the partition table relation.
 * @return: a bool array to indicate the result. The length of array is equal to the number of partition keys.
 * @Notes: remember to pfree the array.
 */
extern bool* CheckPartkeyHasTimestampwithzone(Relation partTableRel, bool isForSubPartition = false);

extern Oid AddNewIntervalPartition(Relation rel, void* insertTuple, bool isDDL = false);

extern int GetIndexKeyAttsByTuple(Relation relation, HeapTuple indexTuple);

extern void AddOrDropUidsAttr(Oid relOid, bool oldRelHasUids, bool newRelHasUids);
#endif   /* HEAP_H */
