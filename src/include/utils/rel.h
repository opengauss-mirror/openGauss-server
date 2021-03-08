/* -------------------------------------------------------------------------
 *
 * rel.h
 *	  POSTGRES relation descriptor (a/k/a relcache entry) definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/utils/rel.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef REL_H
#define REL_H

#include "access/tupdesc.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"


#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#ifdef PGXC
#include "pgxc/locator.h"
#endif
#include "rewrite/prs2lock.h"
#include "storage/buf/block.h"
#include "storage/relfilenode.h"
#include "tcop/stmt_retry.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/reltrigger.h"
#include "utils/partitionmap.h"
#include "catalog/pg_hashbucket_fn.h"


#ifndef HDFS
#define HDFS "hdfs"
#endif
#ifndef OBS
#define OBS "obs"
#endif

/*
 * LockRelId and LockInfo really belong to lmgr.h, but it's more convenient
 * to declare them here so we can have a LockInfoData field in a Relation.
 */

typedef struct LockRelId {
    Oid relId; /* a relation identifier */
    Oid dbId;  /* a database identifier */
    Oid bktId; /* a bucket identifier = bucketid + 1 */
} LockRelId;

typedef struct LockInfoData {
    LockRelId lockRelId;
} LockInfoData;

typedef LockInfoData* LockInfo;
#define InvalidLockRelId  { InvalidOid, InvalidOid, InvalidOid}
#define LockRelIdIsInvalid(__lockrelid) (((__lockrelid).relId == InvalidOid) && ((__lockrelid).dbId == InvalidOid))

/*
 * Cached lookup information for the index access method functions defined
 * by the pg_am row associated with an index relation.
 */
typedef struct RelationAmInfo {
    FmgrInfo aminsert;
    FmgrInfo ambeginscan;
    FmgrInfo amgettuple;
    FmgrInfo amgetbitmap;
    FmgrInfo amrescan;
    FmgrInfo amendscan;
    FmgrInfo ammarkpos;
    FmgrInfo amrestrpos;
    FmgrInfo ammerge;
    FmgrInfo ambuild;
    FmgrInfo ambuildempty;
    FmgrInfo ambulkdelete;
    FmgrInfo amvacuumcleanup;
    FmgrInfo amcanreturn;
    FmgrInfo amcostestimate;
    FmgrInfo amoptions;
} RelationAmInfo;

//describe bucket info of hash Bucketed-Table
typedef  struct  RelationBucketKey
{
    int2vector  *bucketKey;			/*bucket key*/
    Oid         *bucketKeyType;		/*the data type of partition key*/
}RelationBucketKey;

/*
 * Here are the contents of a relation cache entry.
 */

typedef struct RelationData {

    RelFileNode rd_node; /* relation physical identifier */
    /* use "struct" here to avoid needing to include smgr.h: */
    struct SMgrRelationData* rd_smgr; /* cached file handle, or NULL */
    int rd_refcnt;                    /* reference count */
    BackendId rd_backend;             /* owning backend id, if temporary relation */
    bool rd_isscannable;              /* rel can be scanned */
    bool rd_isnailed;                 /* rel is nailed in cache */
    bool rd_isvalid;                  /* relcache entry is valid */
    char rd_indexvalid;               /* state of rd_indexlist: 0 = not valid, 1 =
                                       * valid, 2 = temporarily forced */
    bool rd_islocaltemp;              /* rel is a temp rel of this session */

    /*
     * rd_createSubid is the ID of the highest subtransaction the rel has
     * survived into; or zero if the rel was not created in the current top
     * transaction.  This should be relied on only for optimization purposes;
     * it is possible for new-ness to be "forgotten" (eg, after CLUSTER).
     * Likewise, rd_newRelfilenodeSubid is the ID of the highest
     * subtransaction the relfilenode change has survived into, or zero if not
     * changed in the current transaction (or we have forgotten changing it).
     */
    SubTransactionId rd_createSubid;         /* rel was created in current xact */
    SubTransactionId rd_newRelfilenodeSubid; /* new relfilenode assigned in
                                              * current xact */

    Form_pg_class rd_rel; /* RELATION tuple */
    TupleDesc rd_att;     /* tuple descriptor */
    Oid rd_id;            /* relation's object id */

    LockInfoData rd_lockInfo;  /* lock mgr's info for locking relation */
    RuleLock* rd_rules;        /* rewrite rules */
    MemoryContext rd_rulescxt; /* private memory cxt for rd_rules, if any */
    TriggerDesc* trigdesc;     /* Trigger info, or NULL if rel has none */
    /* use "struct" here to avoid needing to include rewriteRlsPolicy.h */
    struct RlsPoliciesDesc* rd_rlsdesc; /* Row level security policies, or NULL */
    /* data managed by RelationGetIndexList: */
    List* rd_indexlist; /* list of OIDs of indexes on relation */
    Oid rd_oidindex;    /* OID of unique index on OID, if any */
    Oid rd_refSynOid;   /* OID of referenced synonym Oid, if mapping indeed. */

    /* data managed by RelationGetIndexAttrBitmap: */
    Bitmapset* rd_indexattr; /* identifies columns used in indexes */
    Bitmapset* rd_idattr;    /* included in replica identity index */

    /*
     * The index chosen as the relation's replication identity or
     * InvalidOid. Only set correctly if RelationGetIndexList has been
     * called/rd_indexvalid > 0.
     */
    Oid rd_replidindex;

    /*
     * rd_options is set whenever rd_rel is loaded into the relcache entry.
     * Note that you can NOT look into rd_rel for this data.  NULL means "use
     * defaults".
     */
    bytea* rd_options; /* parsed pg_class.reloptions */

    /* These are non-NULL only for an index relation: */
    Oid rd_partHeapOid;   /* partition index's partition oid */
    Form_pg_index rd_index; /* pg_index tuple describing this index */
    /* use "struct" here to avoid needing to include htup.h: */
    struct HeapTupleData* rd_indextuple; /* all of pg_index tuple */
    Form_pg_am rd_am;                    /* pg_am tuple for index's AM */
    int rd_indnkeyatts;     /* index relation's indexkey nums */
	TableAmType rd_tam_type; /*Table accessor method type*/

    /*
     * index access support info (used only for an index relation)
     *
     * Note: only default support procs for each opclass are cached, namely
     * those with lefttype and righttype equal to the opclass's opcintype. The
     * arrays are indexed by support function number, which is a sufficient
     * identifier given that restriction.
     *
     * Note: rd_amcache is available for index AMs to cache private data about
     * an index.  This must be just a cache since it may get reset at any time
     * (in particular, it will get reset by a relcache inval message for the
     * index).	If used, it must point to a single memory chunk palloc'd in
     * rd_indexcxt.  A relcache reset will include freeing that chunk and
     * setting rd_amcache = NULL.
     */
    MemoryContext rd_indexcxt; /* private memory cxt for this stuff */
    RelationAmInfo* rd_aminfo; /* lookup info for funcs found in pg_am */
    Oid* rd_opfamily;          /* OIDs of op families for each index col */
    Oid* rd_opcintype;         /* OIDs of opclass declared input data types */
    RegProcedure* rd_support;  /* OIDs of support procedures */
    FmgrInfo* rd_supportinfo;  /* lookup info for support procedures */
    int16* rd_indoption;       /* per-column AM-specific flags */
    List* rd_indexprs;         /* index expression trees, if any */
    List* rd_indpred;          /* index predicate tree, if any */
    Oid* rd_exclops;           /* OIDs of exclusion operators, if any */
    Oid* rd_exclprocs;         /* OIDs of exclusion ops' procs, if any */
    uint16* rd_exclstrats;     /* exclusion ops' strategy numbers, if any */
    void* rd_amcache;          /* available for use by index AM */
    Oid* rd_indcollation;      /* OIDs of index collations */

    /*
     * foreign-table support
     *
     * rd_fdwroutine must point to a single memory chunk palloc'd in
     * t_thrd.mem_cxt.cache_mem_cxt.	It will be freed and reset to NULL on a relcache
     * reset.
     */

    /* use "struct" here to avoid needing to include fdwapi.h: */
    struct FdwRoutine* rd_fdwroutine; /* cached function pointers, or NULL */

    /*
     * Hack for CLUSTER, rewriting ALTER TABLE, etc: when writing a new
     * version of a table, we need to make any toast pointers inserted into it
     * have the existing toast table's OID, not the OID of the transient toast
     * table.  If rd_toastoid isn't InvalidOid, it is the OID to place in
     * toast pointers inserted into this rel.  (Note it's set on the new
     * version of the main heap, not the toast table itself.)  This also
     * causes toast_save_datum() to try to preserve toast value OIDs.
     */
    Oid rd_toastoid; /* Real TOAST table's OID, or InvalidOid */
    Oid rd_bucketoid;/* bucket OID in pg_hashbucket*/

    /*bucket key info, indicating which keys are used to comoute hash value */
    RelationBucketKey *rd_bucketkey;

    /* For 1-level hash table, it points into a HashBucketMap instances;
     * For 2-level hash table, e.g. range-hash, it points into a RangePartitionMap
     * instances. */

    PartitionMap* partMap;
    Oid parentId; /*if this is construct by partitionGetRelation,this is Partition Oid,else this is InvalidOid*/
    /* use "struct" here to avoid needing to include pgstat.h: */
    struct PgStat_TableStatus* pgstat_info; /* statistics collection area */
#ifdef PGXC
    RelationLocInfo* rd_locator_info;
    PartitionMap* sliceMap;
#endif
    Relation   parent;

    /* double linked list node, partition and bucket relation would be stored in fakerels list of resource owner */
    dlist_node node;

    Oid rd_mlogoid;
} RelationData;

/*
 * StdRdOptions
 *		Standard contents of rd_options for heaps and generic indexes.
 *
 * RelationGetFillFactor() and RelationGetTargetPageFreeSpace() can only
 * be applied to relations that use this format or a superset for
 * private options data.
 */
/* autovacuum-related reloptions. */
typedef struct AutoVacOpts {
    bool enabled;
    int vacuum_threshold;
    int analyze_threshold;
    int vacuum_cost_delay;
    int vacuum_cost_limit;
    int64 freeze_min_age;
    int64 freeze_max_age;
    int64 freeze_table_age;
    float8 vacuum_scale_factor;
    float8 analyze_scale_factor;
} AutoVacOpts;

typedef enum RedisCtidType { REDIS_START_CTID = 0, REDIS_END_CTID } RedisCtidType;

typedef enum RedisRelAction {
    REDIS_REL_INVALID = -1,
    REDIS_REL_NORMAL,
    REDIS_REL_APPEND,
    REDIS_REL_READ_ONLY,
    REDIS_REL_END_CATCHUP,
    REDIS_REL_REFRESH,
    REDIS_REL_DESTINATION,
    REDIS_REL_RESET_CTID
} RedisHtlAction;

typedef struct StdRdOptions {
    int32 vl_len_;           /* varlena header (do not touch directly!) */
    int fillfactor;          /* page fill factor in percent (0..100) */
    AutoVacOpts autovacuum;  /* autovacuum-related options */
    bool security_barrier;   /* for views */
    bool enable_rowsecurity; /* enable row level security or not */
    bool force_rowsecurity;  /* force row level security or not */
    bool enable_tsdb_delta; /* enable delta table for timeseries relations */

    int tsdb_deltamerge_interval;   /* interval for tsdb delta merge job */
    int tsdb_deltamerge_threshold;   /* data threshold for tsdb delta merge job */
    int tsdb_deltainsert_threshold;   /* data threshold for tsdb delta insert */
    int max_batch_rows;            /* the upmost rows at each batch inserting */
    int delta_rows_threshold;      /* the upmost rows delta table holds */
    int partial_cluster_rows;      /* row numbers of partial cluster feature */
    int compresslevel;             /* compress level, see relation storage options 'compresslevel' */
    int internalMask;              /*internal mask*/
    bool ignore_enable_hadoop_env; /* ignore enable_hadoop_env */
    bool user_catalog_table;       /* use as an additional catalog relation */
    bool hashbucket;        /* enable hash bucket for this relation */
    bool primarynode;       /* enable primarynode mode for replication table */
    /* info for redistribution */
    Oid rel_cn_oid;
    RedisHtlAction append_mode_internal;

    // Important:
    // for string type, data is appended at the tail of its parent struct.
    // CHAR* member of this STRUCT stores the offset of its string data.
    // offset=0 means that it's a NULL string.
    //
    // Take Care !!!
    // CHAR* member CANNOT be accessed directly.
    // StdRdOptionsGetStringData macro must be used for accessing CHAR* type member.
    //
    char* compression; /* compress or not compress */
    char* table_access_method; /*table access method kind */
    char* orientation; /* row-store or column-store */
    char* ttl; /* time to live for tsdb data management */
    char* period; /* partition range for tsdb data management */
    char* partition_interval; /* partition interval for streaming contquery table */
    char* time_column; /* time column for streaming contquery table */
    char* ttl_interval; /* ttl interval for streaming contquery table */
    char* gather_interval; /* gather interval for streaming contquery table */
    char* string_optimize; /* string optimize for streaming contquery table */
    char* sw_interval; /* sliding window interval for streaming contquery table */
    char* version;
    char* wait_clean_gpi; /* pg_partition system catalog wait gpi-clean or not */
    /* item for online expand */
    char* append_mode;
    char* start_ctid_internal;
    char* end_ctid_internal;
    char        *merge_list;
    bool on_commit_delete_rows; /* global temp table */
} StdRdOptions;

#define HEAP_MIN_FILLFACTOR 10
#define HEAP_DEFAULT_FILLFACTOR 100

/*
 * RelationIsUsedAsCatalogTable
 *     Returns whether the relation should be treated as a catalog table
 *      from the pov of logical decoding.
 */
#define RelationIsUsedAsCatalogTable(relation) \
    ((relation)->rd_options ? ((StdRdOptions*)(relation)->rd_options)->user_catalog_table : false)

#define RelationIsInternal(relation) (RelationGetInternalMask(relation) != INTERNAL_MASK_DISABLE)

/*
 * RelationGetFillFactor
 *		Returns the relation's fillfactor.  Note multiple eval of argument!
 */
#define RelationGetFillFactor(relation, defaultff) \
    ((relation)->rd_options ? ((StdRdOptions*)(relation)->rd_options)->fillfactor : (defaultff))

/*
 * RelationGetTargetPageUsage
 *		Returns the relation's desired space usage per page in bytes.
 */
#define RelationGetTargetPageUsage(relation, defaultff) (BLCKSZ * RelationGetFillFactor(relation, defaultff) / 100)

/*
 * RelationGetTargetPageFreeSpace
 *		Returns the relation's desired freespace per page in bytes.
 */
#define RelationGetTargetPageFreeSpace(relation, defaultff) \
    (BLCKSZ * (100 - RelationGetFillFactor(relation, defaultff)) / 100)

/*
 * RelationIsSecurityView
 *		Returns whether the relation is security view, or not
 */
#define RelationIsSecurityView(relation) \
    ((relation)->rd_options ? ((StdRdOptions*)(relation)->rd_options)->security_barrier : false)

/*
 * RelationIsValid
 *		True iff relation descriptor is valid.
 */
#define RelationIsValid(relation) PointerIsValid(relation)

#define InvalidRelation ((Relation)NULL)

/*
 * RelationHasReferenceCountZero
 *		True iff relation reference count is zero.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationHasReferenceCountZero(relation) ((bool)((relation)->rd_refcnt == 0))

/*
 * RelationGetForm
 *		Returns pg_class tuple for a relation.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationGetForm(relation) ((relation)->rd_rel)

/*
 * RelationGetRelid
 *		Returns the OID of the relation
 */
#define RelationGetRelid(relation) ((relation)->rd_id)

#define RelationGetBktid(relation) ((relation)->rd_node.bucketNode)

/*
 * RelationGetNumberOfAttributes
 *		Returns the total number of attributes in a relation.
 */
#define RelationGetNumberOfAttributes(relation) ((relation)->rd_rel->relnatts)

/*
 * IndexRelationGetNumberOfAttributes
 *		Returns the number of attributes in an index.
 */
#define IndexRelationGetNumberOfAttributes(relation) ((relation)->rd_index->indnatts)

/*
 * IndexRelationGetNumberOfKeyAttributes
 *		Returns the number of key attributes in an index.
 */
#define IndexRelationGetNumberOfKeyAttributes(relation) \
    (AssertMacro((relation)->rd_indnkeyatts != 0), ((relation)->rd_indnkeyatts))


/*
 * RelationGetDescr
 *		Returns tuple descriptor for a relation.
 */
#define RelationGetDescr(relation) ((relation)->rd_att)

/*
 * RelationGetRelationName
 *      Returns the rel's name.
 *
 * Note that the name is only unique within the containing namespace.
 */
#define RelationGetRelationName(relation) (NameStr((relation)->rd_rel->relname))

#define PartitionGetPartitionName(partition) (NameStr((partition)->pd_part->relname))

#define RelationGetPartType(relation) ((relation)->rd_rel->parttype)

/*
 * RelationGetNamespace
 *		Returns the rel's namespace OID.
 */
#define RelationGetNamespace(relation) ((relation)->rd_rel->relnamespace)

/*
 * RelationIsMapped
 *		True if the relation uses the relfilenode map.
 *
 * NB: this is only meaningful for relkinds that have storage, else it
 * will misleadingly say "true".
 */
#define RelationIsMapped(relation) ((relation)->rd_rel->relfilenode == InvalidOid)

/*
 * RelationOpenSmgr
 *		Open the relation at the smgr level, if not already done.
 */
#define RelationOpenSmgr(relation)                                                                       \
    do {                                                                                                 \
        if ((relation)->rd_smgr == NULL) {                                                               \
            oidvector* bucketlist = NULL;                                                                \
            if (RELATION_CREATE_BUCKET(relation)) {                                                      \
                bucketlist = searchHashBucketByOid(relation->rd_bucketoid);                        \
            }                                                                                            \
            smgrsetowner(&((relation)->rd_smgr), smgropen((relation)->rd_node, (relation)->rd_backend, 0, bucketlist)); \
            }                                                                                             \
    } while (0)

/*
 * RelationCloseSmgr
 *		Close the relation at the smgr level, if not already done.
 *
 * Note: smgrclose should unhook from owner pointer, hence the Assert.
 */
#define RelationCloseSmgr(relation)              \
    do {                                         \
        if ((relation)->rd_smgr != NULL) {       \
            smgrclose((relation)->rd_smgr);      \
            Assert(RelationIsBucket(relation) || (relation)->rd_smgr == NULL); \
        }                                        \
    } while (0)

/*
 * RelationGetTargetBlock
 *		Fetch relation's current insertion target block.
 *
 * Returns InvalidBlockNumber if there is no current target block.	Note
 * that the target block status is discarded on any smgr-level invalidation.
 */
#define RelationGetTargetBlock(relation) \
    ((relation)->rd_smgr != NULL ? (relation)->rd_smgr->smgr_targblock : InvalidBlockNumber)

/*
 * RelationSetTargetBlock
 *		Set relation's current insertion target block.
 */
#define RelationSetTargetBlock(relation, targblock)        \
    do {                                                   \
        RelationOpenSmgr(relation);                        \
        (relation)->rd_smgr->smgr_targblock = (targblock); \
    } while (0)

/*
 * RelationNeedsWAL
 *		True if relation needs WAL.
 */
#define RelationNeedsWAL(relation)                                     \
    ((relation)->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT || \
        (((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP) && STMT_RETRY_ENABLED))

/*
 * RelationUsesLocalBuffers
 *		True if relation's pages are stored in local buffers.
 */
#define RelationUsesLocalBuffers(relation) \
    ((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP || \
     (relation)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
#define RelationIsLocalTemp(relation)                                           \
    ((relation)->rd_rel->relnamespace == u_sess->catalog_cxt.myTempNamespace || \
        (relation)->rd_rel->relnamespace == u_sess->catalog_cxt.myTempToastNamespace)

#define RelationIsRelation(relation) (RELKIND_RELATION == (relation)->rd_rel->relkind)

#define isPartitionedRelation(classForm) (PARTTYPE_PARTITIONED_RELATION == (classForm)->parttype)

#ifdef PGXC
/*
 * RelationGetLocInfo
 *		Return the location info of relation
 */
#define RelationGetLocInfo(relation) ((relation)->rd_locator_info)
#endif

/*
 * RelationGetBucketKey
 *      Fetch relation's current bucket key.
 *
 * Returns NULL if there is no hash bucket in current Relation
 */
#define RelationGetBucketKey(relation) \
    ((relation)->rd_bucketkey != NULL) ? (relation)->rd_bucketkey->bucketKey : NULL

/*
 * RELATION_IS_LOCAL
 *		If a rel is either local temp or global temp relation
 *		or newly created in the current transaction,
 *		it can be assumed to be accessible only to the current backend.
 *		This is typically used to decide that we can skip acquiring locks.
 *
 * Beware of multiple eval of argument
 */
#define RELATION_IS_LOCAL(relation) \
    ((relation)->rd_islocaltemp || \
     (relation)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP || \
     (relation)->rd_createSubid != InvalidSubTransactionId)

/*
 * RELATION_IS_TEMP
 *        Test a rel is either local temp relation of this session
 *         or global temp relation.
 */
#define RELATION_IS_TEMP(relation) \
    ((relation)->rd_islocaltemp || \
     (relation)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)

/* global temp table implementations */
#define RELATION_IS_GLOBAL_TEMP(relation) \
    ((relation) != NULL && (relation)->rd_rel != NULL && \
     (relation)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)

#define RELATION_GTT_ON_COMMIT_DELETE(relation)    \
    ((relation)->rd_options && (relation)->rd_rel->relkind == RELKIND_RELATION && \
    (relation)->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP ? \
    (reinterpret_cast<StdRdOptions *>((relation)->rd_options))->on_commit_delete_rows : false)

#define RelationGetRelPersistence(relation) ((relation)->rd_rel->relpersistence)

/* routines in utils/cache/relcache.c */
extern void RelationIncrementReferenceCount(Relation rel);
extern void RelationDecrementReferenceCount(Relation rel);
extern void RelationIncrementReferenceCount(Oid relationId);
extern void RelationDecrementReferenceCount(Oid relationId);

/*
 * RelationIsAccessibleInLogicalDecoding
 *     True if we need to log enough information to have access via
 *     decoding snapshot.
 */
#define RelationIsAccessibleInLogicalDecoding(relation)       \
    (XLogLogicalInfoActive() && RelationNeedsWAL(relation) && \
        (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation)))

/*
 * RelationIsLogicallyLogged
 *     True if we need to log enough information to extract the data from the
 *     WAL stream.
 *
 * We don't log information for unlogged tables (since they don't WAL log
 * anyway) and for system tables (their content is hard to make sense of, and
 * it would complicate decoding slightly for little gain). Note that we *do*
 * log information for user defined catalog tables since they presumably are
 * interesting to the user...
 */
#define RelationIsLogicallyLogged(relation) \
    (XLogLogicalInfoActive() && RelationNeedsWAL(relation) && !IsCatalogRelation(relation))

#endif /* REL_H */

