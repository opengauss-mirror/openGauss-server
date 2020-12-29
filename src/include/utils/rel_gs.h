/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * rel_gs.h
 *        POSTGRES relation descriptor (a/k/a relcache entry) definitions.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/rel_gs.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef REL_GS_H
#define REL_GS_H

/*
 * Relation Compression Type
 * REL_CMPRS_NOT_SUPPORT:
 *     relation doesn't support compression. That means these relations have constant
 *     commpression attr. 'ALTER TABLE ... SET COMPRESS/UNCOMPRESS' mustn't applies
 *     to these relations. For examples, system relations, index, etc.
 * REL_CMPRS_PAGE_PLAIN:
 *     relation support compression with 'CREATE/ALTER TABLE ... [SET] COMPRESS/UNCOMPRESS' clause.
 *     Under this condition, all the data in this relation are stored in plain format.
 * REL_CMPRS_PAGE_DICT:
 *     relation support compression with 'CREATE/ALTER TABLE ... [SET] COMPRESS/UNCOMPRESS' clause.
 *     Under this condition, many tuples of one page in this relation are stored after compressing.
 * REL_CMPRS_MAX_TYPE:
 *     The other types must be defined above this value.
 */
typedef enum {
    REL_CMPRS_NOT_SUPPORT = 0,
    REL_CMPRS_PAGE_PLAIN,
    REL_CMPRS_FIELDS_EXTRACT,
    REL_CMPRS_MAX_TYPE
} RelCompressType;

#ifndef FRONTEND_PARSER
#include "catalog/pg_partition.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "utils/partitionmap_gs.h"
#include "rel.h"

#define CHECK_CMPRS_VALID(compress) ((compress) > REL_CMPRS_NOT_SUPPORT && (compress) < REL_CMPRS_MAX_TYPE)
#define CHECK_CMPRS_NOT_SUPPORT(compress) (REL_CMPRS_NOT_SUPPORT == (compress))

typedef struct PartitionData {
    RelFileNode pd_node; /* relation physical identifier */
    /* use "struct" here to avoid needing to include smgr.h: */
    struct SMgrRelationData* pd_smgr; /* cached file handle, or NULL */
    int pd_refcnt;                    /* reference count */
    bool pd_isvalid;                  /* partcache entry is valid */
    char pd_indexvalid;               /* state of pd_indexlist: 0 = not valid, 1 =
                                       * valid, 2 = temporarily forced */

    /*
     * rd_createSubid is the ID of the highest subtransaction the rel has
     * survived into; or zero if the rel was not created in the current top
     * transaction.  This should be relied on only for optimization purposes;
     * it is possible for new-ness to be "forgotten" (eg, after CLUSTER).
     * Likewise, rd_newRelfilenodeSubid is the ID of the highest
     * subtransaction the relfilenode change has survived into, or zero if not
     * changed in the current transaction (or we have forgotten changing it).
     */
    SubTransactionId pd_createSubid;         /* rel was created in current xact */
    SubTransactionId pd_newRelfilenodeSubid; /* new relfilenode assigned in
                                              * current xact */

    Form_pg_partition pd_part;                                           /* PARTITION tuple */
    Oid pd_id;                                                           /* partition's object id */
    List* pd_indexlist;                                                  /* list of OIDs of indexes on partition */
    Bitmapset* pd_indexattr;                                             /* identifies columns used in indexes */
    Oid pd_oidindex;                                                     /* OID of unique index on OID, if any */
    LockInfoData pd_lockInfo; /* lock mgr's info for locking relation */ /*?*/
    /*
     * Hack for CLUSTER, rewriting ALTER TABLE, etc: when writing a new
     * version of a table, we need to make any toast pointers inserted into it
     * have the existing toast table's OID, not the OID of the transient toast
     * table.  If rd_toastoid isn't InvalidOid, it is the OID to place in
     * toast pointers inserted into this rel.  (Note it's set on the new
     * version of the main heap, not the toast table itself.)  This also
     * causes toast_save_datum() to try to preserve toast value OIDs.
     */
    Oid pd_toastoid; /* Real TOAST table's OID from pg_partition, or InvalidOid */
    /* use "struct" here to avoid needing to include pgstat.h: */
    bytea* rd_options;                         /* partition's reloptions */
    struct PgStat_TableStatus* pd_pgstat_info; /* statistics collection area */
    Partition  parent;
} PartitionData;

typedef struct AttrMetaData {
    NodeTag type;

    char* attname; /* name of attribute */
    Oid atttypid;
    int2 attlen;
    int2 attnum;
    int4 atttypmod;
    bool attbyval;
    char attstorage;
    char attalign;
    bool attnotnull;
    bool atthasdef;
    bool attisdropped;
    bool attislocal;
    int1 attkvtype;
    int1 attcmprmode;
    int4 attinhcount;
    Oid attcollation;
} AttrMetaData;

typedef struct RelationMetaData {
    NodeTag type;

    Oid rd_id; /* relation's object id */

    // RelFileNode rd_node;		/* relation physical identifier */
    Oid spcNode; /* tablespace */
    Oid dbNode;  /* database */
    Oid relNode; /* relation */
    int4 bucketNode; /* bucket */

    // Form_pg_class rd_rel;		/* RELATION tuple */
    char* relname; /* class name */
    char relkind;  /* see RELKIND_xxx constants below */
    char parttype; /* 'p' for  partitioned relation, 'n' for non-partitioned relation */

    // TupleDesc	rd_att;			/* tuple descriptor */
    int natts;   /* number of attributes in the tuple */
    List* attrs; /* the list of AttrMetaData */
} RelationMetaData;

#define ORIENTATION_ROW "row"
#define ORIENTATION_COLUMN "column"
#define ORIENTATION_ORC "orc"
#define ORIENTATION_TIMESERIES "timeseries"

#define TIME_ONE_HOUR "1 HOUR"
#define TIME_ONE_DAY "1 DAY"
#define TIME_ONE_WEEK "1 WEEK"
#define TIME_ONE_MONTH "1 MONTH"
#define TIME_ONE_YEAR "1 YEAR"
#define TIME_UNDEFINED "UNDEFINED"

#define COLUMN_UNDEFINED "UNDEFINED"

#define ORC_VERSION_011 "0.11"
#define ORC_VERSION_012 "0.12"

#define COMPRESSION_NO "no"
#define COMPRESSION_YES "yes"
#define COMPRESSION_LOW "low"
#define COMPRESSION_MIDDLE "middle"
#define COMPRESSION_HIGH "high"
#define COMPRESSION_ZLIB "zlib"
#define COMPRESSION_SNAPPY "snappy"
#define COMPRESSION_LZ4 "lz4"

/*
 * values for different table access method types.
 */
#define TABLE_ACCESS_METHOD_HEAP "HEAP"
#define TABLE_ACCESS_METHOD_USTORE "USTORE"

#define FILESYSTEM_GENERAL "general"
#define FILESYSTEM_HDFS "hdfs"

#define OptIgnoreEnableHadoopEnv "ignore_enable_hadoop_env"
#define TS_PSEUDO_DIST_COLUMN "ts_pseudo_distcol"

#define OptEnabledWaitCleanGpi "y"
#define OptDisabledWaitCleanGpi "n"

#define INTERNAL_MASK_DISABLE 0x0
#define INTERNAL_MASK_ENABLE 0x8000
#define INTERNAL_MASK_DINSERT 0x01    // disable insert
#define INTERNAL_MASK_DDELETE 0x02    // disable delete
#define INTERNAL_MASK_DALTER 0x04     // disable alter
#define INTERNAL_MASK_DSELECT 0x08    // disable select
#define INTERNAL_MASK_DUPDATE 0x0100  // disable update

#define StdRdOptionsGetStringData(_basePtr, _memberName, _defaultVal)                    \
    (((_basePtr) && (((StdRdOptions*)(_basePtr))->_memberName))                          \
            ? (((char*)(_basePtr) + *(int*)&(((StdRdOptions*)(_basePtr))->_memberName))) \
            : (_defaultVal))

/*
 * ReltionGetTableAccessMethoType
 * 	Returns the relations TableAmType
 */
#define RelationGetTableAccessMethodType(_reloptions) \
    StdRdOptionsGetStringData(_reloptions, table_access_method, TABLE_ACCESS_METHOD_HEAP)

#define RelationIsTableAccessMethodHeapType(_reloptions) \
    pg_strcasecmp(RelationGetTableAccessMethodType(_reloptions), TABLE_ACCESS_METHOD_HEAP) == 0

#define RelationIsTableAccessMethodUStoreType(_reloptions) \
    pg_strcasecmp(RelationGetTableAccessMethodType(_reloptions), TABLE_ACCESS_METHOD_USTORE) == 0


/*
 * @Description: get TableAmType type from Relation's reloptions data.
 * @IN reloptions: Table's reloptions.
 * @Return: return TableAmType type for this relation.
 */
static inline TableAmType get_tableam_from_reloptions(bytea* reloptions, char relkind)
{
    if (relkind == RELKIND_RELATION)
    {
        if (reloptions!= NULL)
        {
            if (RelationIsTableAccessMethodHeapType(reloptions))
            {
                return TAM_HEAP;
            }
            else if (RelationIsTableAccessMethodUStoreType(reloptions))
            {
                 return TAM_USTORE;
            }

            return TAM_HEAP;
        }
        else //For System Tables reloptions can be NULL.
        {
            return TAM_HEAP;
        }
    }
    else if (relkind == RELKIND_INDEX ||
            relkind == RELKIND_TOASTVALUE ||
            relkind == RELKIND_SEQUENCE ||
            relkind == RELKIND_COMPOSITE_TYPE ||
            relkind == RELKIND_VIEW ||
            relkind == RELKIND_FOREIGN_TABLE)
    {
        return TAM_HEAP;
    }

    return TAM_HEAP;
}

/* RelationGetOrientation
 *    Return the relations' orientation
 */
#define RelationGetOrientation(relation) StdRdOptionsGetStringData((relation)->rd_options, orientation, ORIENTATION_ROW)

#define RaletionGetStoreVersion(relation) StdRdOptionsGetStringData((relation)->rd_options, version, ORC_VERSION_012)

#define RelationGetFileSystem(relation) \
    StdRdOptionsGetStringData((relation)->rd_options, filesystem, FILESYSTEM_GENERAL)

#define RelationIsCUFormat(relation)                    \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && \
        pg_strcasecmp(RelationGetOrientation(relation), ORIENTATION_COLUMN) == 0)

#define RelationIsRowFormat(relation)                   \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && \
        pg_strcasecmp(RelationGetOrientation(relation), ORIENTATION_ROW) == 0)

#define RelationIsColumnFormat(relation) \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && (RelationIsCUFormat(relation) || RelationIsPAXFormat(relation)))

#define RelationGetCUDescRelId(r) ((r)->rd_rel->relcudescrelid)
#define RelationGetDeltaRelId(r) ((r)->rd_rel->reldeltarelid)
#define RelationGetRelFileNode(r) ((r)->rd_rel->relfilenode)

/*
 * @Description: get columnar relation's compress level.
 * @IN rel: columnar heap relation
 * @Return: return user explictly defined compresslevel,
 *          or return compresslevel zero.
 * @See also:
 */
static inline int relation_get_compresslevel(Relation rel)
{
    Assert(rel != NULL);
    if ((RELKIND_RELATION == rel->rd_rel->relkind) && rel->rd_options) {
        return ((StdRdOptions*)rel->rd_options)->compresslevel;
    }
    /* Otherwise just return zero */
    return 0;
}

static inline int RelationGetInternalMask(Relation rel)
{
    /* Now internal mask is only available for ordinary relation */
    if ((RELKIND_RELATION == rel->rd_rel->relkind) && rel->rd_options) {
        return ((StdRdOptions*)rel->rd_options)->internalMask;
    } else {
        return INTERNAL_MASK_DISABLE;
    }
}

#define RelationIsInternal(relation) (RelationGetInternalMask(relation) != INTERNAL_MASK_DISABLE)

#define RelationIsRelation(relation) (RELKIND_RELATION == (relation)->rd_rel->relkind)

#define RelationIsMatview(relation) (RELKIND_MATVIEW == (relation)->rd_rel->relkind)

/*
 *	Mgr for Redistribute
 */
static inline RedisHtlAction RelationGetAppendMode(Relation rel)
{
    Assert(rel != NULL);
    if (RelationIsRelation(rel) && rel->rd_options) {
        return ((StdRdOptions*)rel->rd_options)->append_mode_internal;
    }

    /* Otherwise just return zero */
    return REDIS_REL_NORMAL;
}

/*
 * RelationIsPAXFormat
 * Return the relations' orientation. Pax format includes ORC format.
 */
#define RelationIsPAXFormat(relation)                   \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && \
        pg_strcasecmp(RelationGetOrientation(relation), ORIENTATION_ORC) == 0)

/* RelationIsColStore
 * 	  Return relation whether is column store, which includes CU format and PAX format.
 */
#define RelationIsColStore(relation) \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && (RelationIsCUFormat(relation) || RelationIsPAXFormat(relation)))

#define RelationOptionIsDfsStore(optionValue) (optionValue && 0 == pg_strncasecmp(optionValue, HDFS, strlen(HDFS)))

#define RelationIsTsStore(relation) \
    ((RELKIND_RELATION == relation->rd_rel->relkind) && \
        pg_strcasecmp(RelationGetOrientation(relation), ORIENTATION_TIMESERIES) == 0)

#define TsRelWithImplDistColumn(attribute, pos)     \
    (((attribute)[pos]->attkvtype == ATT_KV_HIDE) &&  \
        namestrcmp(&((attribute)[pos]->attname), TS_PSEUDO_DIST_COLUMN) == 0)

// Helper Macro Defination
//
#define StdRelOptGetOrientation(__stdRelOpt) StdRdOptionsGetStringData((__stdRelOpt), orientation, ORIENTATION_ROW)

#define StdRelOptIsColStore(__stdRelOpt) (0 == pg_strcasecmp(ORIENTATION_COLUMN, StdRelOptGetOrientation(__stdRelOpt)))

#define StdRelOptIsRowStore(__stdRelOpt) (0 == pg_strcasecmp(ORIENTATION_ROW, StdRelOptGetOrientation(__stdRelOpt)))

#define StdRelOptIsTsStore(__stdRelOpt) (0 == pg_strcasecmp(ORIENTATION_TIMESERIES,  \
                                                        StdRelOptGetOrientation(__stdRelOpt)))


/* get internal_mask option from reloption */
#define StdRelOptGetInternalMask(__stdRelOpt) StdRdOptionsGetStringData((__stdRelOpt), internalMask, 0)

// RelationGetCompression
//    Return the relation's compression option
//
#define RelationGetCompression(relation) StdRdOptionsGetStringData((relation)->rd_options, compression, COMPRESSION_LOW)

// make sure that:
// 1. RelDefaultFullCuSize = N * BatchMaxSize
// 2. RelDefaultPartialClusterRows = M * RelDefaultFullCuSize
//
#define RelMaxFullCuSize (60 * BatchMaxSize)
#define RelDefaultFullCuSize RelMaxFullCuSize
#define RelDefaultPartialClusterRows (70 * RelDefaultFullCuSize)
#define RelDefaultDletaRows 100

/* the min/max values about compress-level option */
#define REL_MIN_COMPRESSLEVEL (0)
#define REL_MAX_COMPRESSLEVEL (3)

#define RelRoundIntOption(_v1, _v2) (AssertMacro((_v1) >= (_v2)), ((_v2) * ((_v1) / (_v2))))

// RelationGetMaxBatchRows
//    Return the relation's max_batch_rows option
//
#define RelationGetMaxBatchRows(relation)                                                                              \
    ((relation)->rd_options ? RelRoundIntOption(((StdRdOptions*)(relation)->rd_options)->max_batch_rows, BatchMaxSize) \
                            : RelDefaultFullCuSize)

// RelationGetDeltaRowsThreshold
//    Return the relation's delta_rows_threshold option
//
#define RelationGetDeltaRowsThreshold(relation) \
    ((relation)->rd_options ? ((StdRdOptions*)(relation)->rd_options)->delta_rows_threshold : RelDefaultDletaRows)

// RelationGetPartialClusterRows
//    Return the relation's partial_cluster_rows option(return max value for -1)
//
#define RelationGetPartialClusterRows(relation)                                                          \
    ((relation)->rd_options                                                                              \
            ? ((((StdRdOptions*)(relation)->rd_options)->partial_cluster_rows < 0)                       \
                      ? RelRoundIntOption(0x7fffffff, RelationGetMaxBatchRows(relation))                 \
                      : RelRoundIntOption(((StdRdOptions*)(relation)->rd_options)->partial_cluster_rows, \
                            RelationGetMaxBatchRows(relation)))                                          \
            : RelDefaultPartialClusterRows)

/* Relation whether create in current xact */
static inline bool RelationCreateInCurrXact(Relation rel)
{
    return rel->rd_createSubid != InvalidTransactionId;
}

/* RelationGetPgClassOid
 *    return the pg_class OID of this relation.
 *    if this is a PARTITION, return its parent oid;
 *    otherwise the same to macro RelationGetRelid().
 */
#define RelationGetPgClassOid(relation, isPartition) \
    ((isPartition) ? ((relation)->parentId) : RelationGetRelid(relation))

/*
 * PartitionOpenSmgr
 *		Open the partition at the smgr level, if not already done.
 */
#define PartitionOpenSmgr(partition)                                                                 \
    do {                                                                                             \
        if ((partition)->pd_smgr == NULL)                                                            \
            smgrsetowner(&((partition)->pd_smgr), smgropen((partition)->pd_node, InvalidBackendId)); \
    } while (0)


/*
 * PartionCloseSmgr
 *		Close the partition at the smgr level, if not already done.
 *
 * Note: smgrclose should unhook from owner pointer, hence the Assert.
 */
#define PartitionCloseSmgr(partition)             \
    do {                                          \
        if ((partition)->pd_smgr != NULL) {       \
            smgrclose((partition)->pd_smgr);      \
            Assert(PartitionIsBucket(partition) || (partition)->pd_smgr == NULL); \
        }                                         \
    } while (0)

/*
 * PartitionGetTargetBlock
 *		Fetch partition's current insertion target block.
 *
 * Returns InvalidBlockNumber if there is no current target block.	Note
 * that the target block status is discarded on any smgr-level invalidation.
 */
#define PartitionGetTargetBlock(partition) \
    ((partition)->pd_smgr != NULL ? (partition)->pd_smgr->smgr_targblock : InvalidBlockNumber)
/*
 * PartitionSetTargetBlock
 *		Set relation's current insertion target block.
 */
/*
 * PartitionGetPartitionName
 *		Returns the part's name.
 *
 * Note that the name is only unique within the containing namespace.
 */
#define PartitionGetPartitionName(partition) (NameStr((partition)->pd_part->relname))
/*
 * PartitionGetPartid
 *		Returns the OID of the partition
 */
#define PartitionGetPartid(partition) ((partition)->pd_id)

#define PartitionGetRelid(partition) ((partition)->pd_part->parentid)

/*
 * PartitionIsValid
 *		True iff partition descriptor is valid.
 */
#define PartitionIsValid(partition) PointerIsValid(partition)

/*
 * true if the relation is construct from a partition
 */
#define RelationIsPartition(relation)                                                                           \
    (OidIsValid((relation)->parentId) && (PARTTYPE_NON_PARTITIONED_RELATION == (relation)->rd_rel->parttype) && \
        ((RELKIND_RELATION == (relation)->rd_rel->relkind) || (RELKIND_INDEX == (relation)->rd_rel->relkind)))

#define InvalidPartition ((Partition)NULL)

#define InvalidBktId   (-1)
#define RelationIsBucket(relation) \
    ((relation)->rd_node.bucketNode > InvalidBktId)

/*
 * PartitionHasReferenceCountZero
 *		True iff partition reference count is zero.
 *
 * Note:
 *		Assumes partition descriptor is valid.
 */
#define PartitionHasReferenceCountZero(partition) ((bool)((partition)->pd_refcnt == 0))
/* routines in utils/cache/partcache.c */
extern void PartitionIncrementReferenceCount(Partition part);
extern void PartitionDecrementReferenceCount(Partition part);

#define PartitionIsMapped(part) ((part)->pd_part->relfilenode == InvalidOid)

#define CStoreRelationOpenSmgr(relation, col)                                                                   \
    do {                                                                                                        \
        if ((relation)->rd_smgr == NULL)                                                                        \
            smgrsetowner(&((relation)->rd_smgr), smgropen((relation)->rd_node, (relation)->rd_backend, (col))); \
        else {                                                                                                  \
            SMgrRelation tmp;                                                                                   \
            tmp = smgropen((relation)->rd_node, (relation)->rd_backend, (col));                                 \
            Assert((relation)->rd_smgr == tmp);                                                                 \
        }                                                                                                       \
    } while (0)

#define RelationIsLocalTemp(relation)                                           \
    ((relation)->rd_rel->relnamespace == u_sess->catalog_cxt.myTempNamespace || \
        (relation)->rd_rel->relnamespace == u_sess->catalog_cxt.myTempToastNamespace)

#define RelationIsUnlogged(relation) ((relation)->rd_rel->relnamespace == RELPERSISTENCE_UNLOGGED)

#define RelationIsGlobalIndex(relation) (RELKIND_GLOBAL_INDEX == (relation)->rd_rel->relkind)

#define RelationIsIndex(relation) (RELKIND_INDEX == (relation)->rd_rel->relkind || RelationIsGlobalIndex(relation))

#define RelationIsSequnce(relation) (RELKIND_SEQUENCE == (relation)->rd_rel->relkind)

#define RelationIsToast(relation) (RELKIND_TOASTVALUE == (relation)->rd_rel->relkind)

#define RelationIsView(relation) (RELKIND_VIEW == (relation)->rd_rel->relkind)

#define RelationIsContquery(relation) (RELKIND_CONTQUERY == (relation)->rd_rel->relkind)

#define RelationIsForeignTable(relation) (RELKIND_FOREIGN_TABLE == (relation)->rd_rel->relkind)

#define RelationIsStream(relation) (RELKIND_STREAM == (relation)->rd_rel->relkind)

#define RelationIsUnCataloged(relation) (RELKIND_UNCATALOGED == (relation)->rd_rel->relkind)

#define RelationIsPartitioned(relation) (PARTTYPE_PARTITIONED_RELATION == (relation)->rd_rel->parttype)
#define RelationIsRangePartitioned(relation) (PARTTYPE_PARTITIONED_RELATION == (relation)->rd_rel->parttype)

#define RelationIsValuePartitioned(relation) (PARTTYPE_VALUE_PARTITIONED_RELATION == (relation)->rd_rel->parttype)

#define RelationGetValuePartitionList(relation) \
    (RelationIsValuePartitioned(relation) ? ((ValuePartitionMap*)relation->partMap)->partList : NIL)

#define RelationIsNonpartitioned(relation) (PARTTYPE_NON_PARTITIONED_RELATION == (relation)->rd_rel->parttype)

/*
 * RELATION_IS_OTHER_TEMP
 *		Test for a temporary relation that belongs to some other session.
 *
 * Beware of multiple eval of argument
 */
#define RELATION_IS_OTHER_TEMP(relation)                                                                      \
    (STMT_RETRY_ENABLED ? ((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&                       \
                              u_sess->catalog_cxt.myTempNamespace != (relation)->rd_rel->relnamespace &&      \
                              u_sess->catalog_cxt.myTempToastNamespace != (relation)->rd_rel->relnamespace && \
                              CSTORE_NAMESPACE != (relation)->rd_rel->relnamespace &&                         \
                              PG_TOAST_NAMESPACE != (relation)->rd_rel->relnamespace)                         \
                        : ((relation)->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&                       \
                              u_sess->catalog_cxt.myTempNamespace != (relation)->rd_rel->relnamespace &&      \
                              u_sess->catalog_cxt.myTempToastNamespace != (relation)->rd_rel->relnamespace))

#define RELATION_IS_PARTITIONED(relation)                               \
    ((PARTTYPE_PARTITIONED_RELATION == (relation)->rd_rel->parttype) && \
        (RELKIND_RELATION == (relation)->rd_rel->relkind))

#define RELATION_IS_VALUE_PARTITIONED(relation)                               \
    ((PARTTYPE_VALUE_PARTITIONED_RELATION == (relation)->rd_rel->parttype) && \
        (RELKIND_RELATION == (relation)->rd_rel->relkind))

/* 	
 *   type  bucketOid	 bucketKey	   meaning
 *    N		 INV		   INV		   relation has no bucket
 *    B		  1 		   KEY		   relation has bucket but without bucket storage
 *    S		 OID		   INV		   relation has bucket storage without bucket key
 *    K		 OID		   KEY		   relation has bucket key
 */
#define REALTION_BUCKETKEY_INITED(relation)	\
	 ((relation)->rd_bucketkey != (RelationBucketKey*)&((relation)->rd_bucketkey))

#define REALTION_BUCKETKEY_VALID(relation) 		\
	 (REALTION_BUCKETKEY_INITED(relation) &&		\
	 PointerIsValid((relation)->rd_bucketkey))

/* type: SKB*/
#define RELATION_HAS_BUCKET(relation)				\
	 (OidIsValid((relation)->rd_bucketoid))

/* type: SK */
#define RELATION_OWN_BUCKET(relation)				\
	 (RELATION_HAS_BUCKET(relation) &&				\
	 (VirtualBktOid != (relation)->rd_bucketoid))

/* type: K*/
#define RELATION_OWN_BUCKETKEY(relation)			\
	  (RELATION_OWN_BUCKET(relation)  &&			\
	   RelationIsPartitioned(relation))

/* type: SK && Non-part*/
#define RELATION_CREATE_BUCKET(relation)			\
	 (RelationIsNonpartitioned(relation) && 		\
	  RELATION_OWN_BUCKET(relation))


#define RELATION_IS_DELTA(relation)                       \
    (IsCStoreNamespace(RelationGetNamespace(relation)) && \
        pg_strncasecmp(RelationGetRelationName(relation), "pg_delta", strlen("pg_delta")) == 0)

#define RELATION_GET_CMPRS_ATTR(relation) ((relation)->rd_rel->relcmprs)
#define RELATION_SET_CMPRS_ATTR(relation, cmprs) ((relation)->rd_rel->relcmprs = cmprs)

static inline bool IsCompressedByCmprsInPgclass(const RelCompressType cmprInPgclass)
{
    return (cmprInPgclass > REL_CMPRS_PAGE_PLAIN && cmprInPgclass < REL_CMPRS_MAX_TYPE);
}

/* at default row relation is not compressed in options */
/* maybe exsiting compressed row-table don't modify compression option synchronously */
#define RowRelationIsCompressed(relation) \
    (pg_strcasecmp(COMPRESSION_NO, StdRdOptionsGetStringData(relation->rd_options, compression, COMPRESSION_NO)) != 0 || \
        IsCompressedByCmprsInPgclass((RelCompressType)relation->rd_rel->relcmprs))

#define RelIsSpecifiedFTbl(rte, SepcifiedType) \
    ((rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) \
     && isSpecifiedSrvTypeFromRelId(rte->relid, SepcifiedType))

#define RelationGetStartCtidInternal(relation) \
    StdRdOptionsGetStringData((relation)->rd_options, start_ctid_internal, NULL)

#define RelationGetEndCtidInternal(relation) StdRdOptionsGetStringData((relation)->rd_options, end_ctid_internal, NULL)

#define RelationInRedistribute(relation) (REDIS_REL_NORMAL < (RelationGetAppendMode(relation)) ? true : false)

#define RelationInRedistributeReadOnly(relation) \
    (REDIS_REL_READ_ONLY == (RelationGetAppendMode(relation)) ? true : false)

#define RelationInRedistributeEndCatchup(relation) \
    (REDIS_REL_END_CATCHUP == (RelationGetAppendMode(relation)) ? true : false)

#define RelationIsRedistributeDest(relation)		\
    (REDIS_REL_DESTINATION == (RelationGetAppendMode(relation)) ? true : false)

/* Get info */
#define RelationGetRelCnOid(relation) \
    ((relation)->rd_options ? ((StdRdOptions*)(relation)->rd_options)->rel_cn_oid : InvalidOid)

#define RelationGetRelMergeList(relation) \
        StdRdOptionsGetStringData((relation)->rd_options, merge_list, NULL)

#define ParitionGetWaitCleanGpi(partition) StdRdOptionsGetStringData((partition)->rd_options, wait_clean_gpi, OptDisabledWaitCleanGpi)
#define RelationGetWaitCleanGpi(relation) StdRdOptionsGetStringData((relation)->rd_options, wait_clean_gpi, OptDisabledWaitCleanGpi)

/* Partition get reloptions whether have wait_clean_gpi for parition */
static inline bool PartitionEnableWaitCleanGpi(Partition partition)
{
    if (PointerIsValid(partition) && partition->rd_options != NULL &&
        pg_strcasecmp(OptEnabledWaitCleanGpi, ParitionGetWaitCleanGpi(partition)) == 0) {
        return true;
    }

    return false;
}

/* Relation get reloptions whether have wait_clean_gpi for relation */
static inline bool RelationEnableWaitCleanGpi(Relation relation)
{
    if (PointerIsValid(relation) && relation->rd_options != NULL &&
        pg_strcasecmp(OptEnabledWaitCleanGpi, RelationGetWaitCleanGpi(relation)) == 0) {
        return true;
    }

    return false;
}

/* routines in utils/cache/relcache.c */
extern bool RelationIsDfsStore(Relation relatioin);
extern bool RelationIsPaxFormatByOid(Oid relid);
#ifdef ENABLE_MOT
extern bool RelationIsMOTTableByOid(Oid relid);
#endif
extern bool RelationIsCUFormatByOid(Oid relid);

#define IS_FOREIGNTABLE(rel) ((rel)->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
#define IS_STREAM_TABLE(rel) ((rel)->rd_rel->relkind == RELKIND_STREAM)
extern bool have_useft_privilege(void);

extern RelationMetaData* make_relmeta(Relation rel);
extern Relation get_rel_from_meta(RelationMetaData* frel);
extern bool CheckRelOrientationByPgClassTuple(HeapTuple tuple, TupleDesc tupdesc, const char* orientation);

#endif // !FRONTEND_PARSER
#endif /* REL_GS_H */

