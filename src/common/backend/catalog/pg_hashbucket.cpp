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
 * -------------------------------------------------------------------------
 *
 * pg_hashbucket.cpp
 *
 * IDENTIFICATION
 *     src/common/backend/catalog/pg_hashbucket.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/namespace.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_hashbucket_fn.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/storage.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_class.h"
#include "catalog/heap.h"
#include "pgxc/redistrib.h"
#include "storage/lock/lock.h"
#include "storage/smgr/segment.h"
#include "tcop/utility.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "access/hash.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "access/reloptions.h"
#include "access/hbucket_am.h"
#include "executor/node/nodeModifyTable.h"
#include "nodes/makefuncs.h"
#include "commands/cluster.h"
#include "commands/tablespace.h"
#include "parser/parse_utilcmd.h"

#define INITBUCKETCACHESIZE 16
#define BLOCKSIZE (8 * 1024)

typedef struct bucketidcacheent {
    Oid  bucketoid;
    oidvector *bucketlist;
    SubTransactionId createSubid;
} BucketIdCacheEnt;


#define BucketCacheInsert(BID, BUCKETLIST, SUBID)                                                  \
do {                                                                                               \
        BucketIdCacheEnt* idhentry;                                                                \
        bool found = true;                                                                         \
        idhentry = (BucketIdCacheEnt*)hash_search(                                                 \
            u_sess->cache_cxt.BucketIdCache, (void*)&(BID), HASH_ENTER, &found);                   \
        /* used to give notice if found -- now just keep quiet */                                  \
        idhentry->bucketlist = (BUCKETLIST);                                                       \
        idhentry->createSubid = (SUBID);                                                           \
} while (0)

#define BucketIdCacheLookup(BID, BUCKETLIST)                                                                        \
do {                                                                                                                \
        BucketIdCacheEnt* hentry;                                                                                   \
        hentry = (BucketIdCacheEnt*)hash_search(u_sess->cache_cxt.BucketIdCache, (void*)&(BID), HASH_FIND, NULL);   \
        if (hentry != NULL)                                                                                         \
            (BUCKETLIST) = hentry->bucketlist;                                                                      \
        else                                                                                                        \
            (BUCKETLIST) = NULL;                                                                                    \
} while (0)

#define BucketCacheDelete(BID)                                                                           \
{                                                                                                     \
        BucketIdCacheEnt *entry;                                                                      \
        entry = (BucketIdCacheEnt*)hash_search(u_sess->cache_cxt.BucketIdCache,                       \
                                                 (void *) &(BID),                                        \
                                                 HASH_REMOVE, NULL);                                     \
        if (entry == NULL)                                                                            \
            ereport(WARNING,                                                                             \
                   (errcode(ERRCODE_UNDEFINED_TABLE),                                                    \
                   errmsg("trying to delete a bucket cache entry that does not exist")));                \
} while(0)

static void RelationRedisMoveBuckets(Oid relOid1, Oid relOid2, Oid bucketOid, List *bucketList, bool isPart);
static void RelationMoveIndexBuckets(Relation heapRelation1, Relation heapRelation2, List *bucket_list, Oid bucketOid);
static void RelationMoveBuckets(Oid relOid1, Oid relOid2, List *bucket_list, Oid bucketOid);

/*
 * @@GaussDB@@
 * Target        : data partition
 * Brief        : This initializes the relation descriptor cache
 * Description    : At the time that this is invoked, we can't do database access yet (mainly
 *            : because the transaction subsystem is not up); all we are doing is making
 *            : an empty cache hashtable.  This must be done before starting the initialization
 *            : transaction, because otherwise AtEOXact_RelationCache would crash if that
 *            : transaction aborts before we can get the relcache set up.
 * Notes        :
 */

void BucketCacheInitialize(void)
{
    HASHCTL ctl;
    errno_t rc;

    /*
     * create hashtable that indexes the bucketcache
     */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");

    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(BucketIdCacheEnt);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->cache_cxt.BucketIdCache =
        hash_create("Bucket cache by OID", INITBUCKETCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * AtEOXact_BucketCache
 *
 *    Clean up the bucketCache at main-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 * In the case of abort, we don't want to try to rebuild any invalidated
 * cache entries (since we can't safely do database accesses).  Therefore
 * we must reset refcnts before handling pending invalidations.
 *
 * We also need to do special cleanup when the current transaction
 * created any buckets or made use of forced index lists.
 */
void AtEOXact_BucketCache(bool isCommit)
{
    HASH_SEQ_STATUS status;
    BucketIdCacheEnt *idhentry = NULL;

    /*
     * To speed up transaction exit, we want to avoid scanning the bucketcache
     * unless there is actually something for this routine to do.  Other than
     * the debug-only Assert checks, most transactions don't create any work
     * for us to do here, so we keep a static flag that gets set if there is
     * anything to do.    (Currently, this means either a bucket is created in
     * the current xact.)For simplicity, the flag remains set till end of top-level
     * transaction, even though we could clear it at subtransaction end in
     * some cases.
     */
    if (!u_sess->cache_cxt.bucket_cache_need_eoxact_work)
    {
        return;
    }

    hash_seq_init(&status, u_sess->cache_cxt.BucketIdCache);

    while ((idhentry = (BucketIdCacheEnt *) hash_seq_search(&status)) != NULL)
    {
        /*
         * Is it a bucket created in the current transaction?
         *
         * During commit, reset the flag to zero, since we are now out of the
         * creating transaction.  During abort, simply delete the bucket cache
         * entry
         */
        if (idhentry->createSubid != InvalidSubTransactionId)
        {
            if (isCommit)
                idhentry->createSubid = InvalidSubTransactionId;
            else
                BucketCacheDelete(idhentry->bucketoid);
        }
    }
    /* Once done with the transaction, we can reset need_eoxact_work */
    u_sess->cache_cxt.bucket_cache_need_eoxact_work = false;
}

/*
 * AtEOSubXact_BucketCache
 *
 *    Clean up the bucketcache at sub-transaction commit or abort.
 *
 */
void AtEOSubXact_BucketCache(bool isCommit, SubTransactionId mySubid,
                               SubTransactionId parentSubid)
{
    HASH_SEQ_STATUS  status;
    BucketIdCacheEnt *idhentry = NULL;

    /*
     * Skip the relcache scan if nothing to do --- see notes for
     * AtEOXact_BucketCache.
     */
    if (!u_sess->cache_cxt.bucket_cache_need_eoxact_work)
        return;

    hash_seq_init(&status, u_sess->cache_cxt.BucketIdCache);

    while ((idhentry = (BucketIdCacheEnt *) hash_seq_search(&status)) != NULL)
    {
        /*
         * Is it a bucket created in the current subtransaction?
         *
         * During subcommit, mark it as belonging to the parent, instead.
         * During subabort, simply delete the bucket entry.
         */
        if (idhentry->createSubid == mySubid)
        {
            if (isCommit)
                idhentry->createSubid = parentSubid;
            else
                BucketCacheDelete(idhentry->bucketoid);
        }
    }
}

Oid insertHashBucketEntry(oidvector *bucketlist, Oid bucketid)
{
    Relation pg_hashbucket = NULL;
    Datum values[Natts_pg_hashbucket];
    bool  nulls[Natts_pg_hashbucket];
    errno_t errorno = EOK;
    Oid     newBucketOid;
    HeapTuple tup;

    pg_hashbucket = heap_open(HashBucketRelationId, RowExclusiveLock);

    newBucketOid = GetNewOid(pg_hashbucket); /* bucket's persistence only can be 'p'(permanent table)*/

    /* This is a tad tedious, but way cleaner than what we used to do... */
    errorno = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(errorno, "\0", "\0");

    errorno = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(errorno, "\0", "\0");

    Assert(bucketlist != NULL);

    values[Anum_pg_hashbucket_bucketid - 1]      = ObjectIdGetDatum(bucketid);
    values[Anum_pg_hashbucket_bucketcnts - 1]    = Int32GetDatum(bucketlist->dim1);
    values[Anum_pg_hashbucket_bucketmapsize - 1] = Int32GetDatum(t_thrd.xact_cxt.PGXCBucketCnt);
    values[Anum_pg_hashbucket_bucketref - 1]     = Int32GetDatum(1);
    values[Anum_pg_hashbucket_bucketvector - 1]  = PointerGetDatum(bucketlist);

    /* form a tuple using values and null array, and insert it */
    tup = heap_form_tuple(RelationGetDescr(pg_hashbucket), values, nulls);
    HeapTupleSetOid(tup, newBucketOid);
    (void)simple_heap_insert(pg_hashbucket, tup);
    CatalogUpdateIndexes(pg_hashbucket, tup);

    heap_freetuple_ext(tup);
    heap_close(pg_hashbucket, RowExclusiveLock);
    CommandCounterIncrement();

    /* insert bucket cache entry */
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    bucketlist = buildoidvector(bucketlist->values, bucketlist->dim1);
    (void) MemoryContextSwitchTo(oldcxt);
    BucketCacheInsert(newBucketOid, bucketlist, GetCurrentSubTransactionId());
    
    /* must flag that we have rels created in this transaction */
    u_sess->cache_cxt.bucket_cache_need_eoxact_work = true;

    return newBucketOid;
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        :
 * Description  : give the parentId and parttype ,find all tuples matched,
 *                must free the list by call freePartList
 * Notes        :
 */

oidvector *searchHashBucketByOid(Oid bucketOid)
{
    Datum      bucketDatum;
    bool       isNull = false;
    oidvector *bucketList = NULL;
    oidvector *bucketVec = NULL;

    BucketIdCacheLookup(bucketOid, bucketList);
    if (bucketList != NULL)
        return bucketList;

    HeapTuple tuple  = SearchSysCache1(BUCKETRELID, bucketOid);
    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("searchHashBucketByOid cache lookup failed for bucket %u", bucketOid)));
    }

    bucketDatum = SysCacheGetAttr(BUCKETRELID, tuple, 
                                  Anum_pg_hashbucket_bucketvector, &isNull);

    /* if the raw value of bucket key is null, then report error*/
    if (isNull)
    {
        ereport(ERROR,
               (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),  
               errmsg("null bucket for tuple %u", HeapTupleGetOid(tuple))));
    }

    /* sanity check */    
    bucketVec = (oidvector *)PG_DETOAST_DATUM(bucketDatum);
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    bucketList = buildoidvector(bucketVec->values, bucketVec->dim1);
    (void) MemoryContextSwitchTo(oldcxt);
    BucketCacheInsert(bucketOid, bucketList, InvalidSubTransactionId);

    if(bucketVec != (oidvector*)DatumGetPointer(bucketDatum))
        pfree_ext(bucketVec);
    ReleaseSysCache(tuple);

    return bucketList;
}

int searchBucketMapSizeByOid(Oid bucketOid)
{
    Datum      bucketDatum;
    bool       isNull = false;

    HeapTuple tuple  = SearchSysCache1(BUCKETRELID, bucketOid);
    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("searchBucketMapSizeByOid cache lookup failed for bucket %u", bucketOid)));
    }

    bucketDatum = SysCacheGetAttr(BUCKETRELID, tuple, 
                                  Anum_pg_hashbucket_bucketmapsize, &isNull);

    /* if the raw value of bucket key is null, then report error*/
    if (isNull)
    {
        ereport(ERROR,
               (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),  
               errmsg("null bucketmap size for tuple %u", HeapTupleGetOid(tuple))));
    }
    ReleaseSysCache(tuple);

    return DatumGetInt32(bucketDatum);
}

static bool bucketListIsEqual(oidvector *bv1, oidvector *bv2)
{
    if (bv1->dim1 != bv2->dim1)
        return false;

    if (memcmp(bv1->values, bv2->values, bv1->dim1) != 0)
        return false;

    return true;
}

/*
 * @@GaussDB@@
 * Target        : data partition
 * Brief        :
 * Description    : give the parentId and parttype ,find all tuples matched,
 *                     must free the list by call freePartList
 * Notes        :
 */
Oid searchHashBucketByBucketid(oidvector *bucketlist, Oid bucketid)
{
    Relation pg_hashbucket = NULL;
    ScanKeyData key[3];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Oid ret = InvalidOid;

    pg_hashbucket = heap_open(HashBucketRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
                Anum_pg_hashbucket_bucketid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(bucketid));
    ScanKeyInit(&key[1],
                Anum_pg_hashbucket_bucketcnts,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(bucketlist->dim1));
    ScanKeyInit(&key[2],
                Anum_pg_hashbucket_bucketmapsize,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(t_thrd.xact_cxt.PGXCBucketCnt));

    scan = systable_beginscan(pg_hashbucket, HashBucketBidIndexId, true, NULL, 3, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum      bucketDatum;
        bool      isnull = false;
        oidvector *bvec = NULL;    
        
        bucketDatum = heap_getattr(tuple, Anum_pg_hashbucket_bucketvector,
                                   RelationGetDescr(pg_hashbucket),
                                   &isnull);
        Assert(!isnull);
        bvec = (oidvector *)PG_DETOAST_DATUM(bucketDatum);
        if (bucketListIsEqual(bvec, bucketlist)) {
            ret = HeapTupleGetOid(tuple);

            if (bvec != (oidvector*)DatumGetPointer(bucketDatum))
                pfree_ext(bvec);

            break;
        }

        if (bvec != (oidvector*)DatumGetPointer(bucketDatum))
            pfree_ext(bvec);
    }
    systable_endscan(scan);
    heap_close(pg_hashbucket, AccessShareLock);

    return ret;
}

/*
 * @@GaussDB@@
 * Target        : merge list string
 * Brief        :
 * Description    : give the reloid ,return merge list which storage in pgxc_class
 * Notes        :
 */
text* searchMergeListByRelid(Oid reloid, bool *find, bool allowToMiss, bool retresult)
{
    Relation pgxc_class_rel = NULL;
    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    text *ret = NULL;
    *find = false;
    pgxc_class_rel = heap_open(PgxcClassRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
                Anum_pgxc_class_pcrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(reloid));

    scan = systable_beginscan(pgxc_class_rel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum      pgxcDatum;
        bool      isnull = false;

        pgxcDatum = heap_getattr(tuple, Anum_pgxc_class_option,
                                   RelationGetDescr(pgxc_class_rel),
                                   &isnull);
        if (!allowToMiss) {
            if (isnull) {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("got null for pgxc_class option %u", reloid)));
            }
        }

        if (retresult) {
            ret = DatumGetTextPCopy(pgxcDatum);
        }
        *find = true;
        break;
    }
    systable_endscan(scan);
    heap_close(pgxc_class_rel, AccessShareLock);
    return ret;
}

List *relationGetBucketRelList(Relation rel, Partition part)
{
    List       *relList = NIL;
    Relation    bucketRel = NULL;
    oidvector  *bucketlist = searchHashBucketByOid(rel->rd_bucketoid);

    for (int i = 0; i < bucketlist->dim1 ; i++)
    {
        bucketRel = bucketGetRelation(rel, part, bucketlist->values[i]);
        relList   = lappend(relList, bucketRel);
    }

    return relList;
}

Partition bucketGetPartition(Partition part, int2 bucketid)
{
    Partition   bucket = NULL;
    MemoryContext oldcxt;
    errno_t rc;
    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerEnlargeFakepartRefs(t_thrd.utils_cxt.CurrentResourceOwner);
    }
    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);    
    bucket = (Partition)palloc0(sizeof(PartitionData));

    if (!IsBootstrapProcessingMode())
    {
        ResourceOwnerRememberFakepartRef(t_thrd.utils_cxt.CurrentResourceOwner, bucket);
    }
    *bucket = *part;

    bucket->parent = part;
    bucket->pd_part = (Form_pg_partition)palloc(PARTITION_TUPLE_SIZE);
    rc = memcpy_s(bucket->pd_part, PARTITION_TUPLE_SIZE, part->pd_part, PARTITION_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");

    /* fix relation bucketid info */
    bucket->pd_node.bucketNode = bucketid;
    bucket->pd_lockInfo.lockRelId.bktId = bucketid+1;

    /* just reuse partition's stat info */
    bucket->pd_pgstat_info = part->pd_pgstat_info;
    
    bucket->pd_smgr = NULL;
    PartitionOpenSmgr(bucket); /* reopen each time, need to cache bucket smgr in relation */

    (void) MemoryContextSwitchTo(oldcxt);

    return bucket;

}

void bucketClosePartition(Partition bucket)
{
    if (bucket == NULL)
    {
        elog(LOG, "error parameter when release fake bucket relation");
        return;
    }
    if (!IsBootstrapProcessingMode())
    {
        ResourceOwnerForgetFakepartRef(t_thrd.utils_cxt.CurrentResourceOwner, bucket);
    }

    if (bucket->pd_part)
    {
        pfree_ext(bucket->pd_part);
    }
    if(bucket->pd_smgr && PartitionIsBucket(bucket)) {
        smgrclose(bucket->pd_smgr);
    }
    pfree_ext(bucket);
}

bytea* merge_rel_bucket_reloption(Relation rel, int2 bucketid)
{
    HeapTuple rel_tuple = NULL;
    Datum rel_reloptions = (Datum)0;
    Datum merged_reloptions = (Datum)0;

    List* rel_reloptions_list = NIL;
    List* merged_reloptions_list = NIL;

    bytea* merged_rd_options = NULL;
    bool isnull = false;
    char *merge_list = NULL;
    text *merge_list_text = NULL;
    RedisMergeItem *item = NULL;
    bool find_in_pgxcclass = false;

    if(!RelationIsPartition(rel)) {
        rel_tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)), LOG);
        rel_reloptions = SysCacheGetAttr(RELOID, rel_tuple, Anum_pg_class_reloptions, &isnull);
    }else {
        rel_tuple = SearchSysCache1WithLogLevel(PARTRELID , ObjectIdGetDatum(RelationGetRelid(rel)), LOG);
        rel_reloptions = SysCacheGetAttr(PARTRELID, rel_tuple, Anum_pg_partition_reloptions, &isnull);
    }
    if (!HeapTupleIsValid(rel_tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), 
        errmsg("cache lookup failed for relation %u", RelationGetRelid(rel))));

    /* datum ==> list */
    rel_reloptions_list = untransformRelOptions(rel_reloptions);

    ReleaseSysCache(rel_tuple);
    merged_reloptions_list = rel_reloptions_list;

    merge_list_text = searchMergeListByRelid(RelationGetRelid(rel), &find_in_pgxcclass, false);

    if(merge_list_text != NULL) {
        merge_list = VARDATA_ANY(merge_list_text);
        item = hbkt_get_one_merge_item(merge_list, VARSIZE_ANY_EXHDR(merge_list_text), bucketid);
        pfree_ext(merge_list_text);
    }

    ItemPointerData     start_ctid, end_ctid;
    if(item != NULL) {
        ItemPointerSet(&start_ctid, item->start/BLOCKSIZE, item->start%BLOCKSIZE);
        ItemPointerSet(&end_ctid, item->end/BLOCKSIZE, item->end%BLOCKSIZE);
        pfree(item);
    } else {
        /* not exist in merge_list, we return -1 to ignore it */
        ItemPointerSet(&start_ctid, -1, -1);
        ItemPointerSet(&end_ctid, -1, -1);
        /* if not found in merge_list, means need not append only mode */
        RemoveRedisRelOptionsFromList(&merged_reloptions_list);
        merged_reloptions_list =
            lappend(merged_reloptions_list, makeDefElem(pstrdup("append_mode_internal"), (Node*)makeInteger(REDIS_REL_NORMAL)));
    }
    merged_reloptions_list = list_delete_name(merged_reloptions_list, "start_ctid_internal");
    merged_reloptions_list = list_delete_name(merged_reloptions_list, "end_ctid_internal");
    merged_reloptions_list = add_ctid_string_to_reloptions(merged_reloptions_list, "start_ctid_internal", &start_ctid);
    merged_reloptions_list = add_ctid_string_to_reloptions(merged_reloptions_list, "end_ctid_internal", &end_ctid);
    /* list ==> datum */
    merged_reloptions = transformRelOptions((Datum)0, merged_reloptions_list, NULL, NULL, false, false);

    /* datum ==> bytea * */
    merged_rd_options = heap_reloptions(RELKIND_RELATION, merged_reloptions, true);
    return merged_rd_options;
}

Relation bucketGetRelation(Relation rel, Partition part, int2 bucketId)
{
    Relation   bucket = NULL;
    MemoryContext oldcxt;
    errno_t rc = 0;
    bytea *merge_reloption = NULL;
    Assert(BUCKET_NODE_IS_VALID(bucketId));
    Assert(!RelationIsBucket(rel));

    /*
     * Memory malloced in merge_rel_part_reloption cannot mount in CacheMemoryContext,
     * the same is true for other memory in this function and these may be optimized later.
     */
    if (RelationInClusterResizing(rel)) {
        Relation part_rel = NULL;
        if (RelationIsPartitioned(rel)) {
            Assert(PointerIsValid(part));
            part_rel = partitionGetRelation(rel, part);
            merge_reloption = merge_rel_bucket_reloption(part_rel, bucketId);
            releaseDummyRelation(&part_rel);
        } else {
            merge_reloption = merge_rel_bucket_reloption(rel, bucketId);
        }
    } else {
        merge_reloption = rel->rd_options;
    }

    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    bucket = (Relation)palloc0(sizeof(RelationData));
    *bucket = *rel;
    /*
     * Init bucket relation to avoid double free in releaseDummyRelation()
     * for bucket relation is copied from main relation.
     */
    bucket->rd_node.bucketNode = bucketId;
    bucket->rd_rel = NULL;
    bucket->rd_options = NULL;
    bucket->rd_indexcxt = NULL;

    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerRememberFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, bucket);
    }

    bucket->parent = rel;
    bucket->rd_rel = (Form_pg_class)palloc(sizeof(FormData_pg_class));
    rc = memcpy_s(bucket->rd_rel, sizeof(FormData_pg_class), rel->rd_rel, sizeof(FormData_pg_class));
    securec_check(rc, "\0", "\0");

    /* if relation is partitioned then we initialize bucket relation from the specific partition */
    if (RelationIsPartitioned(rel))
    {
        Assert(PointerIsValid(part));
        bucket->rd_id = part->pd_id;
        bucket->parentId = rel->rd_id;
        bucket->rd_node = part->pd_node;
        bucket->rd_refcnt = part->pd_refcnt;
        bucket->rd_isvalid = part->pd_isvalid;
        bucket->rd_createSubid = part->pd_createSubid;
        bucket->rd_newRelfilenodeSubid = part->pd_newRelfilenodeSubid;
        bucket->rd_lockInfo = part->pd_lockInfo;
        bucket->rd_rel->relfilenode = part->pd_part->relfilenode;
        bucket->rd_rel->reltoastrelid = part->pd_part->reltoastrelid;
        bucket->rd_rel->reltablespace = part->pd_part->reltablespace;
    }
    /* fix relation bucketid info */
    bucket->rd_node.bucketNode = bucketId;
    bucket->rd_lockInfo.lockRelId.bktId = bucketId+1;
    bucket->rd_rel->parttype = PARTTYPE_NON_PARTITIONED_RELATION;
    bucket->rd_rel->relpages = 0;
    bucket->rd_rel->reltuples = 0;
    bucket->rd_bucketoid = InvalidOid;
    bucket->rd_bucketkey = NULL;
    bucket->rd_smgr = NULL;
    RelationOpenSmgr(bucket);
    /* just reuse relatition or partition's stat info */    
    bucket->pgstat_info = (part == NULL) ? rel->pgstat_info : part->pd_pgstat_info; 

    if (OidIsValid(rel->rd_rel->relam)) {
        bucket->rd_indexcxt = AllocSetContextCreate(u_sess->cache_mem_cxt,
            PointerIsValid(part) ? PartitionGetPartitionName(part) : RelationGetRelationName(rel),
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
    }

    if (NULL != merge_reloption) {
        int relOptSize = VARSIZE_ANY(merge_reloption);
        errno_t ret = EOK;
        bucket->rd_options = (bytea*)palloc(relOptSize);
        ret = memcpy_s(bucket->rd_options, relOptSize, merge_reloption, relOptSize);
        securec_check(ret, "\0", "\0");
    }

    (void) MemoryContextSwitchTo(oldcxt);

    return bucket;
}

void hashbucket_info_parse_string(char *string, List **vallist)
{
    char *nptr = string;
    char *endptr = NULL;

    while (1) {
        int val = strtoll(nptr, &endptr, 10);
        if (errno == ERANGE || nptr == endptr) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to parse digtal number in string: %m")));
        }
        *vallist = lappend_int(*vallist, val);
        if (*endptr == ' ') {
            nptr = endptr + 1;
        } else if (*endptr == '\0') {
            break;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to parse valid space separator in string")));
        }
    }
}

Datum set_hashbucket_info(PG_FUNCTION_ARGS)
{
    text *hashbucketIdInfo = PG_GETARG_TEXT_P(0);
    char *hashbucketIdStr = NULL;
    List *idList = NIL;
    ListCell *l = NULL;
    int i = 0;

    if (!isRestoreMode || u_sess->storage_cxt.dumpHashbucketIdNum != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("should in restore mode")));
    }

    hashbucketIdStr = text_to_cstring(hashbucketIdInfo);
    hashbucket_info_parse_string(hashbucketIdStr, &idList);

    if (idList == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid hashbucketId syntax")));

    if (idList->length <= 1 || idList->length > BUCKETDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid hashbucketId syntax")));
    }

    u_sess->storage_cxt.dumpHashbucketIdNum = idList->length - 1;
    u_sess->storage_cxt.dumpHashbucketIds = (int2 *)MemoryContextAllocZero(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (idList->length - 1) * sizeof(int2));
    foreach (l, idList) {
        int curval = lfirst_int(l);
        if (curval > BUCKETDATALEN || curval < 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid bucket id syntax")));
        }
        if (i == 0) {
            t_thrd.xact_cxt.PGXCBucketCnt = curval;
        } else {
            u_sess->storage_cxt.dumpHashbucketIds[i - 1] = curval;
        }
        i++;
    }

    pfree_ext(hashbucketIdStr);
    list_free_ext(idList);

    PG_RETURN_BOOL(true);
}


Oid get_relationtuple_bucketoid(HeapTuple tup)
{
    Datum datum;
    bool  isnull = false;

    datum = heap_getattr(tup, Anum_pg_class_relbucket, GetDefaultPgClassDesc(), &isnull);
    if (isnull == true) {
        return InvalidOid;
    }

    return DatumGetObjectId(datum);
}

HeapTuple update_reltuple_bucketoid(HeapTuple tuple, Oid bucketOid)
{
    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    HeapTuple ntup = NULL;
    errno_t rc;

    Assert(OidIsValid(bucketOid));
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relbucket - 1] = true;
    values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(bucketOid);

    ntup = heap_modify_tuple(tuple, GetDefaultPgClassDesc(),
                             values, nulls, replaces);
    return ntup;
}

HeapTuple update_reltuple_bucket(HeapTuple tuple, Oid bucketOid, int2vector* bucketcol)
{
    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    HeapTuple ntup = NULL;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relbucket - 1] = true;
    if (OidIsValid(bucketOid)) {
        values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(bucketOid);
    } else {
        nulls[Anum_pg_class_relbucket - 1] = true;
    }

    replaces[Anum_pg_class_relbucketkey - 1] = true;
    if (bucketcol != NULL)
        values[Anum_pg_class_relbucketkey - 1] = PointerGetDatum(bucketcol);
    else
        nulls[Anum_pg_class_relbucketkey - 1] = true;

    ntup = heap_modify_tuple(tuple, GetDefaultPgClassDesc(),
                             values, nulls, replaces);
    return ntup;
}

int2vector *getBucketKey(HeapTuple tup)
{
    int16      *attNum = NULL;
    int         nColumn;
    int2vector *bkey = NULL;
    attNum = relationGetHBucketKey(tup, &nColumn);
    /* if the raw value of bucket key is null, then set bucketkey to NULL */
    if (attNum == NULL) {
        bkey = NULL;
    } else {
        /* Initialize int2verctor structure for attribute number array of bucket key */
        bkey = buildint2vector(NULL, nColumn);
        /* specify value to int2verctor and build type oid array */
        for (int i = 0; i < nColumn; i++) {
            bkey->values[i] = attNum[i];
        }
    }
    return bkey;
}

static void swap_reltuple_bucket(HeapTuple *t1, HeapTuple *t2)
{
    HeapTuple tup1 = *t1;
    HeapTuple tup2 = *t2;
    Oid   o1, o2;

    o1 = get_relationtuple_bucketoid(*t1);
    o2 = get_relationtuple_bucketoid(*t2);

    int2vector* bkey1 = getBucketKey(tup1);
    int2vector* bkey2 = getBucketKey(tup2);

    *t1 = update_reltuple_bucket(*t1, o2, bkey2);
    *t2 = update_reltuple_bucket(*t2, o1, bkey1);

    Oid r1 = HeapTupleGetOid(tup1);
    if (bkey1 == NULL && bkey2 != NULL) {
        InsertTablebucketidAttribute(r1);
    } else if (bkey1 != NULL && bkey2 == NULL) {
        RemoveAttributeById(r1, BucketIdAttributeNumber);
    }

    /* Clean up. */
    heap_freetuple(tup1);
    heap_freetuple(tup2);
}

static void HbktTransferReplaceOptions(HeapTuple *t1, const HeapTuple *t2)
{
    Datum relReloptions2 = (Datum)0;
    bool isnull = false;
    relReloptions2 = SysCacheGetAttr(RELOID, *t2, Anum_pg_class_reloptions, &isnull);
    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_reloptions - 1] = true;
    if (relReloptions2 != (Datum)0)
        values[Anum_pg_class_reloptions - 1] = relReloptions2;
    else
        nulls[Anum_pg_class_reloptions - 1] = true;

    *t1 = heap_modify_tuple(*t1, GetDefaultPgClassDesc(), values, nulls, replaces);
}

static int IndexGetIndnatts(HeapTuple tup)
{
    Datum datum;
    bool  isnull = false;

    datum = heap_getattr(tup, Anum_pg_index_indnatts, GetDefaultPgIndexDesc(), &isnull);
    Assert(!isnull);

    return Int16GetDatum(datum);
}

static int16 *IndexGetIndkey(HeapTuple tuple, int *nColumn)
{
    Datum       indkeyRaw;
    bool        isNull = false;
    ArrayType  *IndkeyColumns = NULL;

    Assert(PointerIsValid(nColumn));
    /* Get the raw data which contain patition key's columns */
    indkeyRaw = heap_getattr(tuple, Anum_pg_index_indkey,
                            GetDefaultPgIndexDesc(), &isNull);
    /* the raw value of index key is not null */
    Assert(!isNull);

    /* convert Datum to ArrayType */
    IndkeyColumns = DatumGetArrayTypeP(indkeyRaw);

    /* Get number of index key columns from int2verctor */
    *nColumn = ARR_DIMS(IndkeyColumns)[0];

    /* CHECK: the ArrayType of index key is valid */
    if (ARR_NDIM(IndkeyColumns) != 1 || *nColumn < 0 || ARR_HASNULL(IndkeyColumns) ||
        ARR_ELEMTYPE(IndkeyColumns) != INT2OID) {
        ereport(ERROR,
               (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
               errmsg("index key column's number is not a 1-D smallint array")));
    }

    /* Get int2 array of index key column numbers */
    return (int16 *)ARR_DATA_PTR(IndkeyColumns);
}

static int2vector *GetIndexTupleIndkey(HeapTuple tup, int *nColumn)
{
    int16      *attNum = NULL;
    int2vector *indkey = NULL;
    attNum = IndexGetIndkey(tup, nColumn);

    /* Initialize int2verctor structure for attribute number array of bucket key */
    indkey = buildint2vector(NULL, *nColumn);
    /* specify value to int2verctor and build type oid array */
    for (int i = 0; i < *nColumn; i++) {
        indkey->values[i] = attNum[i];
    }
    return indkey;
}

HeapTuple update_reltuple_index(HeapTuple tuple, int indnatts, int2vector* indkey)
{
    Datum       values[Natts_pg_index];
    bool        replaces[Natts_pg_index];
    bool        nulls[Natts_pg_index];
    HeapTuple ntup = NULL;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_index_indnatts - 1] = true;
    values[Anum_pg_index_indnatts - 1] = ObjectIdGetDatum(indnatts);

    replaces[Anum_pg_index_indkey - 1] = true;
    if (indkey != NULL)
        values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);
    else
        nulls[Anum_pg_index_indkey - 1] = true;

    ntup = heap_modify_tuple(tuple, GetDefaultPgIndexDesc(),
                             values, nulls, replaces);
    return ntup;
}


void HbktTransferModifyPgIndexNattsAndIndkey(DataTransferType transferType, HeapTuple *t1, bool *cboffIndex)
{
    int indnatts1 = IndexGetIndnatts(*t1);
    int nColumn = 0;
    int2vector* indkey1 = GetIndexTupleIndkey(*t1, &nColumn);
    int2vector *indkey = NULL;
    int i = 0;
    
    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET) {
        indkey = buildint2vector(NULL, nColumn + 1);
        for (int i = 0; i < nColumn; i++) {
            indkey->values[i] = indkey1->values[i];
        }
        indkey->values[nColumn] = BucketIdAttributeNumber;
        indnatts1 += 1;
        *cboffIndex = false;
    } else {
        int j = 0;
        *cboffIndex = true;
        indkey = buildint2vector(NULL, nColumn - 1);
        for (i = 0; i < nColumn; i++) {
            if (indkey1->values[i] == BucketIdAttributeNumber) {
                *cboffIndex = false;
                continue;
            }
            if (*cboffIndex && i == nColumn - 1) {
                break;
            }
            indkey->values[j++] = indkey1->values[i];
        }
        if (*cboffIndex == false) {
            indnatts1 -= 1;
        }
    }
    if (*cboffIndex == false) {
        *t1 = update_reltuple_index(*t1, indnatts1, indkey);
    }
    pfree_ext(indkey);
    pfree_ext(indkey1);
}


HeapTuple update_reltuple_relnatts(HeapTuple tuple, Oid relnatts)
{
    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    HeapTuple ntup = NULL;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relnatts - 1] = true;
    if (OidIsValid(relnatts)) {
        values[Anum_pg_class_relnatts - 1] = ObjectIdGetDatum(relnatts);
    } else {
        nulls[Anum_pg_class_relnatts - 1] = true;
    }

    ntup = heap_modify_tuple(tuple, GetDefaultPgClassDesc(),
                             values, nulls, replaces);
    return ntup;
}

int pgclass_get_relnatts(HeapTuple tup)
{
    Datum datum;
    bool  isnull = false;

    datum = heap_getattr(tup, Anum_pg_class_relnatts, GetDefaultPgClassDesc(), &isnull);
    Assert(!isnull);

    return Int16GetDatum(datum);
}

HeapTuple HbktTransferModifyPgClassRelbucket(HeapTuple tuple, DataTransferType transferType,
    Oid bucketOid, bool isUsable)
{
    Oid relbucket;
    if (isUsable) {
        return tuple;
    }

    if (transferType == HASHBUCKET_TO_NORMAL || transferType == SEGMENT_TO_NORMAL) {
        relbucket= InvalidOid;
    } else if (transferType == HASHBUCKET_TO_SEGMENT || transferType == NORMAL_TO_SEGMENT) {
        relbucket = VirtualSegmentOid;
    } else {
        relbucket = bucketOid;
    }

    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    HeapTuple ntup = NULL;
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relbucket - 1] = true;
    if (OidIsValid(relbucket)) {
        values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(relbucket);
    } else {
        nulls[Anum_pg_class_relbucket - 1] = true;
    }

    ntup = heap_modify_tuple(tuple, GetDefaultPgClassDesc(),
                             values, nulls, replaces);
    return ntup;
}

HeapTuple HbktModifyRelationRelfilenode(HeapTuple reltup, DataTransferType transferType, Relation indexrel,
    bool isUsable, Oid rel2bucketId)
{
    Oid newrelfilenode;
    int4 bucketNode;
    RelFileNode rnode;
    Datum values[Natts_pg_class];
    bool replaces[Natts_pg_class];
    bool nulls[Natts_pg_class];
    errno_t rc;

    if (isUsable) {
        return reltup;
    }

    bool isBucket = (transferType == TRANSFER_IS_INVALID ? true :
        (transferType != HASHBUCKET_TO_HASHBUCKET ? false : !RelationIsCrossBucketIndex(indexrel)));
    ereport(LOG, (errmsg(
        "The relation's index(%u) is unusable, will create new relfilenode, transferType is %d, isBucket is %d.",
        indexrel->rd_id, transferType, isBucket)));

    if (transferType == HASHBUCKET_TO_NORMAL || transferType == SEGMENT_TO_NORMAL) {
        newrelfilenode = GetNewRelFileNode(indexrel->rd_rel->reltablespace, NULL, indexrel->rd_rel->relpersistence);
        bucketNode = InvalidBktId;
    } else {
        newrelfilenode = seg_alloc_segment(ConvertToRelfilenodeTblspcOid(indexrel->rd_rel->reltablespace),
            u_sess->proc_cxt.MyDatabaseId, isBucket, InvalidBlockNumber);
        bucketNode = SegmentBktId;
    }
    rnode = indexrel->rd_node;
    rnode.relNode = newrelfilenode;
    rnode.bucketNode = bucketNode;
    RelationCreateStorage(rnode, indexrel->rd_rel->relpersistence, indexrel->rd_rel->relowner,
        isBucket ? rel2bucketId : InvalidOid);

    InsertStorageIntoPendingList(&indexrel->rd_node, InvalidAttrNumber, indexrel->rd_backend,
        indexrel->rd_rel->relowner, true);
    Assert(OidIsValid(newrelfilenode));

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relfilenode - 1] = true;
    values[Anum_pg_class_relfilenode - 1] = ObjectIdGetDatum(newrelfilenode);
    reltup = heap_modify_tuple(reltup, GetDefaultPgClassDesc(), values, nulls, replaces);
    return reltup;
}

HeapTuple HbktTransferModifyRelationRelnatts(HeapTuple reltup, DataTransferType transferType, bool cboffIndex)
{
    int indnatts1 = pgclass_get_relnatts(reltup);
    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET) {
        indnatts1 += 1;
    } else if (!cboffIndex) {
        indnatts1 -= 1;
    }

    reltup = update_reltuple_relnatts(reltup, indnatts1);
    return reltup;
}

HeapTuple update_reltuple_reloptions(HeapTuple t1, Datum new_options)
{
    Datum       values[Natts_pg_class];
    bool        replaces[Natts_pg_class];
    bool        nulls[Natts_pg_class];
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_reloptions - 1] = true;
    if (new_options != (Datum)0)
        values[Anum_pg_class_reloptions - 1] = new_options;
    else
        nulls[Anum_pg_class_reloptions - 1] = true;

    t1 = heap_modify_tuple(t1, GetDefaultPgClassDesc(), values, nulls, replaces);
    return t1;
}

HeapTuple HbktTtransferModifyRelationReloptions(HeapTuple reltup, DataTransferType transferType, bool isUsable)
{
    bool isnull = false;
    Datum new_options;
    if (isUsable) {
        return reltup;
    }
    Datum datum = SysCacheGetAttr(RELOID, reltup, Anum_pg_class_reloptions, &isnull);
    List* old_reloptions = untransformRelOptions(datum);
    if (transferType == NORMAL_TO_HASHBUCKET || transferType == SEGMENT_TO_HASHBUCKET) {
        old_reloptions = lappend(old_reloptions, makeDefElem("crossbucket", (Node*)makeString("on")));
    } else {
        old_reloptions = list_delete_name(old_reloptions, "crossbucket");
    }
    new_options = transformRelOptions((Datum)0, old_reloptions, NULL, NULL, false, false);
    list_free_ext(old_reloptions);
    return update_reltuple_reloptions(reltup, new_options);
}

void relation_swap_bucket(Oid r1, Oid r2, bool transfer)
{
    Relation relRelation;
    CatalogIndexState indstate;
    HeapTuple       reltup1;
    HeapTuple       reltup2;

    /* We need writable copies of both pg_class tuples. */
    relRelation = heap_open(RelationRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
    if (!HeapTupleIsValid(reltup1))
            ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_TABLE),
                             errmsg("cache lookup failed for relation %u", r1)));

    reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
    if (!HeapTupleIsValid(reltup2))
            ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_TABLE),
                             errmsg("cache lookup failed for relation %u", r2)));

    swap_reltuple_bucket(&reltup1, &reltup2);

    if (transfer) {
        HbktTransferReplaceOptions(&reltup1, &reltup2);
    }

    /*update catalog*/
    simple_heap_update(relRelation, &reltup1->t_self, reltup1);
    simple_heap_update(relRelation, &reltup2->t_self, reltup2);

    /* Keep system catalogs current */
    indstate = CatalogOpenIndexes(relRelation);
    CatalogIndexInsert(indstate, reltup1);
    CatalogIndexInsert(indstate, reltup2);
    CatalogCloseIndexes(indstate);

    /* Clean up.*/
    heap_freetuple(reltup1);
    heap_freetuple(reltup2);
    heap_close(relRelation, RowExclusiveLock);
}

static List *get_redis_bucketlist(Oid relOid1, Oid relOid2)
{
    oidvector *blist1 = NULL;
    oidvector *blist2 = NULL;
    Relation   rel1 = NULL;
    Relation   rel2 = NULL;
    List  *droplist = NULL;
    int    start = 0;

    rel1 = relation_open(relOid1, NoLock);
    rel2 = relation_open(relOid2, NoLock);
    blist1 = searchHashBucketByOid(rel1->rd_bucketoid);
    blist2 = searchHashBucketByOid(rel2->rd_bucketoid);

    Assert(blist1->dim1 >= blist2->dim1);

    /* nothing to do if they have same buckets */
    if (blist1->dim1 == blist2->dim1) {
        for (int i = 0; i < blist1->dim1; i++) {
            if (blist1->values[i] != blist2->values[i]) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("bucket length is same but they have different buckets")));
            }
        }
        return NULL;
    }

    for (int i = 0; i < blist1->dim1; i++) {
        int pos = lookupHBucketid(blist2, start, blist1->values[i]);
        if (pos != -1) {
            start = pos;
        } else {
            droplist = lappend_oid(droplist, blist1->values[i]);
        }
    }
    relation_close(rel1, NoLock);
    relation_close(rel2, NoLock);

    return droplist;
}

static Oid get_relation_bucket(Oid relOid)
{
    HeapTuple    reltup = NULL;
    Oid     bucketOid;

    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR,
               (errcode(ERRCODE_UNDEFINED_TABLE),
               errmsg("cache lookup failed for relation %u", relOid)));

    /* Get target bucketOid from reltup */
    bucketOid = get_relationtuple_bucketoid(reltup);
    heap_freetuple(reltup);

    return bucketOid;
}

static void update_relation_bucket(Oid relOid1, Oid bucketOid)
{
    CatalogIndexState indstate;
    Relation    relRelation;
    HeapTuple   reltup1 = NULL;

    HeapTuple   ntup = NULL;

    /* We need writable copies of both pg_class tuples. */
    relRelation = heap_open(RelationRelationId, RowExclusiveLock);

    reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid1));
    if (!HeapTupleIsValid(reltup1))
        ereport(ERROR,
               (errcode(ERRCODE_UNDEFINED_TABLE),
               errmsg("cache lookup failed for relation %u", relOid1)));

    /* Sync target bucketOid to reltup1 */
    ntup = update_reltuple_bucketoid(reltup1, bucketOid);

    /* Update catalog */
    simple_heap_update(relRelation, &ntup->t_self, ntup);

    /* Keep system catalog current */
    indstate = CatalogOpenIndexes(relRelation);
    CatalogIndexInsert(indstate, ntup);
    CatalogCloseIndexes(indstate);

    /* Clean up.*/
    heap_freetuple(reltup1);
    heap_freetuple(ntup);    
    heap_close(relRelation, RowExclusiveLock);
}

/*
 * dropBucketList
 * Drop a list of buckets.
 * @ in rel: first level relation.
 * @ in bucketIdList: the hash bucket id list going to drop. The list may contain some id
 *     that not in current DN, we just skip these buckets, and get the bucket we really have.
 * @ in hashMap: the hash bucket map, used to find the bucket oid by bucket id.
 */
static void dropBucketList(Relation rel, List *bucketIdList)
{
    Relation bucketRel;
    ListCell *cell = NULL;
    int2 bucketId;

    /* no need to drop storage of a partitioned table*/
    if (RelationIsPartitioned(rel) || !bucketIdList || RelationIsCrossBucketIndex(rel)) {
        return;
    }

    if (bucketIdList->length >= BUCKETDATALEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("Cannot drop all buckets")));
    }

    foreach(cell, bucketIdList) {
        bucketId = (int2)lfirst_oid(cell);
        if (bucketId >= BUCKETDATALEN) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid bucket id %u, max bucket id is %d", bucketId, BUCKETDATALEN - 1)));
        }
        bucketRel = bucketGetRelation(rel, NULL, bucketId);
        RelationDropStorage(bucketRel);
        bucketCloseRelation(bucketRel);
    }
}

static void RelationDropIndexBuckets(Relation heapRelation, List *bucket_list, Oid bucketOid)
{
    ListCell   *indlist = NULL;

    /* Ask the relcache to produce a list of the indexes of the rel */
    foreach(indlist, RelationGetIndexList(heapRelation, true)) {
        Oid      indexId = lfirst_oid(indlist);

        /* Open the index relation; use exclusive lock, just to be sure */
        Relation currentIndex = index_open(indexId, AccessExclusiveLock);

        dropBucketList(currentIndex, bucket_list);

        /* Update the bucketoid */
        update_relation_bucket(indexId, bucketOid);
        CacheInvalidateRelcache(currentIndex);

        index_close(currentIndex, NoLock);
    }
}

static void MoveToastIndexBuckets(Relation heapRelation1, Relation heapRelation2, List *bucket_list, Oid bucketOid)
{
    errno_t rc = EOK;
    char relIndexName1[NAMEDATALEN];
    char relIndexName2[NAMEDATALEN];
    rc = snprintf_s(relIndexName1, NAMEDATALEN, NAMEDATALEN - 1, "%s_index", RelationGetRelationName(heapRelation1));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(relIndexName2, NAMEDATALEN, NAMEDATALEN - 1, "%s_index", RelationGetRelationName(heapRelation2));
    securec_check_ss(rc, "\0", "\0");
    Oid indexId1 = get_relname_relid(relIndexName1, RelationGetNamespace(heapRelation1));
    Oid indexId2 = get_relname_relid(relIndexName2, RelationGetNamespace(heapRelation2));
    RelationRedisMoveBuckets(indexId1, indexId2, bucketOid, bucket_list, false);
}

static void RelationMoveIndexBuckets(Relation heapRelation1, Relation heapRelation2, List *bucket_list, Oid bucketOid)
{
    Assert(PointerIsValid(heapRelation1));
    Assert(PointerIsValid(heapRelation2));
    List* indicesList = RelationGetIndexList(heapRelation1);
    ListCell* cell = NULL;
    char* srcIdxName = NULL;
    char* tmpIdxName = NULL;
    char* srcSchema = NULL;
    Oid indexId1;
    Oid indexId2;

    foreach (cell, indicesList) {
        indexId1 = lfirst_oid(cell);
        /* Get src index name by oid */
        srcIdxName = get_rel_name(indexId1);
        if (!PointerIsValid(srcIdxName)) {
            continue;
        }
        srcSchema = get_namespace_name(heapRelation1->rd_rel->relnamespace);
        tmpIdxName = getTmptableIndexName(srcSchema, srcIdxName);
        /*
         * The tmp index name is same as src index name, check generateClonedIndexStmt.
         * Get namespace from tmp table, the index of tmp table must have same namespace with tmp table
         */
        indexId2 = get_relname_relid(tmpIdxName, RelationGetNamespace(heapRelation2));
        Assert(OidIsValid(indexId2));

        RelationRedisMoveBuckets(indexId1, indexId2, bucketOid, bucket_list, RelationIsPartitioned(heapRelation1));
        pfree_ext(srcIdxName);
        pfree_ext(tmpIdxName);
        pfree_ext(srcSchema);
    }

    list_free_ext(indicesList);
}

static void RelationDropBuckets(Oid relOid1, List *bucket_list, Oid bucketOid)
{
    Relation relation = relation_open(relOid1, AccessExclusiveLock);
    Oid toastrelid = relation->rd_rel->reltoastrelid;

    /* drop buckets for relation1 */
    dropBucketList(relation, bucket_list);
    RelationDropIndexBuckets(relation, bucket_list, bucketOid);

    /* Update the bucketoid */
    update_relation_bucket(relOid1, bucketOid);

    CacheInvalidateRelcache(relation);

    relation_close(relation, NoLock);

    if (OidIsValid(toastrelid)) {
        RelationDropBuckets(toastrelid, bucket_list, bucketOid);
    }
}

static void RelationMoveBuckets(Oid relOid1, Oid relOid2, List *bucket_list, Oid bucketOid)
{
    Relation relation1 = relation_open(relOid1, AccessExclusiveLock);
    Relation relation2 = relation_open(relOid2, AccessExclusiveLock);
    Oid toastrelid1 = relation1->rd_rel->reltoastrelid;
    Oid toastrelid2 = relation2->rd_rel->reltoastrelid;

    Assert(OidIsValid(toastrelid1) == OidIsValid(toastrelid2));

    if (!RelationIsPartitioned(relation1)) {
        /* open smgr */
        RelationOpenSmgr(relation1);
        RelationOpenSmgr(relation2);

        /* flush all buffers of relation2 in buffer pool */
        flush_all_buffers(relation2, InvalidOid);
        /* move buckets from relation2 to relation1 */
        smgrmovebuckets(relation1->rd_smgr, relation2->rd_smgr, bucket_list);
    }

    /* Update the bucketoid */
    update_relation_bucket(relOid1, bucketOid);

    CacheInvalidateRelcache(relation1);

    /* deal with relation index */
    if (RelationIsRelation(relation1)) {
        RelationMoveIndexBuckets(relation1, relation2, bucket_list, bucketOid);
    } else if (RelationIsToast(relation1)) {
        MoveToastIndexBuckets(relation1, relation2, bucket_list, bucketOid);
    }

    relation_close(relation1, NoLock);
    relation_close(relation2, NoLock);

    if (OidIsValid(toastrelid1)) {
        RelationMoveBuckets(toastrelid1, toastrelid2, bucket_list, bucketOid);
    }
}

static void PartitionDropIndexBuckets(Oid partOid, List *bucket_list)
{
    ListCell *cell;

    /* Do nothing if we got an empty drop list */
    if (bucket_list == NULL) {
        return;
    }

    List *partIndexlist = searchPartitionIndexesByblid(partOid);
    foreach(cell, partIndexlist) {
        HeapTuple partIndexTuple = (HeapTuple)lfirst(cell);
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(partIndexTuple);

        /* Open the index's parent relation with AccessShareLock */
        Relation  parentIndexRel = index_open(partForm->parentid, AccessShareLock);
        /* Open index partition */
        Partition indexPart = partitionOpen(parentIndexRel, HeapTupleGetOid(partIndexTuple), AccessExclusiveLock);
        /* Get index partition's dummy relation */
        Relation  partIndexRel = partitionGetRelation(parentIndexRel, indexPart); 

        /* Drop all bucktes in the drop list */
        dropBucketList(partIndexRel, bucket_list);

        /* Clean up. */
        releaseDummyRelation(&partIndexRel);
        partitionClose(parentIndexRel, indexPart, NoLock);
        index_close(parentIndexRel, NoLock);
    }
}

static void PartitionDropBuckets(Relation heapRelation, Oid partOid, List *bucket_list, Oid bucketOid)
{
    Partition part = partitionOpen(heapRelation, partOid, AccessExclusiveLock);
    Relation  partRel = partitionGetRelation(heapRelation, part);
    Oid toastrelid = partRel->rd_rel->reltoastrelid;

    /* Drop all buckets in the drop list */
    dropBucketList(partRel, bucket_list);
    PartitionDropIndexBuckets(partOid, bucket_list);

    /* Deal with toast if any */
    if (OidIsValid(toastrelid)) {
        RelationDropBuckets(toastrelid, bucket_list, bucketOid);
    }

    /* Clean up. */
    releaseDummyRelation(&partRel);
    partitionClose(heapRelation, part, NoLock);
}

static void PartitionMoveBuckets(Relation heapRelation1, Relation heapRelation2,
                                 Partition part1, Partition part2, List *bucket_list, Oid bucketOid)
{
    /* Open Partition's dummy relation */
    Relation  partRel1 = partitionGetRelation(heapRelation1, part1);
    Relation  partRel2 = partitionGetRelation(heapRelation2, part2);

    /* Get toast oid of each relation */
    Oid toastrelid1 = partRel1->rd_rel->reltoastrelid;
    Oid toastrelid2 = partRel2->rd_rel->reltoastrelid;
    Assert(OidIsValid(toastrelid1) == OidIsValid(toastrelid2));

    RelationOpenSmgr(partRel1);
    RelationOpenSmgr(partRel2);

    /* flush all buffers of partRel2 in buffer pool */
    flush_all_buffers(partRel2, InvalidOid);
    smgrmovebuckets(partRel1->rd_smgr, partRel2->rd_smgr, bucket_list);

    if (OidIsValid(toastrelid1)) {
        RelationMoveBuckets(toastrelid1, toastrelid2, bucket_list, bucketOid);
    }

    /* Clean up. */
    releaseDummyRelation(&partRel1);
    releaseDummyRelation(&partRel2);
}

static void PartitionRedisDropBuckets(Oid relOid, List *bucketList, Oid bucketOid)
{
    ListCell *partCell = NULL;
    Relation relation = relation_open(relOid, AccessExclusiveLock);

    /* Get the partition oid list and do the loop */
    List *partTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, relOid);
    foreach(partCell, partTupleList) {
        Oid partOid = HeapTupleGetOid((HeapTuple)lfirst(partCell));
        PartitionDropBuckets(relation, partOid, bucketList, bucketOid);
    }

    freePartList(partTupleList);
    relation_close(relation, NoLock);
}

static void PartitionRedisMoveBuckets(Oid relOid1, Oid relOid2, List *bucketList, Oid bucketOid)
{
    ListCell *partCell1, *partCell2;
    List* partList1 = NIL;
    List* partList2 = NIL;
    int count = 0;

    Relation relation1 = relation_open(relOid1, AccessExclusiveLock);
    Relation relation2 = relation_open(relOid2, AccessExclusiveLock);

    /* Get the partition oid list and do the loop */
    if (RelationIsIndex(relation1)) {
        partList1 = GetIndexPartitionListByOrder(relation1, ExclusiveLock);
        partList2 = GetIndexPartitionListByOrder(relation2, ExclusiveLock);
    } else {
        partList1 = relationGetPartitionList(relation1, ExclusiveLock);
        partList2 = relationGetPartitionList(relation2, ExclusiveLock);
    }
    Assert(list_length(partList1) == list_length(partList2));

    /* Move buckets in each partition */
    forboth(partCell1, partList1, partCell2, partList2) {
        Partition part1 = (Partition)lfirst(partCell1);
        Partition part2 = (Partition)lfirst(partCell2);
        count++;
        if (part2 == NULL) {
            if (part1->pd_part->indisusable) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("The %dth partition of relation(%u) is not exists.", count, relOid2)));
            }
            ereport(LOG, (errmsg("The partition's index(%u) of relation(%u) is unusable, will skip move bucket",
                part1->pd_id, relOid1)));
            continue;
        }
        PartitionMoveBuckets(relation1, relation2, part1, part2, bucketList, bucketOid);
    }

    releasePartitionList(relation1, &partList1, ExclusiveLock);
    releasePartitionList(relation2, &partList2, ExclusiveLock, false);

    relation_close(relation1, NoLock);
    relation_close(relation2, NoLock);
}

static void RelationRedisMoveBuckets(Oid relOid1, Oid relOid2, Oid bucketOid, List *bucketList, bool isPart)
{
    /* Move bucket storage for heap/index/toast */
    if (isPart) {
        PartitionRedisMoveBuckets(relOid1, relOid2, bucketList, bucketOid);
    }

    /* Deal with relation level*/
    RelationMoveBuckets(relOid1, relOid2, bucketList, bucketOid);
}

static void RelationRedisDropBuckets(Oid relOid1, Oid bucketOid, List *bucketList, bool isPart)
{
    /* Drop bucket storage for heap/index/toast */
    if (isPart) {
        PartitionRedisDropBuckets(relOid1, bucketList, bucketOid);
    }

    /* Deal with relation level*/
    RelationDropBuckets(relOid1, bucketList, bucketOid);
}

/*
 * drop a list of buckets
 */
int64 execute_drop_bucketlist(Oid relOid1, Oid relOid2, bool isPart)
{
    List *bucketList = NIL;
    Oid   bucketOid;

    /* Get the drop bucket list */
    bucketList = get_redis_bucketlist(relOid1, relOid2);

    if (bucketList != NULL) {
        bucketOid  = get_relation_bucket(relOid2);
        RelationRedisDropBuckets(relOid1, bucketOid, bucketList, isPart);
        list_free_ext(bucketList);
    }
    return  1;
}

/*
 * move a list of buckets
 */
int64 execute_move_bucketlist(Oid relOid1, Oid relOid2, bool isPart)
{
    List *bucketList = NIL;
    Oid   bucketOid;

    /* Get the move bucket list */
    bucketList = get_redis_bucketlist(relOid2, relOid1);

    if (bucketList != NULL) {
        bucketOid  = get_relation_bucket(relOid2);
        RelationRedisMoveBuckets(relOid1, relOid2, bucketOid, bucketList, isPart);
        list_free_ext(bucketList);
    }
    return  1;
}

bool hashbucket_eq(oidvector* bucket1, oidvector* bucket2)
{
    if (bucket1 == bucket2) {
        return true;
    }
    if (bucket1->dim1 != bucket2->dim1) {
        return false;
    }

    for (int i = 0; i < bucket1->dim1; i++) {
        if (bucket1->values[i] != bucket2->values[i]) {
            return false;
        }
    }
    return true;
}

int bid_cmp(const void* p1, const void* p2)
{
    Oid b1 = *((const Oid*)p1);
    Oid b2 = *((const Oid*)p2);

    if (b1 < b2)
        return -1;
    if (b1 > b2)
        return 1;
    return 0;
}
