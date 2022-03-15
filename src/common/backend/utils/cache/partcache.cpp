/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * partcache.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/cache/partcache.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <sys/file.h>
#include "access/csnlog.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/schemapg.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/node/nodeModifyTable.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/page_compression.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "catalog/storage.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "access/cstore_am.h"
#include "utils/snapmgr.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "catalog/pg_partition.h"
#include "postmaster/autovacuum.h"
#include "nodes/makefuncs.h"
#include "utils/knl_relcache.h"
#include "utils/knl_partcache.h"
#include "replication/walreceiver.h"

/*
 *part 2: static functions used only in this c source file
 *
 *non-export function prototypes
 */
static HeapTuple ScanPgPartition(Oid targetPartId, bool indexOK, Snapshot snapshot);
static Partition AllocatePartitionDesc(Form_pg_partition relp);
static void PartitionFlushPartition(Partition partition);

static void PartitionParseRelOptions(Partition partition, HeapTuple tuple);

static HeapTuple ScanPgPartition(Oid targetPartId, bool indexOK, Snapshot snapshot)
{
    HeapTuple pg_partition_tuple;
    Relation pg_partition_desc;
    SysScanDesc pg_partition_scan;
    ScanKeyData key[1];

    /*
     * If something goes wrong during backend startup, we might find ourselves
     * trying to read pg_partition before we've selected a database.  That ain't
     * gonna work, so bail out with a useful error message.  If this happens,
     * it probably means a partcache entry that needs to be nailed isn't.
     */
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        ereport(FATAL,
            (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("cannot read pg_class without having selected a database")));
    }

    if (snapshot == NULL) {
        snapshot = GetCatalogSnapshot();
    }

    /*
     * form a scan key
     */
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(targetPartId));

    /*
     * Open pg_partition and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical partcache entries (this includes initdb and startup
     * without a pg_internal.init file).  The caller can also force a heap
     * scan by setting indexOK == false.
     */
    /*u_sess->relcache_cxt.criticalRelcachesBuilt--->criticalPartcachesBuilt*/
    pg_partition_desc = heap_open(PartitionRelationId, AccessShareLock);
    pg_partition_scan = systable_beginscan(pg_partition_desc,
        PartitionOidIndexId,
        indexOK && LocalRelCacheCriticalRelcachesBuilt(),
        snapshot,
        1,
        key);

    pg_partition_tuple = systable_getnext(pg_partition_scan);

    /*
     * Must copy tuple before releasing buffer.
     */
    if (HeapTupleIsValid(pg_partition_tuple)) {
        pg_partition_tuple = heap_copytuple(pg_partition_tuple);
    }

    /* all done */
    systable_endscan(pg_partition_scan);
    heap_close(pg_partition_desc, AccessShareLock);

    return pg_partition_tuple;
}

static Partition AllocatePartitionDesc(Form_pg_partition partp)
{
    Partition partition;
    MemoryContext oldcxt;
    Form_pg_partition partitionForm;
    errno_t rc = 0;

    /* Relcache entries must live in LocalMyDBCacheMemCxt() */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /*
     * allocate and zero space for new relation descriptor
     */
    partition = (Partition)palloc0(sizeof(PartitionData));

    /* make sure relation is marked as having no open file yet */
    partition->pd_smgr = NULL;

    /*
     * Copy the partition tuple form
     *
     * We only allocate space for the fixed fields, ie, CLASS_TUPLE_SIZE. The
     * variable-length fields (relacl, reloptions) are NOT stored in the
     * partcache --- there'd be little point in it, since we don't copy the
     * tuple's nulls bitmap and hence wouldn't know if the values are valid.
     * Bottom line is that relacl *cannot* be retrieved from the partcache. Get
     * it from the syscache if you need it.  The same goes for the original
     * form of reloptions (however, we do store the parsed form of reloptions
     * in rd_options).
     */
    partitionForm = (Form_pg_partition)palloc(PARTITION_TUPLE_SIZE);

    rc = memcpy_s(partitionForm, PARTITION_TUPLE_SIZE, partp, PARTITION_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");

    /* initialize relation tuple form */
    partition->pd_part = partitionForm;

    (void)MemoryContextSwitchTo(oldcxt);
    return partition;
}

StorageType PartitionGetStorageType(Partition partition, Oid parentOid)
{
    HeapTuple  pg_class_tuple;
    StorageType  storageType;
    bool  isNull = false;
    Datum datum;

    pg_class_tuple = ScanPgRelation(parentOid, true, false);
    Assert(HeapTupleIsValid(pg_class_tuple));

    /* fetch relbucketoid from pg_class tuple */
    datum = heap_getattr(pg_class_tuple,
                         Anum_pg_class_relbucket,
                         GetDefaultPgClassDesc(),
                         &isNull);
    if (isNull) {
        storageType = HEAP_DISK;
    } else {
        storageType = SEGMENT_PAGE;
    }

    heap_freetuple_ext(pg_class_tuple);

    return storageType;
}

Partition PartitionBuildDesc(Oid targetPartId, StorageType storage_type, bool insertIt)
{
    Partition partition;
    Oid partid;
    HeapTuple pg_partition_tuple;
    Form_pg_partition partp;

    /*
     * find the tuple in pg_class corresponding to the given relation id
     */
    pg_partition_tuple = ScanPgPartition(targetPartId, true, NULL);
    /*
     * if no such tuple exists, return NULL
     */
    if (!HeapTupleIsValid(pg_partition_tuple)) {
        return NULL;
    }

    /*
     * get information from the pg_class_tuple
     */
    partid = HeapTupleGetOid(pg_partition_tuple);
    partp = (Form_pg_partition)GETSTRUCT(pg_partition_tuple);
    Assert(partid == targetPartId);

    /*
     * allocate storage for the relation descriptor, and copy pg_partition_tuple
     * to partition->pd_part.
     */
    partition = AllocatePartitionDesc(partp);
    /*
     * initialize the partition's partition id (partition->pd_id)
     */
    partition->pd_id = partid;

    /*
     * normal relations are not nailed into the cache; nor can a pre-existing
     * relation be new.  It could be temp though.  (Actually, it could be new
     * too, but it's okay to forget that fact if forced to flush the entry.)
     */
    partition->pd_refcnt = 0;
    partition->pd_createSubid = InvalidSubTransactionId;
    partition->pd_newRelfilenodeSubid = InvalidSubTransactionId;

    PartitionParseRelOptions(partition, pg_partition_tuple);
    /*
     * initialize the relation lock manager information
     */
    PartitionInitLockInfo(partition); /* see lmgr.c */

    /*
     * initialize physical addressing information for the partition
     */
    if (partition->pd_part->parentid != InvalidOid) {
        PartitionInitPhysicalAddr(partition);
        Oid partitionTableOid = InvalidOid;
        if (partition->pd_part->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
            partitionTableOid = partid_get_parentid(partition->pd_part->parentid);
        } else {
            partitionTableOid = partition->pd_part->parentid;
        }
        if (storage_type == INVALID_STORAGE) {
            storage_type = PartitionGetStorageType(partition, partitionTableOid);
        }
    }

    /*
     * initialize storage type information for the partition
     */
    if (!PartitionIsPartitionedTable(partition) && storage_type == SEGMENT_PAGE) {
        partition->pd_node.bucketNode = SegmentBktId;
    } else {
        partition->pd_node.bucketNode = InvalidBktId;
    }

    /* make sure relation is marked as having no open file yet */
    partition->pd_smgr = NULL;

    /* make sure the partrel is empty yet */
    partition->partrel = NULL;

    /* Assign value in partitiongetrelation. */
    partition->partMap = NULL;

    if (IS_DISASTER_RECOVER_MODE) {
        TransactionId xmin = HeapTupleGetRawXmin(pg_partition_tuple);
        partition->xmin_csn = CSNLogGetDRCommitSeqNo(xmin);
    } else {
        partition->xmin_csn = InvalidCommitSeqNo;
    }

    /*
     * now we can free the memory allocated for pg_class_tuple
     */
    heap_freetuple_ext(pg_partition_tuple);
    /* It's fully valid */
    partition->pd_isvalid = true;
    /*
     * Insert newly created relation into partcache hash table, if requested.
     */
    if (insertIt) {
        PartitionIdCacheInsertIntoLocal(partition);
    }

    return partition;
}


void PartitionInitPhysicalAddr(Partition partition)
{
    partition->pd_node.spcNode = ConvertToRelfilenodeTblspcOid(partition->pd_part->reltablespace);
    if (partition->pd_node.spcNode == GLOBALTABLESPACE_OID) {
        partition->pd_node.dbNode = InvalidOid;
    } else {
        partition->pd_node.dbNode = u_sess->proc_cxt.MyDatabaseId;
    }

    if (partition->pd_part->relfilenode) {
        partition->pd_node.relNode = partition->pd_part->relfilenode;
    } else if (partition->pd_id) {
        partition->pd_node.relNode = partition->pd_id;
    } else {
        /* Consult the relation mapper */
        partition->pd_node.relNode = RelationMapOidToFilenode(partition->pd_id, false);
        if (!OidIsValid(partition->pd_node.relNode)) {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND),
                    errmsg("could not find relation mapping for partition \"%s\", OID %u",
                        PartitionGetPartitionName(partition),
                        partition->pd_id)));
        }
    }

    partition->pd_node.opt = 0;
    if (partition->rd_options) {
        SetupPageCompressForRelation(&partition->pd_node, &((StdRdOptions*)(partition->rd_options))->compress,
                                      PartitionGetPartitionName(partition));
    }
}

/*
 *part 3: functions can be used by  other modules
 *
 *
 */
Partition PartitionIdGetPartition(Oid partitionId, StorageType storage_type)
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->partdefcache.PartitionIdGetPartition(partitionId, storage_type);
    }
    Partition pd;
    /*
     * first try to find reldesc in the cache
     */
    PartitionIdCacheLookup(partitionId, pd);
    if (PartitionIsValid(pd)) {
        PartitionIncrementReferenceCount(pd);
        /* revalidate cache entry if necessary */
        if (!pd->pd_isvalid) {
            /*
             * Indexes only have a limited number of possible schema changes,
             * and we don't want to use the full-blown procedure because it's
             * a headache for indexes that reload itself depends on.
             */
            if (pd->pd_part->parttype == PART_OBJ_TYPE_INDEX_PARTITION) {
                PartitionReloadIndexInfo(pd);
            } else {
                PartitionClearPartition(pd, true);
            }
        }
        return pd;
    }

    /*
     * no partdesc in the cache, so have PartitionBuildDesc() build one and add
     * it.
     */
    pd = PartitionBuildDesc(partitionId, storage_type, true);
    if (PartitionIsValid(pd)) {
        PartitionIncrementReferenceCount(pd);
    }

    return pd;
}

char* PartitionOidGetName(Oid partOid)
{
    HeapTuple tuple = ScanPgPartition(partOid, true, NULL);
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }

    Form_pg_partition part = (Form_pg_partition)GETSTRUCT(tuple);
    char* relName = (char*)palloc0(NAMEDATALEN);
    error_t rc = strncpy_s(relName, NAMEDATALEN, part->relname.data, NAMEDATALEN - 1);
    securec_check_ss(rc, "\0", "\0");
    heap_freetuple_ext(tuple);

    return relName;
}

Oid PartitionOidGetTablespace(Oid partOid)
{
    HeapTuple tuple = ScanPgPartition(partOid, true, NULL);
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }

    Form_pg_partition part = (Form_pg_partition)GETSTRUCT(tuple);
    Oid tablespaceOid = part->reltablespace;
    heap_freetuple_ext(tuple);

    return tablespaceOid;
}

void PartitionClose(Partition partition)
{
    /* Note: no locking manipulations needed */
    PartitionDecrementReferenceCount(partition);

#ifdef PARTCACHE_FORCE_RELEASE
    if (PartitionHasReferenceCountZero(partition) && partition->pd_createSubid == InvalidSubTransactionId &&
        partition->pd_newRelfilenodeSubid == InvalidSubTransactionId) {
        PartitionClearPartition(partition, false);
    }
#endif
}

Partition PartitionBuildLocalPartition(const char *relname, Oid partid, Oid partfilenode, Oid parttablespace,
    StorageType storage_type, Datum reloptions)
{
    Partition part;
    MemoryContext oldcxt;

    /*
     * switch to the cache context to create the partcache entry.
     */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /*
     * allocate a new relation descriptor and fill in basic state fields.
     */
    part = (Partition)palloc0(sizeof(PartitionData));

    /* make sure relation is marked as having no open file yet */
    part->pd_smgr = NULL;
    part->pd_refcnt = 0;

    /* it's being created in this transaction */
    part->pd_createSubid = GetCurrentSubTransactionId();
    part->pd_newRelfilenodeSubid = InvalidSubTransactionId;

    /* must flag that we have rels created in this transaction */
    SetPartCacheNeedEOXActWork(true);

    /*
     * initialize partition tuple form (caller may add/override data later)
     */

    part->pd_part = (Form_pg_partition)palloc0(PARTITION_TUPLE_SIZE);

    (void)namestrcpy(&part->pd_part->relname, relname);
    /*
     * Insert relation physical and logical identifiers (OIDs) into the right
     * places.  For a mapped relation, we set relfilenode to zero and rely on
     * RelationInitPhysicalAddr to consult the map.
     */
    part->pd_id = partid;
    part->pd_part->reltablespace = parttablespace;

    part->pd_part->relfilenode = partfilenode;


    /*belowing: cast out from Partition to Relation*/
    PartitionInitLockInfo(part); /* see lmgr.c */

    if (partfilenode != InvalidOid) {
        PartitionInitPhysicalAddr(part);
        /* compressed option was set by PartitionInitPhysicalAddr if part->rd_options != NULL */
        if (part->rd_options == NULL && reloptions) {
            StdRdOptions* options = (StdRdOptions*)default_reloptions(reloptions, false, RELOPT_KIND_HEAP);
            SetupPageCompressForRelation(&part->pd_node, &options->compress, PartitionGetPartitionName(part));
        }
    }

    if (storage_type == SEGMENT_PAGE) {
        part->pd_node.bucketNode = SegmentBktId;
    } else {
        part->pd_node.bucketNode = InvalidBktId;
    }

    /* It's fully valid */
    part->pd_isvalid = true;
    /*
     * Okay to insert into the partcache hash tables.
     */
    PartitionIdCacheInsertIntoLocal(part);
    /*
     * done building partcache entry.
     */
    (void)MemoryContextSwitchTo(oldcxt);
    
    /*
     * Caller expects us to pin the returned entry.
     */
    PartitionIncrementReferenceCount(part);
    return part;
}

/*
 * PartitionDestroyPartition
 *
 *	Physically delete a partition cache entry and all subsidiary data.
 *	Caller must already have unhooked the entry from the hash table.
 */
void PartitionDestroyPartition(Partition partition)
{
    Assert(PartitionHasReferenceCountZero(partition));

    /*
     * Make sure smgr and lower levels close the partition's files, if they
     * weren't closed already.  (This was probably done by caller, but let's
     * just be real sure.)
     */
    PartitionCloseSmgr(partition);
    /*
     * Free all the subsidiary data structures of the partcache entry, then the
     * entry itself.
     */
    pfree_ext(partition->pd_indexattr);
    pfree_ext(partition->pd_part);
    list_free_ext(partition->pd_indexlist);
    pfree_ext(partition->rd_options);
    if (partition->partrel) {
        /* in function releaseDummyRelation, owner->nfakerelrefs decrease one due to ResourceOwnerForgetFakerelRef,
         * which is not we expect, so we use ResourceOwnerRememberFakerelRef correspondingly */
        if (!IsBootstrapProcessingMode()) {
            ResourceOwnerRememberFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, partition->partrel);
        }
        releaseDummyRelation(&partition->partrel);
    }
    if (partition->partMap != NULL) {
        RelationDestroyPartitionMap(partition->partMap);
        partition->partMap = NULL;
    }
    pfree_ext(partition);
}

#define SWAPFIELD(fldtype, fldname)            \
    do {                                       \
        fldtype _tmp = newpart->fldname;       \
        newpart->fldname = partition->fldname; \
        partition->fldname = _tmp;             \
    } while (0)
/*
 * PartitionClearPartition
 *
 *	 Physically blow away a partition cache entry, or reset it and rebuild
 *	 it from scratch (that is, from catalog entries).  The latter path is
 *	 used when we are notified of a change to an open relation (one with
 *	 refcount > 0).
 *
 *	 NB: when rebuilding, we'd better hold some lock on the relation,
 *	 else the catalog data we need to read could be changing under us.
 *	 Also, a rel to be rebuilt had better have refcnt > 0.	This is because
 *	 an sinval reset could happen while we're accessing the catalogs, and
 *	 the rel would get blown away underneath us by RelationCacheInvalidate
 *	 if it has zero refcnt.
 *
 *	 The "rebuild" parameter is redundant in current usage because it has
 *	 to match the relation's refcnt status, but we keep it as a crosscheck
 *	 that we're doing what the caller expects.
 */
void PartitionClearPartition(Partition partition, bool rebuild)
{
    /*
     * As per notes above, a rel to be rebuilt MUST have refcnt > 0; while of
     * course it would be a bad idea to blow away one with nonzero refcnt.
     */
    Assert(rebuild ? !PartitionHasReferenceCountZero(partition) : PartitionHasReferenceCountZero(partition));

    /*
     * Make sure smgr and lower levels close the partition's files, if they
     * weren't closed already.  If the partition is not getting deleted, the
     * next smgr access should reopen the files automatically.	This ensures
     * that the low-level file access state is updated after, say, a vacuum
     * truncation.
     */
    PartitionCloseSmgr(partition);

    /*
     * Never, never ever blow away a nailed-in system relation, because we'd
     * be unable to recover.  However, we must redo RelationInitPhysicalAddr
     * in case it is a mapped relation whose mapping changed.
     *
     * If it's a nailed index, then we need to re-read the pg_partition row to see
     * if its relfilenode changed.	We can't necessarily do that here, because
     * we might be in a failed transaction.  We assume it's okay to do it if
     * there are open references to the partcache entry (cf notes for
     * AtEOXact_RelationCache).  Otherwise just mark the entry as possibly
     * invalid, and it'll be fixed when next opened.
     */

    /*
     * Even non-system indexes should not be blown away if they are open and
     * have valid index support information.  This avoids problems with active
     * use of the index support information.  As with nailed indexes, we
     * re-read the pg_class row to handle possible physical relocation of the
     * index, and we check for pg_index updates too.
     */
    if (partition->pd_part->parttype == PART_OBJ_TYPE_INDEX_PARTITION && partition->pd_refcnt > 0) {
        partition->pd_isvalid = false; /* needs to be revalidated */
        PartitionReloadIndexInfo(partition);
        return;
    }

    /* Mark it invalid until we've finished rebuild */
    partition->pd_isvalid = false;

    /*
     * If we're really done with the partcache entry, blow it away. But if
     * someone is still using it, reconstruct the whole deal without moving
     * the physical PartitionData record (so that the someone's pointer is
     * still valid).
     */
    if (!rebuild) {
        /* Remove it from the hash table */
        PartitionIdCacheDeleteLocal(partition);

        /* And release storage */
        PartitionDestroyPartition(partition);
    } else {
        /*
         * Our strategy for rebuilding an open partcache entry is to build a
         * new entry from scratch, swap its contents with the old entry, and
         * finally delete the new entry (along with any infrastructure swapped
         * over from the old entry).  This is to avoid trouble in case an
         * error causes us to lose control partway through.  The old entry
         * will still be marked !rd_isvalid, so we'll try to rebuild it again
         * on next access.	Meanwhile it's not any less valid than it was
         * before, so any code that might expect to continue accessing it
         * isn't hurt by the rebuild failure.  (Consider for example a
         * subtransaction that ALTERs a table and then gets canceled partway
         * through the cache entry rebuild.  The outer transaction should
         * still see the not-modified cache entry as valid.)  The worst
         * consequence of an error is leaking the necessarily-unreferenced new
         * entry, and this shouldn't happen often enough for that to be a big
         * problem.
         *
         * When rebuilding an open partcache entry, we must preserve ref count,
         * rd_createSubid/rd_newRelfilenodeSubid, and rd_toastoid state.  Also
         * attempt to preserve the pg_class entry (rd_rel), tupledesc, and
         * rewrite-rule substructures in place, because various places assume
         * that these structures won't move while they are working with an
         * open partcache entry.  (Note: the refcount mechanism for tupledescs
         * might someday allow us to remove this hack for the tupledesc.)
         *
         * Note that this process does not touch CurrentResourceOwner; which
         * is good because whatever ref counts the entry may have do not
         * necessarily belong to that resource owner.
         */
        Partition newpart = NULL;
        Oid save_partid = PartitionGetPartid(partition);
        errno_t rc = 0;

        /* Build temporary entry, but don't link it into hashtable */
        int4 tempNode = partition->pd_node.bucketNode;
        if (EnableLocalSysCache()) {
            // call build means local doesnt contain relation or it it invalid, so search from global directly
            newpart = t_thrd.lsc_cxt.lsc->partdefcache.SearchPartitionFromGlobalCopy<false>(save_partid);
        }
        if (!RelationIsValid(newpart)) {
            newpart = PartitionBuildDesc(save_partid, INVALID_STORAGE, false);
        }

        if (NULL == newpart) {
            /* Should only get here if partition was deleted */
            PartitionIdCacheDeleteLocal(partition);
            PartitionDestroyPartition(partition);
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE), errmsg("partition %u deleted while still in use", save_partid)));
        }

        if (tempNode != partition->pd_node.bucketNode) {
            ereport(LOG, (errmsg("partition %u storage type changing from [%d] to [%d]", save_partid, tempNode,
                partition->pd_node.bucketNode)));
        }
        if (PartitionHasSubpartition(newpart) && newpart->pd_part->parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
            Oid parentOid = partid_get_parentid(newpart->pd_id);
            Relation rel = relation_open(parentOid, NoLock);
            Relation partRel = partitionGetRelation(rel, newpart);
            releaseDummyRelation(&partRel);
            relation_close(rel, NoLock);
        }
        /*
         * Perform swapping of the partcache entry contents.  Within this
         * process the old entry is momentarily invalid, so there *must* be no
         * possibility of CHECK_FOR_INTERRUPTS within this sequence. Do it in
         * all-in-line code for safety.
         *
         * Since the vast majority of fields should be swapped, our method is
         * to swap the whole structures and then re-swap those few fields we
         * didn't want swapped.
         */
        PartitionData tmpstruct;
        rc = memcpy_s(&tmpstruct, sizeof(PartitionData), newpart, sizeof(PartitionData));
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(newpart, sizeof(PartitionData), partition, sizeof(PartitionData));
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(partition, sizeof(PartitionData), &tmpstruct, sizeof(PartitionData));
        securec_check(rc, "\0", "\0");

        /* rd_smgr must not be swapped, due to back-links from smgr level */
        SWAPFIELD(SMgrRelation, pd_smgr);
        /* rd_refcnt must be preserved */
        SWAPFIELD(int, pd_refcnt);
        /* creation sub-XIDs must be preserved */
        SWAPFIELD(SubTransactionId, pd_createSubid);
        SWAPFIELD(SubTransactionId, pd_newRelfilenodeSubid);
        /* un-swap rd_rel pointers, swap contents instead */
        SWAPFIELD(Form_pg_partition, pd_part);
        /* ... but actually, we don't have to update newrel->rd_rel */

        rc = memcpy_s(partition->pd_part, PARTITION_TUPLE_SIZE, newpart->pd_part, PARTITION_TUPLE_SIZE);
        securec_check(rc, "\0", "\0");

        /* toast OID override must be preserved */
        SWAPFIELD(Oid, pd_toastoid);	
        /* pgstat_info must be preserved */
        SWAPFIELD(struct PgStat_TableStatus*, pd_pgstat_info);

        /* newcbi flag and its related information must be preserved */
        if (newpart->newcbi) {
            SWAPFIELD(bool, newcbi);
            partition->pd_node.bucketNode = newpart->pd_node.bucketNode;
        }

        if (newpart->partMap) {
            RebuildPartitonMap(newpart->partMap, partition->partMap);
            SWAPFIELD(PartitionMap*, partMap);
        }

        SWAPFIELD(LocalPartitionEntry*, entry);
#undef SWAPFIELD

        /* And now we can throw away the temporary entry */
        PartitionDestroyPartition(newpart);
    }
}

/*
 * PartitionFlushPartition
 *
 *	 Rebuild the partition if it is open (refcount > 0), else blow it away.
 */
static void PartitionFlushPartition(Partition partition)
{
    if (partition->pd_createSubid != InvalidSubTransactionId ||
        partition->pd_newRelfilenodeSubid != InvalidSubTransactionId) {
        /*
         * New partcache entries are always rebuilt, not flushed; else we'd
         * forget the "new" status of the partition, which is a useful
         * optimization to have.  Ditto for the new-relfilenode status.
         *
         * The rel could have zero refcnt here, so temporarily increment the
         * refcnt to ensure it's safe to rebuild it.  We can assume that the
         * current transaction has some lock on the rel already.
         */
        PartitionIncrementReferenceCount(partition);
        PartitionClearPartition(partition, true);
        PartitionDecrementReferenceCount(partition);
    } else {
        /*
         * Pre-existing parts can be dropped from the partcache if not open.
         */
        bool rebuild = !PartitionHasReferenceCountZero(partition);

        PartitionClearPartition(partition, rebuild);
    }
}

/*
 * PartitionForgetPartition - unconditionally remove a partcache entry
 *
 *		   External interface for destroying a partcache entry when we
 *		   drop the relation.
 */
void PartitionForgetPartition(Oid partid)
{
    Partition partition;

    PartitionIdCacheLookupOnlyLocal(partid, partition);

    if (!PointerIsValid(partition)) {
        return; /* not in cache, nothing to do */
    }

    if (!PartitionHasReferenceCountZero(partition)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("partition %u is still open", partid)));
    }

    /* Unconditionally destroy the partcache entry */
    PartitionClearPartition(partition, false);
}

/*
 *		RelationCacheInvalidateEntry
 *
 *		This routine is invoked for SI cache flush messages.
 *
 * Any relcache entry matching the relid must be flushed.  (Note: caller has
 * already determined that the relid belongs to our database or is a shared
 * relation.)
 *
 * We used to skip local relations, on the grounds that they could
 * not be targets of cross-backend SI update messages; but it seems
 * safer to process them, so that our *own* SI update messages will
 * have the same effects during CommandCounterIncrement for both
 * local and nonlocal relations.
 */
void PartitionCacheInvalidateEntry(Oid partitionId)
{
    Partition partition;

    PartitionIdCacheLookupOnlyLocal(partitionId, partition);

    if (PointerIsValid(partition)) {
        PartitionFlushPartition(partition);
    }
}

/*
 * RelationCacheInvalidate
 *	 Blow away cached relation descriptors that have zero reference counts,
 *	 and rebuild those with positive reference counts.	Also reset the smgr
 *	 relation cache and re-read relation mapping data.
 *
 *	 This is currently used only to recover from SI message buffer overflow,
 *	 so we do not touch new-in-transaction relations; they cannot be targets
 *	 of cross-backend SI updates (and our own updates now go through a
 *	 separate linked list that isn't limited by the SI message buffer size).
 *	 Likewise, we need not discard new-relfilenode-in-transaction hints,
 *	 since any invalidation of those would be a local event.
 *
 *	 We do this in two phases: the first pass deletes deletable items, and
 *	 the second one rebuilds the rebuildable items.  This is essential for
 *	 safety, because hash_seq_search only copes with concurrent deletion of
 *	 the element it is currently visiting.	If a second SI overflow were to
 *	 occur while we are walking the table, resulting in recursive entry to
 *	 this routine, we could crash because the inner invocation blows away
 *	 the entry next to be visited by the outer scan.  But this way is OK,
 *	 because (a) during the first pass we won't process any more SI messages,
 *	 so hash_seq_search will complete safely; (b) during the second pass we
 *	 only hold onto pointers to nondeletable entries.
 *
 *	 The two-phase approach also makes it easy to update relfilenodes for
 *	 mapped relations before we do anything else, and to ensure that the
 *	 second pass processes nailed-in-cache items before other nondeletable
 *	 items.  This should ensure that system catalogs are up to date before
 *	 we attempt to use them to reload information about other open relations.
 */

void PartitionCacheInvalidate(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->partdefcache.InvalidateAll();
        return;
    }
    HASH_SEQ_STATUS status;
    PartIdCacheEnt* idhentry = NULL;
    Partition partition;
    List* rebuildList = NIL;
    ListCell* l = NULL;

    /*
     * Reload relation mapping data before starting to reconstruct cache.
     */

    /* Phase 1 */
    hash_seq_init(&status, u_sess->cache_cxt.PartitionIdCache);

    while ((idhentry = (PartIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        partition = idhentry->partdesc;

        /* Must close all smgr references to avoid leaving dangling ptrs */
        PartitionCloseSmgr(partition);

        /* Ignore new relations, since they are never cross-backend targets */
        if (partition->pd_createSubid != InvalidSubTransactionId)
            continue;

        if (PartitionHasReferenceCountZero(partition)) {
            /* Delete this entry immediately */
            PartitionClearPartition(partition, false);
        } else {
            rebuildList = lappend(rebuildList, partition);
        }
    }

    /*
     * Now zap any remaining smgr cache entries.  This must happen before we
     * start to rebuild entries, since that may involve catalog fetches which
     * will re-open catalog files.
     */
    smgrcloseall();

    /* Phase 2: rebuild the items found to need rebuild in phase 1 */
    foreach (l, rebuildList) {
        partition = (Partition)lfirst(l);
        PartitionClearPartition(partition, true);
    }
    list_free_ext(rebuildList);
}

/*
 * RelationCloseSmgrByOid - close a relcache entry's smgr link
 *
 * Needed in some cases where we are changing a relation's physical mapping.
 * The link will be automatically reopened on next use.
 */
void PartitionCloseSmgrByOid(Oid partitionId)
{
    Partition partition;

    PartitionIdCacheLookupOnlyLocal(partitionId, partition);

    if (!PointerIsValid(partition)) {
        return; /* not in cache, nothing to do */
    }

    PartitionCloseSmgr(partition);
}

/*
 * AtEOXact_PartitionCache
 *
 *	Clean up the partitonCache at main-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 * In the case of abort, we don't want to try to rebuild any invalidated
 * cache entries (since we can't safely do database accesses).  Therefore
 * we must reset refcnts before handling pending invalidations.
 *
 * We also need to do special cleanup when the current transaction
 * created any partitions or made use of forced index lists.
 */
void AtEOXact_PartitionCache(bool isCommit)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->partdefcache.AtEOXact_PartitionCache(isCommit);
        return;
    }
    HASH_SEQ_STATUS status;
    PartIdCacheEnt* idhentry = NULL;

    /*
     * To speed up transaction exit, we want to avoid scanning the partitioncache
     * unless there is actually something for this routine to do.  Other than
     * the debug-only Assert checks, most transactions don't create any work
     * for us to do here, so we keep a static flag that gets set if there is
     * anything to do.	(Currently, this means either a partition is created in
     * the current xact, or one is given a new relfilenode, or an index list
     * is forced.)	For simplicity, the flag remains set till end of top-level
     * transaction, even though we could clear it at subtransaction end in
     * some cases.
     */
    if (!GetPartCacheNeedEOXActWork()
#ifdef USE_ASSERT_CHECKING
        && !assert_enabled
#endif
    ) {
        return;
    }

    hash_seq_init(&status, u_sess->cache_cxt.PartitionIdCache);

    while ((idhentry = (PartIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Partition partition = idhentry->partdesc;

        /*
         * The relcache entry's ref count should be back to its normal
         * not-in-a-transaction state: 0 unless it's nailed in cache.
         *
         * In bootstrap mode, this is NOT true, so don't check it --- the
         * bootstrap code expects relations to stay open across start/commit
         * transaction calls.  (That seems bogus, but it's not worth fixing.)
         */
#ifdef USE_ASSERT_CHECKING
        if (!IsBootstrapProcessingMode()) {
            const int expected_refcnt = 0;
            Assert(partition->pd_refcnt == expected_refcnt);
        }
#endif

        /*
         * Is it a partition created in the current transaction?
         *
         * During commit, reset the flag to zero, since we are now out of the
         * creating transaction.  During abort, simply delete the relcache
         * entry --- it isn't interesting any longer.  (NOTE: if we have
         * forgotten the new-ness of a new relation due to a forced cache
         * flush, the entry will get deleted anyway by shared-cache-inval
         * processing of the aborted pg_class insertion.)
         */
        if (partition->pd_createSubid != InvalidSubTransactionId) {
            if (isCommit) {
                partition->pd_createSubid = InvalidSubTransactionId;
            } else {
                PartitionClearPartition(partition, false);
                continue;
            }
        }

        /*
         * Likewise, reset the hint about the relfilenode being new.
         */
        partition->pd_newRelfilenodeSubid = InvalidSubTransactionId;
    }

    /* Once done with the transaction, we can reset need_eoxact_work */
    SetPartCacheNeedEOXActWork(false);
}

/*
 * AtEOSubXact_RelationCache
 *
 *	Clean up the partitioncache at sub-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 */
void AtEOSubXact_PartitionCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->partdefcache.AtEOSubXact_PartitionCache(isCommit, mySubid, parentSubid);
        return;
    }
    HASH_SEQ_STATUS status;
    PartIdCacheEnt* idhentry = NULL;

    /*
     * Skip the relcache scan if nothing to do --- see notes for
     * AtEOXact_PartitionCache.
     */
    if (!GetPartCacheNeedEOXActWork())
        return;

    hash_seq_init(&status, u_sess->cache_cxt.PartitionIdCache);

    while ((idhentry = (PartIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Partition partition = idhentry->partdesc;

        /*
         * Is it a partition created in the current subtransaction?
         *
         * During subcommit, mark it as belonging to the parent, instead.
         * During subabort, simply delete the partition entry.
         */
        if (partition->pd_createSubid == mySubid) {
            if (isCommit)
                partition->pd_createSubid = parentSubid;
            else {
                PartitionClearPartition(partition, false);
                continue;
            }
        }

        /*
         * Likewise, update or drop any new-relfilenode-in-subtransaction
         * hint.
         */
        if (partition->pd_newRelfilenodeSubid == mySubid) {
            if (isCommit)
                partition->pd_newRelfilenodeSubid = parentSubid;
            else
                partition->pd_newRelfilenodeSubid = InvalidSubTransactionId;
        }
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: This initializes the relation descriptor cache
 * Description	: At the time that this is invoked, we can't do database access yet (mainly
 *			: because the transaction subsystem is not up); all we are doing is making
 *			: an empty cache hashtable.  This must be done before starting the initialization
 *			: transaction, because otherwise AtEOXact_RelationCache would crash if that
 *			: transaction aborts before we can get the relcache set up.
 * Notes		:
 */

void PartitionCacheInitialize(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->partdefcache.Init();
        return;
    }
    HASHCTL ctl;
    errno_t rc;

    /*
     * create hashtable that indexes the partcache
     */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");

    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(PartIdCacheEnt);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    const int INITPARTCACHESIZE = 128;
    u_sess->cache_cxt.PartitionIdCache =
        hash_create("Partcache by OID", INITPARTCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/* ----------------------------------------------------------------
 *				cache invalidation support routines
 * ----------------------------------------------------------------
 */

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Increments partition reference count.
 * Description	:
 * Notes		: bootstrap mode has its own weird ideas about relation refcount
 *			: behavior; we ought to fix it someday, but for now, just disable
 *			: reference count ownership tracking in bootstrap mode.
 */
void PartitionIncrementReferenceCount(Partition part)
{
    ResourceOwnerEnlargePartitionRefs(t_thrd.utils_cxt.CurrentResourceOwner);
    part->pd_refcnt += 1;
    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerRememberPartitionRef(t_thrd.utils_cxt.CurrentResourceOwner, part);
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Decrements relation reference count.
 * Description	:
 * Notes		:
 */
void PartitionDecrementReferenceCount(Partition part)
{
    Assert(part->pd_refcnt > 0);
    part->pd_refcnt -= 1;

    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerForgetPartitionRef(t_thrd.utils_cxt.CurrentResourceOwner, part);
    }
}

bytea* merge_rel_part_reloption(Oid rel_oid, Oid part_oid)
{
    HeapTuple part_tuple = NULL, rel_tuple = NULL;
    Datum rel_reloptions = (Datum)0;
    Datum part_reloptions = (Datum)0;
    Datum merged_reloptions = (Datum)0;

    List* rel_reloptions_list = NIL;
    List* part_reloptions_list = NIL;
    List* merged_reloptions_list = NIL;
    ListCell* lc = NULL;

    bytea* merged_rd_options = NULL;
    bool isnull = false;

    /* get tuples */
    part_tuple = SearchSysCache1WithLogLevel(PARTRELID, ObjectIdGetDatum(part_oid), LOG);
    if (!HeapTupleIsValid(part_tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", part_oid)));
    part_reloptions = SysCacheGetAttr(PARTRELID, part_tuple, Anum_pg_partition_reloptions, &isnull);

    rel_tuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(rel_oid), LOG);
    if (!HeapTupleIsValid(rel_tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", rel_oid)));
    rel_reloptions = SysCacheGetAttr(RELOID, rel_tuple, Anum_pg_class_reloptions, &isnull);

    /* datum ==> list */
    rel_reloptions_list = untransformRelOptions(rel_reloptions);
    part_reloptions_list = untransformRelOptions(part_reloptions);

    ReleaseSysCache(part_tuple);
    ReleaseSysCache(rel_tuple);

    if (part_reloptions_list == NIL || list_length(part_reloptions_list) == 0) {
        merged_reloptions_list = rel_reloptions_list;
    } else {
        foreach (lc, rel_reloptions_list) {
            DefElem* d = (DefElem*)lfirst(lc);
            ListCell* cell = NULL;
            int i = 0;

            foreach (cell, part_reloptions_list) {
                DefElem* d2 = (DefElem*)lfirst(cell);
                if (pg_strncasecmp(d->defname, d2->defname, strlen(d->defname)) == 0) {
                    merged_reloptions_list = lappend(merged_reloptions_list, (void*)d2);
                    break;
                }
                i++;
            }

            if (i == list_length(part_reloptions_list)) {
                merged_reloptions_list = lappend(merged_reloptions_list, (void*)d);
            }
        }
    }

    /* list ==> datum */
    merged_reloptions = transformRelOptions((Datum)0, merged_reloptions_list, NULL, NULL, false, false);

    /* datum ==> bytea * */
    merged_rd_options = heap_reloptions(RELKIND_RELATION, merged_reloptions, true);

    return merged_rd_options;
}

void UpdatePartrelPointer(Relation partrel, Relation rel, Partition part)
{
    Assert(partrel->rd_refcnt == part->pd_refcnt);
    Assert(partrel->rd_isvalid == part->pd_isvalid);
    Assert(partrel->rd_indexvalid == part->pd_indexvalid);
    Assert(partrel->rd_createSubid == part->pd_createSubid);
    Assert(partrel->rd_newRelfilenodeSubid == part->pd_newRelfilenodeSubid);

    Assert(partrel->rd_rel->reltoastrelid == part->pd_part->reltoastrelid);
    Assert(partrel->rd_rel->reltablespace == part->pd_part->reltablespace);
    Assert(partrel->rd_rel->relfilenode == part->pd_part->relfilenode);
    Assert(partrel->rd_rel->relpages == part->pd_part->relpages);
    Assert(partrel->rd_rel->reltuples == part->pd_part->reltuples);
    Assert(partrel->rd_rel->relallvisible == part->pd_part->relallvisible);
    Assert(partrel->rd_rel->relcudescrelid == part->pd_part->relcudescrelid);
    Assert(partrel->rd_rel->relcudescidx == part->pd_part->relcudescidx);
    Assert(partrel->rd_rel->reldeltarelid == part->pd_part->reldeltarelid);
    Assert(partrel->rd_rel->reldeltaidx == part->pd_part->reldeltaidx);
    Assert(partrel->rd_bucketoid == rel->rd_bucketoid);

    partrel->rd_att = rel->rd_att;
    Assert(partrel->rd_partHeapOid == part->pd_part->indextblid);
    partrel->rd_index = rel->rd_index;
    partrel->rd_indextuple = rel->rd_indextuple;
    partrel->rd_am = rel->rd_am;
    partrel->rd_indnkeyatts = rel->rd_indnkeyatts;
    Assert(partrel->rd_tam_type == rel->rd_tam_type);

    partrel->rd_aminfo = rel->rd_aminfo;
    partrel->rd_opfamily = rel->rd_opfamily;
    partrel->rd_opcintype = rel->rd_opcintype;
    partrel->rd_support = rel->rd_support;
    partrel->rd_supportinfo = rel->rd_supportinfo;

    partrel->rd_indoption = rel->rd_indoption;
    partrel->rd_indexprs = rel->rd_indexprs;
    partrel->rd_indpred = rel->rd_indpred;
    partrel->rd_exclops = rel->rd_exclops;
    partrel->rd_exclprocs = rel->rd_exclprocs;
    partrel->rd_exclstrats = rel->rd_exclstrats;

    partrel->rd_amcache = rel->rd_amcache;
    partrel->rd_indcollation = rel->rd_indcollation;
    Assert(partrel->rd_id == part->pd_id);
    partrel->rd_indexlist = part->pd_indexlist;
    Assert(partrel->rd_oidindex == part->pd_oidindex);
    Assert(partrel->rd_toastoid == part->pd_toastoid);
    Assert(partrel->subpartitiontype == part->pd_part->parttype);
    partrel->pgstat_info = part->pd_pgstat_info;
    Assert(partrel->parentId == rel->rd_id);
    Assert(partrel->rd_isblockchain == rel->rd_isblockchain);
    Assert(partrel->storage_type == rel->storage_type);
}

static void SetRelationPartitionMap(Relation relation, Partition part)
{
    if (!(PartitionHasSubpartition(part) && part->pd_part->parttype == PART_OBJ_TYPE_TABLE_PARTITION)) {
        return;
    }
    if (part->partMap != NULL) {
        relation->partMap = part->partMap;
    } else {
        RelationInitPartitionMap(relation, true);
        part->partMap = relation->partMap;
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		: the invoker should release the EMS memory
 */

Relation partitionGetRelation(Relation rel, Partition part)
{
    Relation relation;
    MemoryContext oldcxt;
    errno_t rc = 0;
    bytea* merge_reloption = NULL;
    bytea* des_reloption = NULL;

    Assert(PointerIsValid(rel) && PointerIsValid(part));
    /*
     * If the rel is subpartitiontable and the part is subpartition, we need open Level 1 partition to get subpartition
     * relation. When the caller gets subpartition, The level-1 partition has been locked. Therefore, partitionOpen used
     * NoLock here.
     */
    if (RelationIsSubPartitioned(rel) && rel->rd_id != part->pd_part->parentid) {
        Assert(rel->rd_id == partid_get_parentid(part->pd_part->parentid));
        Partition parentPart = partitionOpen(rel, part->pd_part->parentid, NoLock);
        Relation parentPartRel = partitionGetRelation(rel, parentPart);
        relation = partitionGetRelation(parentPartRel, part);
        releaseDummyRelation(&parentPartRel);
        partitionClose(rel, parentPart, NoLock);
        return relation;
    }
    Assert(rel->rd_id == part->pd_part->parentid);

    /*
     * Memory malloced in merge_rel_part_reloption cannot mount in CacheMemoryContext,
     * the same is true for other memory in this function and these may be optimized later.
     */
    if (RelationInClusterResizing(rel)) {
        /* tuple.column(datum) ==> list ==> datum ==> bytea * */
        merge_reloption = merge_rel_part_reloption(RelationGetRelid(rel), PartitionGetPartid(part));
    }

    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation = (Relation)palloc0(sizeof(RelationData));
    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerRememberFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, relation);
    }

    relation->rd_node = part->pd_node;
    relation->rd_refcnt = part->pd_refcnt;
    relation->rd_backend = InvalidBackendId;
    relation->rd_isnailed = false;
    relation->rd_isvalid = part->pd_isvalid;
    relation->rd_indexvalid = part->pd_indexvalid;
    relation->rd_createSubid = part->pd_createSubid;
    relation->rd_newRelfilenodeSubid = part->pd_newRelfilenodeSubid;
    relation->rd_rel = (Form_pg_class)palloc(sizeof(FormData_pg_class));
    rc = memcpy_s(relation->rd_rel, sizeof(FormData_pg_class), rel->rd_rel, sizeof(FormData_pg_class));
    securec_check(rc, "\0", "\0");
    relation->rd_rel->reltoastrelid = part->pd_part->reltoastrelid;
    relation->rd_rel->reltablespace = part->pd_part->reltablespace;
    if (PartitionHasSubpartition(part))
        relation->rd_rel->parttype = PARTTYPE_PARTITIONED_RELATION;
    else
        relation->rd_rel->parttype = PARTTYPE_NON_PARTITIONED_RELATION;
    relation->rd_rel->relfilenode = part->pd_part->relfilenode;
    relation->rd_rel->relpages = part->pd_part->relpages;
    relation->rd_rel->reltuples = part->pd_part->reltuples;
    relation->rd_rel->relallvisible = part->pd_part->relallvisible;
    relation->rd_rel->relcudescrelid = part->pd_part->relcudescrelid;
    relation->rd_rel->relcudescidx = part->pd_part->relcudescidx;
    relation->rd_rel->reldeltarelid = part->pd_part->reldeltarelid;
    relation->rd_rel->reldeltaidx = part->pd_part->reldeltaidx;
    relation->rd_bucketoid = rel->rd_bucketoid;
    if (REALTION_BUCKETKEY_INITED(rel))
        relation->rd_bucketkey = rel->rd_bucketkey;
    else
        relation->rd_bucketkey = NULL;
    relation->rd_att = rel->rd_att;
    relation->rd_partHeapOid = part->pd_part->indextblid;
    relation->rd_index = rel->rd_index;
    relation->rd_indextuple = rel->rd_indextuple;
    relation->rd_am = rel->rd_am;
    relation->rd_indnkeyatts = rel->rd_indnkeyatts;
	relation->rd_tam_type = rel->rd_tam_type;

    if (!OidIsValid(rel->rd_rel->relam)) {
        relation->rd_indexcxt = NULL;
    } else {
        Assert(rel->rd_indexcxt != NULL);
        relation->rd_indexcxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(),
            PartitionGetPartitionName(part),
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
    }

    relation->rd_aminfo = rel->rd_aminfo;
    relation->rd_opfamily = rel->rd_opfamily;
    relation->rd_opcintype = rel->rd_opcintype;
    relation->rd_support = rel->rd_support;
    relation->rd_supportinfo = rel->rd_supportinfo;

    relation->rd_indoption = rel->rd_indoption;
    relation->rd_indexprs = rel->rd_indexprs;
    relation->rd_indpred = rel->rd_indpred;
    relation->rd_exclops = rel->rd_exclops;
    relation->rd_exclprocs = rel->rd_exclprocs;
    relation->rd_exclstrats = rel->rd_exclstrats;

    relation->rd_amcache = rel->rd_amcache;
    relation->rd_indcollation = rel->rd_indcollation;
    relation->rd_id = part->pd_id;
    relation->rd_indexlist = part->pd_indexlist;
    relation->rd_oidindex = part->pd_oidindex;
    relation->rd_lockInfo = part->pd_lockInfo;
    relation->rd_toastoid = part->pd_toastoid;
    relation->partMap = NULL;
    relation->subpartitiontype = part->pd_part->parttype;
    relation->pgstat_info = part->pd_pgstat_info;
    relation->parentId = rel->rd_id;
    if (part->pd_part->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
        relation->grandparentId = rel->parentId;
    } else {
        relation->grandparentId = InvalidOid;
    }
    relation->rd_smgr = part->pd_smgr;
    relation->rd_isblockchain = rel->rd_isblockchain;
    relation->storage_type = rel->storage_type;

    /*detach the binding between partition and SmgrRelation*/
    part->pd_smgr = NULL;

    /*build the binding between dummy Relation and SmgrRelation*/
    if (relation->rd_smgr) {
        smgrsetowner(&((relation)->rd_smgr), relation->rd_smgr);
    }

    if (NULL != merge_reloption)
        des_reloption = merge_reloption;
    else
        des_reloption = rel->rd_options;

    if (NULL != des_reloption) {
        int relOptSize = VARSIZE_ANY(des_reloption);
        errno_t ret = EOK;
        relation->rd_options = (bytea*)palloc(relOptSize);
        ret = memcpy_s(relation->rd_options, relOptSize, des_reloption, relOptSize);
        securec_check(ret, "\0", "\0");
    }

    SetRelationPartitionMap(relation, part);

    (void)MemoryContextSwitchTo(oldcxt);

    return relation;
}

/*
 * NOTICE: caller MUST be sure relation parameter is a temprary RelationData, which is NOT in relcache.
 */
void releaseDummyRelation(Relation* relation)
{
    if (relation == NULL || *relation == NULL) {
        elog(LOG, "error parameter when release fake relation");
        return;
    }
    if (!IsBootstrapProcessingMode()) {
        ResourceOwnerForgetFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, *relation);
    }

    /*detach the binding between Relation and SmgrRelation*/
    if ((*relation)->rd_smgr != NULL) {
        /* put SmgrRelation object into unowned list */
        if (RelationIsBucket(*relation)) {
            smgrclose((*relation)->rd_smgr);
        } else {
            smgrclearowner(&(*relation)->rd_smgr, (*relation)->rd_smgr);
        }
    }

    if ((*relation)->rd_indexcxt != NULL) {
        MemoryContextDelete((*relation)->rd_indexcxt);
        (*relation)->rd_indexcxt = NULL;
    }

    /*free relation*/
    /*if palloc(sizeof(FormData_pg_class)) throw exception, (*relation)->rd_rel will be null*/
    if (PointerIsValid((*relation)->rd_rel)) {
        pfree_ext((*relation)->rd_rel);
    }

    if (NULL != (*relation)->rd_options) {
        pfree_ext((*relation)->rd_options);
    }

    pfree_ext(*relation);
    *relation = NULL;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: reload minimal information for an open  index partition
 * Description	:
 */
void PartitionReloadIndexInfo(Partition part)
{
    HeapTuple pg_partition_tuple;
    Form_pg_partition partForm;
    errno_t rc = 0;

    /*
     * Should be called only for invalidated indexe partition
     */
    Assert(PART_OBJ_TYPE_INDEX_PARTITION == part->pd_part->parttype && !part->pd_isvalid);

    /*
     * Should be closed at smgr level
     */
    Assert(NULL == part->pd_smgr);

    pg_partition_tuple = ScanPgPartition(PartitionGetPartid(part), true, NULL);
    if (!HeapTupleIsValid(pg_partition_tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA),
                errmsg("could not find pg_partition tuple for index %u", PartitionGetPartid(part))));
    }

    if (part->rd_options) {
        pfree_ext(part->rd_options);
    }

    PartitionParseRelOptions(part, pg_partition_tuple);

    partForm = (Form_pg_partition)GETSTRUCT(pg_partition_tuple);

    rc = memcpy_s(part->pd_part, PARTITION_TUPLE_SIZE, partForm, PARTITION_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");

    heap_freetuple_ext(pg_partition_tuple);

    /* We must recalculate physical address in case it changed 
     * bucketNode will not change anyway 
     */
    PartitionInitPhysicalAddr(part);

    /*
     * we can't read value from pg_index, we should read value from pg_partition
     * or a other catalog for partition index but indisvalid  indcheckxmin indisready
     * is not been added.
     */
    part->pd_isvalid = true;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Assign a new relfilenode (physical file name) to the partition.
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
void PartitionSetNewRelfilenode(Relation parent, Partition part, TransactionId freezeXid, MultiXactId freezeMultiXid)
{
    Oid newrelfilenode;
    RelFileNodeBackend newrnode;
    Relation pg_partition;
    HeapTuple tuple;
    HeapTuple ntup;
    Form_pg_partition partform;
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    bool replaces[Natts_pg_partition];
    errno_t rc;
    bool isbucket;

    Assert((parent->rd_rel->relkind == RELKIND_INDEX || RELKIND_IS_SEQUENCE(parent->rd_rel->relkind))
               ? freezeXid == InvalidTransactionId
               : TransactionIdIsNormal(freezeXid));

    /* Allocate a new relfilenode */
    if (RelationGetStorageType(parent) == (uint4)HEAP_DISK) {
        newrelfilenode = GetNewRelFileNode(part->pd_part->reltablespace, NULL, parent->rd_rel->relpersistence);
    } else {
        /* segment storage */
        Assert(parent->storage_type == SEGMENT_PAGE);
        isbucket = BUCKET_OID_IS_VALID(parent->rd_bucketoid) && !RelationIsCrossBucketIndex(parent);
        newrelfilenode = seg_alloc_segment(ConvertToRelfilenodeTblspcOid(part->pd_part->reltablespace),
                                           u_sess->proc_cxt.MyDatabaseId, isbucket, InvalidBlockNumber);
    }


    /*
     * Get a writable copy of the pg_partition tuple for the given relation.
     */
    pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(PartitionGetPartid(part)));

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("could not find tuple for partition %u", PartitionGetPartid(part))));
    }
    partform = (Form_pg_partition)GETSTRUCT(tuple);

    // CStore Relation must deal with cudesc relation, delta relation
    if (RelationIsColStore(parent)) {
        // step 1: CUDesc relation must set new relfilenode
        // step 2: CUDesc index must be set new relfilenode
        //
        DescTableSetNewRelfilenode(part->pd_part->relcudescrelid, freezeXid, true);

        // Step 3: Deta relation must be set new relfilenode
        //
        DeltaTableSetNewRelfilenode(part->pd_part->reldeltarelid, freezeXid, true);

        // Step 4: Create first data file for newrelfilenode
        // Note that we need add xlog when create file
        //
        Relation partRel = partitionGetRelation(parent, part);
        CStore::CreateStorage(partRel, newrelfilenode);
        releaseDummyRelation(&partRel);
    }

    ereport(LOG,
        (errmsg("Partition %s(%u) set newfilenode %u oldfilenode %u xid %lu",
            PartitionGetPartitionName(part),
            PartitionGetPartid(part),
            newrelfilenode,
            part->pd_node.relNode,
            GetCurrentTransactionIdIfAny())));

    /*
     * Create storage for the main fork of the new relfilenode.
     *
     * NOTE: any conflict in relfilenode value will be caught here, if
     * GetN
     * ewRelFileNode messes up for any reason.
     */
    newrnode.node = part->pd_node;
    newrnode.node.relNode = newrelfilenode;
    newrnode.backend = parent->rd_backend;

    if (RelationIsCrossBucketIndex(parent)) {
        part->newcbi = true;
    }

    partition_create_new_storage(parent, part, newrnode);

    Assert(!((part)->pd_part->relfilenode == InvalidOid));
    partform->relfilenode = newrelfilenode;

    Assert(!RELKIND_IS_SEQUENCE(parent->rd_rel->relkind));
    partform->relpages = 0; /* it's empty until further notice */
    partform->reltuples = 0;
    partform->relallvisible = 0;

    /* set relfrozenxid64 */
    partform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
    values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(freezeXid);

#ifndef ENABLE_MULTIPLE_NODES
    replaces[Anum_pg_partition_relminmxid - 1] = true;
    values[Anum_pg_partition_relminmxid - 1] = TransactionIdGetDatum(freezeMultiXid);
#endif

    ntup = heap_modify_tuple(tuple, RelationGetDescr(pg_partition), values, nulls, replaces);

    simple_heap_update(pg_partition, &ntup->t_self, ntup);
    CatalogUpdateIndexes(pg_partition, ntup);

    heap_freetuple_ext(ntup);
    heap_freetuple_ext(tuple);

    heap_close(pg_partition, RowExclusiveLock);

    CommandCounterIncrement();

    /*
     * Mark the part as having been given a new relfilenode in the current
     * (sub) transaction.  This is a hint that can be used to optimize later
     * operations on the rel in the same transaction.
     */
    part->pd_newRelfilenodeSubid = GetCurrentSubTransactionId();

    /* ... and now we have eoxact cleanup work to do */
    SetPartCacheNeedEOXActWork(true);
}

static void PartitionParseRelOptions(Partition partition, HeapTuple tuple)
{
    bytea* options = NULL;
    bool isnull = false;
    Datum datum;
    Relation partitionRel;
    errno_t rc;

    partition->rd_options = NULL;

    partitionRel = relation_open(PartitionRelationId, RowExclusiveLock);
    /*
     * Fetch reloptions from tuple; have to use a hardwired descriptor because
     * we might not have any other for pg_class yet (consider executing this
     * code for pg_class itself)
     */
    datum = fastgetattr(tuple, Anum_pg_partition_reloptions, RelationGetDescr(partitionRel), &isnull);

    /*close pg_partition catalog*/
    relation_close(partitionRel, RowExclusiveLock);

    if (isnull)
        return;

    options = heap_reloptions(RELKIND_RELATION, datum, false);

    /*
     * Copy parsed data into LocalMyDBCacheMemCxt().  To guard against the
     * possibility of leaks in the reloptions code, we want to do the actual
     * parsing in the caller's memory context and copy the results into
     * LocalMyDBCacheMemCxt() after the fact.
     */
    if (options != NULL) {
        partition->rd_options = (bytea*)MemoryContextAlloc(LocalMyDBCacheMemCxt(), VARSIZE(options));
        rc = memcpy_s(partition->rd_options, VARSIZE(options), options, VARSIZE(options));
        securec_check(rc, "", "");
        pfree_ext(options);
    }

    return;
}

/* Check one partition whether it is normal use, and save in liveParts */
static bool PartitionStatusIsLive(Oid partOid, OidRBTree** liveParts)
{
    HeapTuple partTuple = NULL;

    if (OidRBTreeMemberOid(*liveParts, partOid)) {
        return true;
    }

    /* Get partition information from syscache */
    partTuple = SearchSysCache1WithLogLevel(PARTRELID, ObjectIdGetDatum(partOid), LOG);
    if (HeapTupleIsValid(partTuple)) {
        ReleaseSysCache(partTuple);
        (void)OidRBTreeInsertOid(*liveParts, partOid);
        return true;
    }

    return false;
}

/* Check one invisible partition whether enable clean */
static bool InvisblePartEnableClean(HeapTuple partTuple, TupleDesc tupleDesc)
{
    Datum partOptions;
    bool isNull = false;

    partOptions = fastgetattr(partTuple, Anum_pg_partition_reloptions, tupleDesc, &isNull);
    if (isNull || !PartitionInvisibleMetadataKeep(partOptions)) {
        return true;
    }

    return false;
}

/* Just for lazy vacuum get one partition's status */
static PartStatus PartTupleStatusForVacuum(HeapTuple partTuple, Buffer buffer, TransactionId oldestXmin)
{
    PartStatus partStatus = PART_METADATA_NOEXIST;

    /*
     * We could possibly get away with not locking the buffer here,
     * since caller should hold ShareLock on the relation, but let's
     * be conservative about it.  (This remark is still correct even
     * with HOT-pruning: our pin on the buffer prevents pruning.)
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    switch (HeapTupleSatisfiesVacuum(partTuple, oldestXmin, buffer)) {
        case HEAPTUPLE_INSERT_IN_PROGRESS:
        case HEAPTUPLE_DELETE_IN_PROGRESS:
            partStatus = PART_METADATA_CREATING;
            break;
        case HEAPTUPLE_LIVE:
            partStatus = PART_METADATA_LIVE;
            break;
        case HEAPTUPLE_DEAD:
        case HEAPTUPLE_RECENTLY_DEAD:
            partStatus = PART_METADATA_INVISIBLE;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("unexpected HeapTupleSatisfiesVacuum result")));
            partStatus = PART_METADATA_NOEXIST; /* keep compiler quiet */
            break;
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    return partStatus;
}

/*
 * Check current partition status use HeapTupleSatisfiesVacuum
 *
 * Notes: return PART_METADATA_CEATING scenario occurs only in the process of automatically creating
 * partitions when the interval partition insert statement is executed, Other partition
 * change scenarios have AccessExclusiveLock locks, which are not executed concurrently
 * with the vacuum process
 */
static PartStatus PartitionStatusForVacuum(Oid partOid)
{
    Relation pgPartition = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[1];
    HeapTuple partTuple = NULL;
    TransactionId oldestXmin;
    PartStatus partStatus = PART_METADATA_NOEXIST;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    oldestXmin = u_sess->utils_cxt.RecentGlobalXmin;
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partOid));

    scan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 1, key);
    while (HeapTupleIsValid(partTuple = systable_getnext(scan))) {
        partStatus = PartTupleStatusForVacuum(partTuple, scan->scan->rs_base.rs_cbuf, oldestXmin);
        /* The status of a partition is creating or live, the partition status is the latest */
        if (partStatus == PART_METADATA_CREATING || partStatus == PART_METADATA_LIVE) {
            break;
        }
    }
    systable_endscan(scan);
    heap_close(pgPartition, NoLock);

    return partStatus;
}

/*
 * This function is used by global partition index to determine whether
 * the partition corresponding to the partoid in index tuple should be ignored,
 * The scenarios are as follows:
 * a partition is created in a transaction and data is inserted into the partition,
 * However, the transaction is aborted. Alternatively,
 * a partition is created in a transaction, data is inserted into the partition,
 * and the partition is deleted. The transaction is committed.
 *
 * Notes: In this case, lazy_vacuum of pg_partition must meet the following requirements:
 * Before clearing a dead tuple, ensure that global partition index (if any) does not contain
 * any indextuple containing partoid of the dead tuple.
 */
PartStatus PartitionGetMetadataStatus(Oid partOid, bool vacuumFlag)
{
    HeapTuple partTuple;

    /* Get partition information from syscache */
    partTuple = SearchSysCache1WithLogLevel(PARTRELID, ObjectIdGetDatum(partOid), LOG);
    if (HeapTupleIsValid(partTuple)) {
        ReleaseSysCache(partTuple);
        return PART_METADATA_LIVE;
    }

    /* When vacuum is performed, must checks whether the partition is being created */
    if (vacuumFlag) {
        return PartitionStatusForVacuum(partOid);
    }

    /*
     * Find the tuple in pg_partition corresponding to the given partition oid
     *
     * Notes: use SnapshotAny to ensure that the tuple of pg_partition
     * in the invisible state is obtained.
     */
    partTuple = ScanPgPartition(partOid, false, SnapshotAny);
    /* If get tuple exists, return status invisible */
    if (HeapTupleIsValid(partTuple)) {
        pfree_ext(partTuple);
        return PART_METADATA_INVISIBLE;
    }

    return PART_METADATA_NOEXIST;
}

/* Set reloptions wait_clean_gpi, Just for pg_partition's tuple */
Datum SetWaitCleanGpiRelOptions(Datum oldOptions, bool enable)
{
    Datum newOptions;
    List* defList = NIL;
    DefElem* def = NULL;
    Value* defArg = enable ? makeString(OptEnabledWaitCleanGpi) : makeString(OptDisabledWaitCleanGpi);
    def = makeDefElem(pstrdup("wait_clean_gpi"), (Node*)defArg);
    defList = lappend(defList, def);
    newOptions = transformRelOptions(oldOptions, defList, NULL, NULL, false, false);
    pfree_ext(def->defname);
    list_free_ext(defList);

    return newOptions;
}

/* Update pg_partition's tuple attribute reloptions wait_clean_gpi */
void UpdateWaitCleanGpiRelOptions(Relation pgPartition, HeapTuple partTuple, bool enable, bool inplace)
{
    HeapTuple newTuple;
    Datum partOptions;
    Datum newOptions;
    Datum replVal[Natts_pg_partition];
    bool replNull[Natts_pg_partition];
    bool replRepl[Natts_pg_partition];
    errno_t rc;
    bool isNull = false;

    partOptions = fastgetattr(partTuple, Anum_pg_partition_reloptions, RelationGetDescr(pgPartition), &isNull);
    /* If the caller use replacement to update reloptions, but the effect is the same as not set, just return */
    if (inplace && enable == PartitionInvisibleMetadataKeep(partOptions)) {
        return;
    }
    newOptions = SetWaitCleanGpiRelOptions(isNull ? (Datum)0 : partOptions, enable);

    rc = memset_s(replVal, sizeof(replVal), 0, sizeof(replVal));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replNull, sizeof(replNull), false, sizeof(replNull));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replRepl, sizeof(replRepl), false, sizeof(replRepl));
    securec_check(rc, "\0", "\0");

    if (PointerIsValid(newOptions)) {
        replVal[Anum_pg_partition_reloptions - 1] = newOptions;
        replNull[Anum_pg_partition_reloptions - 1] = false;
    } else {
        replNull[Anum_pg_partition_reloptions - 1] = true;
    }
    replRepl[Anum_pg_partition_reloptions - 1] = true;

    newTuple = heap_modify_tuple(partTuple, RelationGetDescr(pgPartition), replVal, replNull, replRepl);

    if (inplace) {
        heap_inplace_update(pgPartition, newTuple);
    } else {
        simple_heap_update(pgPartition, &newTuple->t_self, newTuple);
        CatalogUpdateIndexes(pgPartition, newTuple);
    }

    ereport(LOG, (errmsg("partition %u set reloptions wait_clean_gpi success", HeapTupleGetOid(partTuple))));
    heap_freetuple_ext(newTuple);
}

/* Set one partitioned relation's reloptions wait_clean_gpi */
void PartitionedSetWaitCleanGpi(const char* parentName, Oid parentPartOid, bool enable, bool inplace)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }

    HeapTuple partTuple;
    Relation pgPartition;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    partTuple = SearchSysCache3(PARTPARTOID,
        PointerGetDatum(parentName),
        CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE),
        ObjectIdGetDatum(parentPartOid));
    if (!HeapTupleIsValid(partTuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", parentPartOid)));
    }
    UpdateWaitCleanGpiRelOptions(pgPartition, partTuple, enable, inplace);
    ReleaseSysCache(partTuple);
    heap_close(pgPartition, NoLock);

    /* Make changes visible */
    CommandCounterIncrement();

    ereport(LOG, (errmsg("partition relation %s set reloptions wait_clean_gpi success", parentName)));
}

/* Set one partition's reloptions wait_clean_gpi */
void PartitionSetWaitCleanGpi(Oid partOid, bool enable, bool inplace)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }

    Relation pgPartition;
    HeapTuple partTuple;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(partTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", partOid)));
    }
    UpdateWaitCleanGpiRelOptions(pgPartition, partTuple, enable, inplace);
    ReleaseSysCache(partTuple);
    heap_close(pgPartition, NoLock);

    /* Make changes visible */
    CommandCounterIncrement();

    ereport(LOG, (errmsg("partition %u set reloptions wait_clean_gpi success", partOid)));
}

/*
 * check one partition's invisible metadata whether need to skip the check of gpi for global index
 * 
 * Notes: if datumPartType is 'x', that is local index of one partiiton, this case can skip the check
 * of wait_clean_gpi for partition's global index
 */
bool PartitionLocalIndexSkipping(Datum datumPartType)
{
    char parttype;
    
    if (!PointerIsValid(datumPartType)) {
        return false;
    }

    parttype = DatumGetChar(datumPartType);
    if (parttype == PART_OBJ_TYPE_INDEX_PARTITION) {
        return false; 
    } 

    return true;
}

/*
 * Check one partition's invisible metadata tuple whether still keep
 *
 * Notes: if wait_clean_gpi=y is contained in reloptions, determine to keep
 */
bool PartitionInvisibleMetadataKeep(Datum datumRelOptions)
{
    bool ret = false;
    bytea* options = NULL;
    char* waitCleanGpi;

    if (!PointerIsValid(datumRelOptions)) {
        return false;
    }

    options = heap_reloptions(RELKIND_RELATION, datumRelOptions, true);
    if (options != NULL) {
        waitCleanGpi = (char*)StdRdOptionsGetStringData(options, wait_clean_gpi, OptDisabledWaitCleanGpi);
        if (pg_strcasecmp(OptEnabledWaitCleanGpi, waitCleanGpi) == 0) {
            ret = true;
        }

        pfree_ext(options);
    }

    return ret;
}

/*
 * Check whether a partition is properly used.
 */
bool PartitionParentOidIsLive(Datum parentDatum)
{
    Oid parentid = InvalidOid;
    HeapTuple partTuple = NULL;

    if (!PointerIsValid(parentDatum)) {
        return false;
    }

    parentid = DatumGetObjectId(parentDatum);

    /* Get table information from syscache */
    partTuple = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(parentid), LOG);
    if (HeapTupleIsValid(partTuple)) {
        ReleaseSysCache(partTuple);
        return true;
    }

    return false;
}

/*
 * In pg_partition, search all tuples (visible and invisible) containing wait_clean_gpi=y
 * in reloptios of one partitioed relation and set wait_clean_gpi=n
 *
 * Notes: This function is called only when a partition table is lazy vacuumed,
 * and cannot be executed in parallel with PartitionSetWaitCleanGpi, Currently,
 * the AccessShareLock lock of ADD_PARTITION_ACTION is used to ensure that no concurrent
 * operations are performed.
 */
void PartitionedSetEnabledClean(Oid parentOid)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }
    Relation pgPartition = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[2];
    HeapTuple tuple = NULL;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    ScanKeyInit(
        &key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOid));

    scan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 2, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        UpdateWaitCleanGpiRelOptions(pgPartition, tuple, false, true);
    }
    systable_endscan(scan);
    heap_close(pgPartition, NoLock);

    ereport(LOG, (errmsg("partitioned %u set reloptions wait_clean_gpi=n success", parentOid)));
}

/*
 * In pg_partition, search all tuples (visible and invisible) containing wait_clean_gpi=y
 * in reloptios of one partition's all partitions and set wait_clean_gpi=n
 *
 * input cleanedParts means a collection of partoids that have been cleaned of all remaining invalid partitions
 * input invisibleParts means the collection of partoids for invalid partitions that have been deleted
 * input updatePartitioned means need check whether update partitioned's reloptions
 *
 * Notes: This function is called only when a partition table is lazy vacuumed,
 * and cannot be executed in parallel with PartitionSetWaitCleanGpi, if updatePartitioned
 */
void PartitionSetEnabledClean(
    Oid parentOid, OidRBTree* cleanedParts, OidRBTree* invisibleParts, bool updatePartitioned)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }
    Relation pgPartition = NULL;
    TupleDesc partTupdesc = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[2];
    HeapTuple tuple = NULL;
    Oid partOid;
    OidRBTree* liveParts = CreateOidRBTree();
    bool needSetOpts = false;
    bool needSetPartitioned = updatePartitioned;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    partTupdesc = RelationGetDescr(pgPartition);
    ScanKeyInit(&key[0],
        Anum_pg_partition_parttype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOid));
    scan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 2, key);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        needSetOpts = false;
        partOid = HeapTupleGetOid(tuple);
        if (OidRBTreeMemberOid(cleanedParts, partOid)) {
            needSetOpts = true;
        } else if (OidRBTreeMemberOid(invisibleParts, partOid)) {
            needSetOpts = true;
        } else if (PartitionStatusIsLive(partOid, &liveParts)) {
            needSetOpts = true;
        } else if (updatePartitioned && InvisblePartEnableClean(tuple, partTupdesc)) {
            continue;
        } else {
            needSetPartitioned = false;
        }

        if (needSetOpts) {
            UpdateWaitCleanGpiRelOptions(pgPartition, tuple, false, true);
        }
    }
    systable_endscan(scan);
    heap_close(pgPartition, NoLock);
    DestroyOidRBTree(&liveParts);

    if (needSetPartitioned) {
        PartitionedSetEnabledClean(parentOid);
    }
}

/*
 * In pg_partition, search all tuples containing wait_clean_gpi=y
 * in reloptios of one relation's all partitions (visible and invisible)
 * in a partition and set wait_clean_gpi=n
 *
 * Notes: This function is called only when a partitioned table is vacuum full,
 * and cannot be executed in parallel with PartitionSetWaitCleanGpi.
 */
void PartitionSetAllEnabledClean(Oid parentOid)
{
    Relation pgPartition = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[1];
    HeapTuple tuple = NULL;

    pgPartition = heap_open(PartitionRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOid));

    scan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 1, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_partition partitionForm = (Form_pg_partition)GETSTRUCT(tuple);
        if (partitionForm->relfilenode == InvalidOid) {
            SysScanDesc subscan = NULL;
            ScanKeyData subkey[1];
            HeapTuple subtuple = NULL;
            ScanKeyInit(&subkey[0], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(HeapTupleGetOid(tuple)));
            subscan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 1, subkey);
            while (HeapTupleIsValid(subtuple = systable_getnext(subscan))) {
                UpdateWaitCleanGpiRelOptions(pgPartition, subtuple, false, true);
            }
            systable_endscan(subscan);
        }
        UpdateWaitCleanGpiRelOptions(pgPartition, tuple, false, true);
    }
    systable_endscan(scan);
    heap_close(pgPartition, NoLock);

    ereport(LOG, (errmsg("relation %u set all partition's reloptions wait_clean_gpi=n success", parentOid)));
}

/*
 * Get all invisible partition from pg_partition
 *
 * Notes: Before calling the function, you must ensure that a lock with parentOid
 * is already held (to prevent parallelism with any ALTER table partition process)
 * and AccessShareLock for ADD_PARTITION_ACTION (to prevent parallelism with the
 * process of automatically creating partitions in any interval partition)
 */
void PartitionGetAllInvisibleParts(Oid parentOid, OidRBTree** invisibleParts)
{
    Relation pgPartition = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[2];
    HeapTuple tuple = NULL;
    OidRBTree* liveParts = CreateOidRBTree();
    Oid partOid;

    pgPartition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&key[0],
        Anum_pg_partition_parttype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOid));

    scan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 2, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        partOid = HeapTupleGetOid(tuple);
        if (OidRBTreeMemberOid(*invisibleParts, partOid)) {
            continue;
        } else if (PartitionStatusIsLive(partOid, &liveParts)) {
            continue;
        } else {
            (void)OidRBTreeInsertOid(*invisibleParts, partOid);
        }

        Form_pg_partition partitionForm = (Form_pg_partition)GETSTRUCT(tuple);
        if (partitionForm->relfilenode == InvalidOid) {
            SysScanDesc subscan = NULL;
            ScanKeyData subkey[2];
            HeapTuple subtuple = NULL;
            ScanKeyInit(&subkey[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
                CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));
            ScanKeyInit(&subkey[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(partOid));
            subscan = systable_beginscan(pgPartition, InvalidOid, false, SnapshotAny, 2, subkey);
            while (HeapTupleIsValid(subtuple = systable_getnext(subscan))) {
                Oid subpartOid = HeapTupleGetOid(subtuple);
                if (!OidRBTreeMemberOid(*invisibleParts, subpartOid) && PartitionStatusIsLive(subpartOid, &liveParts)) {
                    (void)OidRBTreeInsertOid(*invisibleParts, subpartOid);
                }
            }
            systable_endscan(subscan);
        }
    }
    systable_endscan(scan);
    heap_close(pgPartition, NoLock);
    DestroyOidRBTree(&liveParts);
}

/*
 * Check whether contain a tuple in pg_partition, which includes
 * wait_clean_gpi=y in the reloptions of the tuple
 *
 * Notes: this function is called only when vacuum full pg_partition
 */
bool PartitionMetadataDisabledClean(Relation pgPartition)
{
    bool result = false;
    TupleDesc partTupdesc = NULL;
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    ScanKeyData key[1];
    Form_pg_partition partform;
    char* relName = NULL;

    if (RelationGetRelid(pgPartition) != PartitionRelationId) {
        return result;
    }

    ScanKeyInit(
        &key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE));

    partTupdesc = RelationGetDescr(pgPartition);
    scan = systable_beginscan(pgPartition, PartitionParentOidIndexId, true, NULL, 1, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        bool isNull = false;
        Datum partOptions = fastgetattr(tuple, Anum_pg_partition_reloptions, partTupdesc, &isNull);
        if (isNull) {
            continue;
        }
        if (PartitionInvisibleMetadataKeep(partOptions)) {
            partform = (Form_pg_partition)GETSTRUCT(tuple);
            relName = (char*)palloc0(NAMEDATALEN);
            error_t rc = strncpy_s(relName, NAMEDATALEN, partform->relname.data, NAMEDATALEN - 1);
            securec_check_ss(rc, "\0", "\0");
            result = true;
            break;
        }
    }
    systable_endscan(scan);

    if (result) {
        ereport(WARNING,
            (errmsg("system table pg_partition contain relation %s have reloptions wait_clean_gpi=y,"
                    "must run the vacuum (full) %s first",
                relName,
                relName)));
    }
    return result;
}
