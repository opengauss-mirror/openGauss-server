/* -------------------------------------------------------------------------
 *
 * storage.cpp
 *	  code to create and destroy physical storage for relations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/storage.cpp
 *
 * NOTES
 *	  Some of this code used to be in storage/smgr/smgr.c, and the
 *	  function names still reflect that.
 *
 * -------------------------------------------------------------------------
 */

#include <unistd.h>
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_am.h"
#include "access/visibilitymap.h"
#include "access/multi_redo_api.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_hashbucket_fn.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "commands/verify.h"
#include "pgxc/pgxc.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "threadpool/threadpool.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "ddes/dms/ss_transaction.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/cache/part_cachemgr.h"
#include "tsdb/cache/partid_cachemgr.h"
#include "tsdb/storage/part.h"
#include "tsdb/utils/constant_def.h"
#include "tsdb/utils/ts_pg_cudesc.h"
#include "tsdb/utils/ts_relcache.h"
#endif   /* ENABLE_MULTIPLE_NODES */
/*
 * We keep a list of all relations (represented as RelFileNode values)
 * that have been created or deleted in the current transaction.  When
 * a relation is created, we create the physical file immediately, but
 * remember it so that we can delete the file again if the current
 * transaction is aborted.	Conversely, a deletion request is NOT
 * executed immediately, but is just entered in the list.  When and if
 * the transaction commits, we can delete the physical file.
 *
 * To handle subtransactions, every entry is marked with its transaction
 * nesting level.  At subtransaction commit, we reassign the subtransaction's
 * entries to the parent nesting level.  At subtransaction abort, we can
 * immediately execute the abort-time actions for all entries of the current
 * nesting level.
 *
 * NOTE: the list is kept in t_thrd.top_mem_cxt to be sure it won't disappear
 * unbetimes.  It'd probably be OK to keep it in u_sess->top_transaction_mem_cxt,
 * but I'm being paranoid.
 */

typedef struct PendingRelDelete {
    RelFileNode relnode;           /* relation that may need to be deleted */
    ForkNumber forknum;            /* MAIN_FORKNUM for row table; or valid column ForkNum */
    BackendId backend;             /* InvalidBackendId if not a temp rel */
    Oid relOid;                    /* InvalidOid if not a global temp rel */
    Oid ownerid;                   /* owner id for user space statistics */
    bool atCommit;                 /* T=delete at commit; F=delete at abort */
    int nestLevel;                 /* xact nesting level of request */
    bool tempTable;                /* whether a table is a temporary table */
    struct PendingRelDelete* next; /* linked-list link */
} PendingRelDelete;

#define ColMainFileNodesDefNum 16

THR_LOCAL StringInfo vf_store_root = NULL;

typedef struct MapperFileOptions {
    char filesystem[NAMEDATALEN]; /* like "hdfs" */
    char address[MAXPGPATH];      /* like "10.185.178.239:25000, ..." */
    char cfgpath[MAXPGPATH];      /* like "/opt/config" */
    char tblpath[MAXPGPATH];      /* like "/user/tbl_mppdb" */
} MapperFileOptions;

extern int64 calculate_relation_size(RelFileNode* rfn, BackendId backend, ForkNumber forknum);
extern bool find_tmptable_cache_key(Oid relNode);
extern void make_tmptable_cache_key(Oid relNode);

static inline bool cbi_is_dummy_rel(Relation rel)
{
    return (PointerIsValid(rel) && !OidIsValid(rel->rd_id) && IsCreatingCrossBucketIndex(rel));
}

/* before creating storage and inserting into the pending
 * delete list, we first set the right backend and check
 * to need wal logs.
 */
static void StorageSetBackendAndLogged(_in_ char relpersistence, _out_ BackendId* backend, _out_ bool* needs_wal)
{
    switch (relpersistence) {
        case RELPERSISTENCE_TEMP:
            *backend = InvalidBackendId;
            if (STMT_RETRY_ENABLED) {
                *needs_wal = true;
            } else {
                *needs_wal = false;
            }
            break;
        case RELPERSISTENCE_GLOBAL_TEMP:
            *backend = BackendIdForTempRelations;
            *needs_wal = false;
            break;
        case RELPERSISTENCE_UNLOGGED:
            *backend = InvalidBackendId;
            *needs_wal = false;
            break;
        case RELPERSISTENCE_PERMANENT:
            *backend = InvalidBackendId;
            *needs_wal = true;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relpersistence: %c", relpersistence)));
            return; /* placate compiler */
    }
}

/* Add the relation to the list of stuff to delete at abort.
 * if it's a row-storage table, *whichAttr* must is *AllTheAttrs*.
 * if it's a column-storage table, *whichAttr* >= *AllTheAttrs*.
 */
void InsertStorageIntoPendingList(_in_ const RelFileNode* rnode, _in_ AttrNumber attrnum, _in_ BackendId backend,
    _in_ Oid ownerid, _in_ bool atCommit, _in_ bool isDfsTruncate, Relation rel)
{
    PendingRelDelete* pending = (PendingRelDelete*)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(PendingRelDelete));
    pending->relnode = *rnode;

    if (AttrNumberIsForUserDefinedAttr(attrnum)) {
        pending->forknum = ColumnId2ColForkNum(attrnum);
    } else {
        pending->forknum = MAIN_FORKNUM;
    }
    pending->backend = backend;
    pending->relOid = InvalidOid;
    pending->ownerid = ownerid;
    pending->atCommit = atCommit; /* false: delete if abort; true: delete if commit */
    pending->nestLevel = GetCurrentTransactionNestLevel();
    pending->next = u_sess->catalog_cxt.pendingDeletes;
    u_sess->catalog_cxt.pendingDeletes = pending;

    if (rel && RELATION_IS_TEMP(rel)) {
        pending->tempTable = true;
    } else {
        pending->tempTable = false;
    }
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        pending->relOid = RelationGetRelid(rel);
    } else if (!u_sess->attr.attr_sql.enable_cluster_resize && enable_heap_bcm_data_replication()) {
        /* Lock RelFileNode to control concurrent with Catchup Thread */
        LockRelFileNode(*rnode, AccessExclusiveLock);
    }
}

void RelationCreateStorageInternal(RelFileNode rnode, char relpersistence, Oid ownerid, Relation rel = NULL)
{
    SMgrRelation srel;
    BackendId backend;
    bool needs_wal = false;
    bool found = false;
    HTAB *unlink_rel_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
   
    /*
     * Look up unlink_rel_hashtbl with target rnode, and if this key already exists in the hash table,
     * we must invalidate by oursevles
     */
    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    (void)hash_search(unlink_rel_hashtbl, (void *)&rnode, HASH_FIND, &found);
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
    
    if (found) {
        drop_rel_all_forks_buffers();
    }

    StorageSetBackendAndLogged(relpersistence, &backend, &needs_wal);

    srel = smgropen(rnode, backend);
    smgrcreate(srel, MAIN_FORKNUM, false);

    if (needs_wal) {
        log_smgrcreate(&srel->smgr_rnode.node, MAIN_FORKNUM);
    }

    /* Add the relation to the list of stuff to delete at abort */
    if (!IsBucketFileNode(rnode)) {
        InsertStorageIntoPendingList(&rnode, InvalidAttrNumber, backend, ownerid, false, false, rel);
    }

    /* remember global temp table storage info to localhash */
    if (rel && relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        remember_gtt_storage_info(rnode, rel);
    }
}

/*
 * RelationCreateStorage
 *      Create physical storage for a relation.
 *
 * Create the underlying disk file storage for the relation. This only
 * creates the main fork; additional forks are created lazily by the
 * modules that need them.
 *
 * This function is transactional. The creation is WAL-logged, and if the
 * transaction aborts later on, the storage will be destroyed.
 */
void RelationCreateStorage(RelFileNode rnode, char relpersistence, Oid ownerid, Oid bucketOid, Relation rel)
{
    if (IsSegmentFileNode(rnode)) {
        bool newcbi = (rel != NULL) && rel->newcbi;
        ereport(LOG, (errmodule(MOD_SEGMENT_PAGE), errmsg("Relation Create Storage, rnode %u/%u/%u/%u, "
                "bucketOid: %u, rel->newcbi: %d",
                rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, bucketOid, newcbi)));
    }
    if (BUCKET_OID_IS_VALID(bucketOid) && !(rel != NULL && IsCreatingCrossBucketIndex(rel))) {
        BucketCreateStorage(rnode, bucketOid, ownerid);
    } else {
        if (cbi_is_dummy_rel(rel)) {
            /* dummy relation */
            rel = NULL;
        }
        RelationCreateStorageInternal(rnode, relpersistence, ownerid, rel);
    }
}

/*
 * CStoreRelCreateStorage
 *    Create physical storage for a column of column-storage relation.
 */
void CStoreRelCreateStorage(RelFileNode* rnode, AttrNumber attrnum, char relpersistence, Oid ownerid)
{
    Assert(AttrNumberIsForUserDefinedAttr(attrnum));

    BackendId backend = InvalidBackendId;
    bool needs_wal = false;
    StorageSetBackendAndLogged(relpersistence, &backend, &needs_wal);

    if (needs_wal)
        log_smgrcreate(rnode, (ForkNumber)ColumnId2ColForkNum(attrnum));

    /* Add the relation to the list of stuff to delete at abort */
    InsertStorageIntoPendingList(rnode, attrnum, backend, ownerid, false);
}

void BucketCreateStorage(RelFileNode rnode, Oid bucketOid, Oid ownerid)
{
    RelFileNodeBackend newrnode;
    oidvector* bucketlist = searchHashBucketByOid(bucketOid);

    newrnode.node = rnode;
    newrnode.backend = InvalidBackendId;

    ereport(LOG, (errmodule(MOD_SEGMENT_PAGE), errmsg("Bucket Create Storage, bucket dim: %d", bucketlist->dim1)));
    /* create file storage for each bucket relation */
    for (int i = 0; i < bucketlist->dim1; i++) {
        newrnode.node.bucketNode = (int16)bucketlist->values[i];
        RelationCreateStorageInternal(newrnode.node, RELPERSISTENCE_PERMANENT, ownerid);
        smgrclosenode(newrnode);
    }

    /* Add the relation to the list of stuff to delete at abort */
    InsertStorageIntoPendingList(&rnode, InvalidAttrNumber, InvalidBackendId, ownerid, false);
}

/*
 * Perform XLogInsert of a XLOG_SMGR_CREATE record to WAL.
 */
void log_smgrcreate(RelFileNode* rnode, ForkNumber forkNum)
{
    if (IsSegmentFileNode(*rnode)) {
        return;
    }

    xl_smgr_create_compress xlrec;
    uint size;
    uint8 info = XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE;
    /*
    * compressOptions Copy
    */
    if (rnode->opt != 0) {
        xlrec.pageCompressOpts = rnode->opt;
        size = sizeof(xl_smgr_create_compress);
        info |= XLR_REL_COMPRESS;
    } else {
        size = sizeof(xl_smgr_create);
    }

    /*
     * Make an XLOG entry reporting the file creation.
     */
    xlrec.xlrec.forkNum = forkNum;
    RelFileNodeRelCopy(xlrec.xlrec.rnode, *rnode);

    XLogBeginInsert();
    XLogRegisterData((char*)&xlrec, (int)size);
    XLogInsert(RM_SMGR_ID, info, rnode->bucketNode);
}

static void CStoreRelDropStorage(Relation rel, RelFileNode* rnode, Oid ownerid)
{
    Assert((RelationIsColStore(rel)));

    TupleDesc desc = RelationGetDescr(rel);
    int nattrs = desc->natts;
    FormData_pg_attribute* attrs = desc->attrs;

    /* add all the cu files to the list of stuff to delete at commit */
    for (int i = 0; i < nattrs; ++i) {
        InsertStorageIntoPendingList(rnode, attrs[i].attnum, rel->rd_backend, ownerid, true);
    }
}

#ifdef ENABLE_MULTIPLE_NODES
namespace Tsdb {
/*
 * Insert part storage file (field/time data fiel) into pending list
 * The storage file is determined by partition_rnode + part_id
 */
void InsertPartStorageIntoPendingList(_in_ RelFileNode* partition_rnode, _in_ AttrNumber part_id,
    _in_ BackendId backend, _in_ Oid ownerid, _in_ bool atCommit)
{
    Assert(part_id >= 2000);
    PendingRelDelete* pending = (PendingRelDelete*)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(PendingRelDelete));
    pending->relnode = *partition_rnode;
    pending->forknum = ColumnId2ColForkNum(part_id);
    pending->backend = backend;
    pending->ownerid = ownerid;
    pending->atCommit = atCommit; /* false: delete if abort; true: delete if commit */
    pending->nestLevel = GetCurrentTransactionNestLevel();
    pending->next = u_sess->catalog_cxt.pendingDeletes;
    u_sess->catalog_cxt.pendingDeletes = pending;

    pending->tempTable = false;

    /* Lock RelFileNode to control concurrent with Catchup Thread */
    LockRelFileNode(*partition_rnode, RowExclusiveLock);
}

/*
 * Drop given parts(cudesc rel + field&time data file).
 * @param partition_id: the given parts should be in same partition
 * @param partition_rnode: partition's relfilenode
 * @param backend
 * @param ownerid
 * @param target_cudesc_relids: parts to drop
 */
void DropPartStorage(
    Oid partition_id, RelFileNode* partition_rnode, BackendId backend, Oid ownerid, List* target_cudesc_relids)
{
    if (list_length(target_cudesc_relids) == 0) {
        return;
    }
    List* cudesc_valid_oids = NIL;
    List* cudesc_valid_part_id = NIL;
    ListCell* cell = NULL;
    bool result = false;
    Tsdb::TsCUDesc tscudesc;
    foreach(cell, target_cudesc_relids) {
        Oid cudesc_oid = lfirst_oid(cell);
        Relation cudesc_rel = heap_open(cudesc_oid, AccessShareLock);
        TsCUDescUtils tscudesc_util(cudesc_rel);
        tscudesc.cudesc->Reset();
        result = tscudesc_util.get_part_desc(tscudesc);
        if (result) {
            cudesc_valid_oids = lappend_oid(cudesc_valid_oids, cudesc_oid);
            cudesc_valid_part_id = lappend_oid(cudesc_valid_part_id, tscudesc.cudesc->cu_id);
            ereport(DEBUG3, (errmsg("DropPartStorage, partition_oid=%u, cudesc oid=%d, part_id= %u", 
                partition_id, cudesc_oid, tscudesc.cudesc->cu_id)));
        } else {
            ereport(WARNING, (errmsg("DropPartStorage failed, cudesc oid=%u", cudesc_oid)));
        }
        heap_close(cudesc_rel, AccessShareLock);
    }
    if (list_length(cudesc_valid_oids) > 0) {
        performTsCudescDeletion(cudesc_valid_oids);
        foreach(cell, cudesc_valid_part_id) {
            uint32 part_id = lfirst_oid(cell);
            InsertPartStorageIntoPendingList(partition_rnode, part_id, backend, ownerid, true);
            InsertPartStorageIntoPendingList(
                partition_rnode, part_id + TsConf::TIME_FILE_OFFSET, backend, ownerid, true);
        }
    }
    list_free_ext(cudesc_valid_oids);
    list_free_ext(cudesc_valid_part_id);
}
}
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * - Brief: Drop column for a CStore table which includes
 *      1. Delete the column information from the cudesc
 *      2. Schedule unlinking of physical storage of the column at transaction commit.
 * - Parameter:
 *      @rel: target relation to drop column
 *      @attrnum: the column to drop
 *		@ownerid: owerid for the target table
 * - Return:
 *      no return value
 */
void CStoreRelDropColumn(Relation rel, AttrNumber attrnum, Oid ownerid)
{
    Assert(RelationIsCUFormat(rel));

    if (!RELATION_IS_PARTITIONED(rel)) {
        CStoreDropColumnInCuDesc(rel, attrnum);
        InsertStorageIntoPendingList(&rel->rd_node, attrnum, rel->rd_backend, ownerid, true);
    } else {
        List* partitions = NIL;
        ListCell* cell = NULL;
        Partition partition = NULL;
        Relation partRel = NULL;
        partitions = relationGetPartitionList(rel, AccessExclusiveLock);

        foreach (cell, partitions) {
            partition = (Partition)lfirst(cell);
            partRel = partitionGetRelation(rel, partition);

            CStoreDropColumnInCuDesc(partRel, attrnum);
            InsertStorageIntoPendingList(&partRel->rd_node, attrnum, partRel->rd_backend, ownerid, true);

            releaseDummyRelation(&partRel);
        }

        releasePartitionList(rel, &partitions, AccessExclusiveLock);
    }
}

/*
 * RelationDropStorage
 *		Schedule unlinking of physical storage at transaction commit.
 */
void RelationDropStorage(Relation rel, bool isDfsTruncate)
{
    // global temp table files may not exist
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        if (rel->rd_smgr == NULL) {
            /* Open it at the smgr level if not already done */
            RelationOpenSmgr(rel);
        }
        if (!smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
            return;
        }
    }

    /*
     * First we must push the column file, column bcm file to the pendingDeletes and
     * then push the logical table file to pendingDeletes.
     * When delete the file, smgrDoPendingDeletes will first delete the logical table file,
     * and it will call DropRelFileNodeAllBuffers to make all buffer invaild, include the
     * column bcm buffer, then we can drop the column file and column bcm file.
     * Examples: if a column table relfilenode is 16384, it will create 16384, 16384_C1.0,
     * 16384_C1_bcm... push into pendingDeletes
     * push:						pop:
     * 		16384(third)				16384(make all buffer invaild, unlink file)
     * 		16384_C1_bcm(second)		16384_C1_bcm(unlink file)
     * 		16384_C1.0(first)			16384_C1.0(unlink file)
     */
    if (RelationUsesSpaceType(rel->rd_rel->relpersistence) == SP_TEMP) {
        make_tmptable_cache_key(rel->rd_rel->relfilenode);
    }
    if (RelationIsCUFormat(rel)) {
        CStoreRelDropStorage(rel, &rel->rd_node, rel->rd_rel->relowner);
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (RelationIsTsStore(rel)) {
        /* rel is partition relation */
        List* cudesc_relid_list = search_all_cudesc(rel->rd_id, true);
        if (list_length(cudesc_relid_list) > 0) {
            Tsdb::DropPartStorage(rel->rd_id, &(rel->rd_node), rel->rd_backend,
                rel->rd_rel->relowner, cudesc_relid_list);
        }
        if (g_instance.attr.attr_common.enable_tsdb) {
            Tsdb::PartCacheMgr::GetInstance().clear_partition_cache(rel->rd_id);
        }
        list_free_ext(cudesc_relid_list);
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    /* Add the relation to the list of stuff to delete at commit */
    InsertStorageIntoPendingList(
        &rel->rd_node, InvalidAttrNumber, rel->rd_backend, rel->rd_rel->relowner, true, isDfsTruncate, rel);

    /*
     * NOTE: if the relation was created in this transaction, it will now be
     * present in the pending-delete list twice, once with atCommit true and
     * once with atCommit false.  Hence, it will be physically deleted at end
     * of xact in either case (and the other entry will be ignored by
     * smgrDoPendingDeletes, so no error will occur).  We could instead remove
     * the existing list entry and delete the physical file immediately, but
     * for now I'll keep the logic simple.
     */
    RelationCloseSmgr(rel);
}

/*
 * PartitionDropStorage
 *		Schedule unlinking of physical storage at transaction commit.
 */
void PartitionDropStorage(Relation rel, Partition part)
{
    /*
     * First we must push the column file, column bcm file to the pendingDeletes and
     * then push the logical table file to pendingDeletes.
     * When delete the file, smgrDoPendingDeletes will first delete the logical table file,
     * and it will call DropRelFileNodeAllBuffers to make all buffer invaild, include the
     * column bcm buffer, then we can drop the column file and column bcm file.
     * Examples: if a column table relfilenode is 16384, it will create 16384, 16384_C1.0,
     * 16384_C1_bcm... push into pendingDeletes
     * push:						pop:
     * 		16384(third)				16384(make all buffer invaild, unlink file)
     * 		16384_C1_bcm(second)		16384_C1_bcm(unlink file)
     * 		16384_C1.0(first)			16384_C1.0(unlink file)
     */
    if (RelationIsColStore(rel)) {
        CStoreRelDropStorage(rel, &part->pd_node, rel->rd_rel->relowner);
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (RelationIsTsStore(rel)) {
        List* cudesc_relid_list = search_all_cudesc(part->pd_id, true);
        if (list_length(cudesc_relid_list) > 0) {
            Tsdb::DropPartStorage(
                part->pd_id, &(part->pd_node), rel->rd_backend, rel->rd_rel->relowner, cudesc_relid_list);
        }
        if (g_instance.attr.attr_common.enable_tsdb) {
            Tsdb::PartCacheMgr::GetInstance().clear_partition_cache(part->pd_id);
        }
        list_free_ext(cudesc_relid_list);
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    /* Add the relation to the list of stuff to delete at commit */
    InsertStorageIntoPendingList(&part->pd_node, InvalidAttrNumber, rel->rd_backend, rel->rd_rel->relowner, true);

    /*
     * NOTE: if the relation was created in this transaction, it will now be
     * present in the pending-delete list twice, once with atCommit true and
     * once with atCommit false.  Hence, it will be physically deleted at end
     * of xact in either case (and the other entry will be ignored by
     * smgrDoPendingDeletes, so no error will occur).  We could instead remove
     * the existing list entry and delete the physical file immediately, but
     * for now I'll keep the logic simple.
     */
    PartitionCloseSmgr(part);
}

void RelationPreserveStorage(RelFileNode rnode, bool atCommit)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* prev = NULL;
    PendingRelDelete* next = NULL;

    prev = NULL;
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (RelFileNodeRelEquals(rnode, pending->relnode) && pending->atCommit == atCommit) {
            /* unlink and delete list entry */
            if (prev != NULL)
                prev->next = next;
            else
                u_sess->catalog_cxt.pendingDeletes = next;
            pfree(pending);
            /* prev does not change */
        } else {
            /* unrelated entry, don't touch it */
            prev = pending;
        }
    }
}

/*
 * RelationTruncate
 *		Physically truncate a relation to the specified number of blocks.
 *
 * This includes getting rid of any buffers for the blocks that are to be
 * dropped.
 */
void RelationTruncate(Relation rel, BlockNumber nblocks, TransactionId latest_removed_xid)
{
    /* Currently, segment-page tables should not be truncated */
    Assert(!RelationIsSegmentTable(rel));

    bool fsm = false;
    bool vm = false;
    bool bcm = false;

    /* decrease the permanent space on users' record */
    uint64 relSize = GetSMgrRelSize(&(rel->rd_node), rel->rd_backend, InvalidForkNumber);
    relSize -= nblocks * BLCKSZ; /* Ignore FSM VM BCM reserved space */
    perm_space_decrease(rel->rd_rel->relowner, relSize, RelationUsesSpaceType(rel->rd_rel->relpersistence));

    /* Open it at the smgr level if not already done */
    RelationOpenSmgr(rel);

    /*
     * Make sure smgr_targblock etc aren't pointing somewhere past new end
     */
    rel->rd_smgr->smgr_targblock = InvalidBlockNumber;
    rel->rd_smgr->smgr_prevtargblock = InvalidBlockNumber;
    rel->rd_smgr->smgr_fsm_nblocks = InvalidBlockNumber;
    rel->rd_smgr->smgr_vm_nblocks = InvalidBlockNumber;
    rel->rd_smgr->smgr_cached_nblocks = InvalidBlockNumber;

    for (int i = 0; i < rel->rd_smgr->smgr_bcmarry_size; i++)
        rel->rd_smgr->smgr_bcm_nblocks[i] = InvalidBlockNumber;

    /* Truncate the FSM first if it exists */
    fsm = smgrexists(rel->rd_smgr, FSM_FORKNUM);
    if (fsm)
        FreeSpaceMapTruncateRel(rel, nblocks);

    /* Truncate the visibility map too if it exists. */
    vm = smgrexists(rel->rd_smgr, VISIBILITYMAP_FORKNUM);
    if (vm)
        visibilitymap_truncate(rel, nblocks);

    /* Truncate the bcm too if it exists. */
    bcm = smgrexists(rel->rd_smgr, BCM_FORKNUM);
    if (bcm)
        BCM_truncate(rel);

    /* skip truncating if global temp table index does not exist */
    if (RELATION_IS_GLOBAL_TEMP(rel) && !smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
        return;
    }

    /*
     * We WAL-log the truncation before actually truncating, which means
     * trouble if the truncation fails. If we then crash, the WAL replay
     * likely isn't going to succeed in the truncation either, and cause a
     * PANIC. It's tempting to put a critical section here, but that cure
     * would be worse than the disease. It would turn a usually harmless
     * failure to truncate, that might spell trouble at WAL replay, into a
     * certain PANIC.
     */
    if (RelationNeedsWAL(rel)) {
        /*
         * Make an XLOG entry reporting the file truncation.
         */
        XLogRecPtr lsn;
        xl_smgr_truncate_compress xlrec;
        uint size;
        uint8 info = XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE;

        size = sizeof(xl_smgr_truncate_compress);
        xlrec.xlrec.blkno = nblocks;
        xlrec.pageCompressOpts = rel->rd_node.opt;
        xlrec.latest_removed_xid = latest_removed_xid;

        if (rel->rd_node.opt != 0) {
            info |= XLR_REL_COMPRESS;
        }

        RelFileNodeRelCopy(xlrec.xlrec.rnode, rel->rd_node);

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, (int)size);

        lsn = XLogInsert(RM_SMGR_ID, info, rel->rd_node.bucketNode);

        /*
         * Flush, because otherwise the truncation of the main relation might
         * hit the disk before the WAL record, and the truncation of the FSM
         * or visibility map. If we crashed during that window, we'd be left
         * with a truncated heap, but the FSM or visibility map would still
         * contain entries for the non-existent heap pages.
         */
        if (fsm || vm)
            XLogWaitFlush(lsn);
    }

    if (!RELATION_IS_GLOBAL_TEMP(rel)) {
        /* Lock RelFileNode to control concurrent with Catchup Thread */
        LockRelFileNode(rel->rd_node, AccessExclusiveLock);
    }

    /* Do the real work */
    smgrtruncate(rel->rd_smgr, MAIN_FORKNUM, nblocks);
    BatchClearBadBlock(rel->rd_node, MAIN_FORKNUM, nblocks);
}

void PartitionTruncate(Relation parent, Partition part, BlockNumber nblocks, TransactionId latest_removed_xid)
{
    /* Currently, segment-page tables should not be truncated */
    Assert(!RelationIsSegmentTable(parent));

    Relation rel = NULL;
    bool fsm = false;
    bool vm = false;
    bool bcm = false;

    /* Open it at the smgr level if not already done */
    PartitionOpenSmgr(part);
    /* transform partition to fake relation */
    rel = partitionGetRelation(parent, part);

    /* decrease the permanent space on users' record */
    uint64 size = GetSMgrRelSize(&(rel->rd_node), rel->rd_backend, InvalidForkNumber);
    size -= nblocks * BLCKSZ; /* Ignore FSM VM BCM reserved space */
    perm_space_decrease(rel->rd_rel->relowner, size, RelationUsesSpaceType(rel->rd_rel->relpersistence));

    /*
     * Make sure smgr_targblock etc aren't pointing somewhere past new end
     */
    rel->rd_smgr->smgr_targblock = InvalidBlockNumber;
    rel->rd_smgr->smgr_prevtargblock = InvalidBlockNumber;
    rel->rd_smgr->smgr_fsm_nblocks = InvalidBlockNumber;
    rel->rd_smgr->smgr_vm_nblocks = InvalidBlockNumber;

    for (int i = 0; i < rel->rd_smgr->smgr_bcmarry_size; i++)
        rel->rd_smgr->smgr_bcm_nblocks[i] = InvalidBlockNumber;

    /* Truncate the FSM first if it exists */
    fsm = smgrexists(rel->rd_smgr, FSM_FORKNUM);
    if (fsm)
        FreeSpaceMapTruncateRel(rel, nblocks);

    /* Truncate the visibility map too if it exists. */
    vm = smgrexists(rel->rd_smgr, VISIBILITYMAP_FORKNUM);
    if (vm)
        visibilitymap_truncate(rel, nblocks);

    bcm = smgrexists(rel->rd_smgr, BCM_FORKNUM);
    if (bcm)
        BCM_truncate(rel);

    if (RelationNeedsWAL(parent)) {
        XLogRecPtr lsn;
        xl_smgr_truncate_compress xlrec;
        uint8 info = XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE;
        int redoSize;

        redoSize = sizeof(xl_smgr_truncate_compress);

        xlrec.xlrec.blkno = nblocks;
        xlrec.pageCompressOpts = rel->rd_node.opt;
        xlrec.latest_removed_xid = latest_removed_xid;

        if (rel->rd_node.opt != 0) {
            info |= XLR_REL_COMPRESS;
        }

        RelFileNodeRelCopy(xlrec.xlrec.rnode, part->pd_node);

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, redoSize);

        lsn = XLogInsert(RM_SMGR_ID, info, part->pd_node.bucketNode);

        /*
         * Flush, because otherwise the truncation of the main relation might
         * hit the disk before the WAL record, and the truncation of the
         * visibility map. If we crashed during that window, we'd be left
         * with a truncated heap, but the visibility map would still
         * contain entries for the non-existent heap pages.
         */
        if (fsm || vm)
            XLogWaitFlush(lsn);
    }

    /* Lock RelFileNode to control concurrent with Catchup Thread */
    LockRelFileNode(rel->rd_node, AccessExclusiveLock);

    /* Do the real work */
    smgrtruncate(rel->rd_smgr, MAIN_FORKNUM, nblocks);
    BatchClearBadBlock(rel->rd_node, MAIN_FORKNUM, nblocks);

    /* release fake relation */
    releaseDummyRelation(&rel);
}

static inline bool smgrCheckPendingNumberOverHashThreshold(bool isCommit)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;
    int nestLevel = GetCurrentTransactionNestLevel();

    uint4 pending_cnt = 0;
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel >= nestLevel) {
            if (pending->atCommit == isCommit) {
                if (!IsValidColForkNum(pending->forknum) && IsSegmentFileNode(pending->relnode)) {
                    pending_cnt++;
                    if (pending_cnt > DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD) {
                        return true;
                    }
                }
            }
        }
    }
    
    return false;
}

void smgrDoDropBufferUsingScan(bool isCommit)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;
    int nestLevel = GetCurrentTransactionNestLevel();
    int rnode_len = 0;

    RelFileNode rnodes[DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD];
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel >= nestLevel) {
            if (pending->atCommit == isCommit) {
                if (!IsValidColForkNum(pending->forknum) && IsSegmentFileNode(pending->relnode)) {
                    rnodes[rnode_len++] = pending->relnode;
                }
            }
        }
    }
    DropRelFileNodeAllBuffersUsingScan(rnodes, rnode_len);

    if (ENABLE_DMS && SS_PRIMARY_MODE && rnode_len > 0) {
        SSBCastDropRelAllBuffer(rnodes, rnode_len);
    }
}

void smgrDoDropBufferUsingHashTbl(bool isCommit)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;

    int rnode_len = 0;
    RelFileNode rnodes[DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD];

    int nestLevel = GetCurrentTransactionNestLevel();
    HTAB* relfilenode_hashtbl = relfilenode_hashtbl_create();
    int enter_cnt = 0;
    bool found = false;
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel >= nestLevel) {
            /* do deletion if called for */
            if (pending->atCommit == isCommit) {
                if (!IsValidColForkNum(pending->forknum) && IsSegmentFileNode(pending->relnode)) {
                    (void)hash_search(relfilenode_hashtbl, &(pending->relnode), HASH_ENTER, &found);
                    if (!found) {
                        enter_cnt++;

                        if (ENABLE_DMS && SS_PRIMARY_MODE) {
                            if (rnode_len >= DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD) {
                                SSBCastDropRelAllBuffer(rnodes, rnode_len);
                                rnode_len = 0;
                            }
                            rnodes[rnode_len++] = pending->relnode;
                        }
                    }
                } 
            } 
        }
    }

    if (ENABLE_DMS && SS_PRIMARY_MODE && rnode_len > 0) {
        SSBCastDropRelAllBuffer(rnodes, rnode_len);
    }
    
    /* At least one relnode founded */
    if (enter_cnt > 0) {
        DropRelFileNodeAllBuffersUsingHash(relfilenode_hashtbl);
    }
    hash_destroy(relfilenode_hashtbl);
    relfilenode_hashtbl = NULL;
}

static inline void segmentSmgrDoDropBuffers(bool isCommit)
{
    bool over_hash_thresh = smgrCheckPendingNumberOverHashThreshold(isCommit);
    if (over_hash_thresh) {
        smgrDoDropBufferUsingHashTbl(isCommit);
    } else {
        smgrDoDropBufferUsingScan(isCommit);
    }
}

static bool ContainsSegmentTable(bool isCommit)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;

    int nestLevel = GetCurrentTransactionNestLevel();
    bool found = false;
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel >= nestLevel && pending->atCommit == isCommit) {
            if (!IsValidColForkNum(pending->forknum) && IsSegmentFileNode(pending->relnode)) {
                found = true;
                return found;
            } 
        }
    }
    return found;
}

void push_del_rel_to_hashtbl(bool isCommit)
{
    HTAB *relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    DelFileTag *entry = NULL;
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;
    uint del_rel_num = 0;
    int nestLevel = GetCurrentTransactionNestLevel();
    bool found = false;

    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel >= nestLevel) {
            /* do deletion if called for */
            if (pending->atCommit == isCommit) {
                if (!IsValidColForkNum(pending->forknum)) {
                    del_rel_num++;
                }
            }
        }
    }

    /* Only push */
    if (del_rel_num > 0) {
        LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_EXCLUSIVE);
        for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
            next = pending->next;
            if (pending->nestLevel >= nestLevel) {
                /* do deletion if called for */
                if (pending->atCommit == isCommit) {
                    if (!IsValidColForkNum(pending->forknum) && !IsSegmentFileNode(pending->relnode)) {
                        entry = (DelFileTag*)hash_search(relfilenode_hashtbl, &(pending->relnode), HASH_ENTER, &found);
                        if (!found) {
                            entry->rnode.spcNode = pending->relnode.spcNode;
                            entry->rnode.dbNode = pending->relnode.dbNode;
                            entry->rnode.relNode = pending->relnode.relNode;
                            entry->rnode.bucketNode = pending->relnode.bucketNode;
                            entry->rnode.opt = pending->relnode.opt;
                            entry->maxSegNo = -1;
                            entry->fileUnlink = false;
                        }
                        BatchClearBadBlock(pending->relnode, pending->forknum, 0);
                    }
                }
            }
        }
        LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
    }

    if (del_rel_num > 0 && g_instance.bgwriter_cxt.invalid_buf_proc_latch != NULL) {
        SetLatch(g_instance.bgwriter_cxt.invalid_buf_proc_latch);
    }
    return;
}


/*
 *	smgrDoPendingDeletes() -- Take care of relation deletes at end of xact.
 *
 * This also runs when aborting a subxact; we want to clean up a failed
 * subxact immediately.
 *
 * Note: It's possible that we're being asked to remove a relation that has
 * no physical storage in any fork. In particular, it's possible that we're
 * cleaning up an old temporary relation for which RemovePgTempFiles has
 * already recovered the physical storage.
 */
void smgrDoPendingDeletes(bool isCommit)
{
    int nestLevel = GetCurrentTransactionNestLevel();
    PendingRelDelete* pending = NULL;
    PendingRelDelete* prev = NULL;
    PendingRelDelete* next = NULL;
    bool needDelete = PointerIsValid(u_sess->catalog_cxt.pendingDeletes);
    ColMainFileNodesCreate();

    if (needDelete) {
        START_CRIT_SECTION();
        t_thrd.pgxact->delayChkpt = true;
    }

    if (ContainsSegmentTable(isCommit)) {
        segmentSmgrDoDropBuffers(isCommit);
    }
    push_del_rel_to_hashtbl(isCommit);

    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        if (pending->nestLevel < nestLevel) {
            /* outer-level entries should not be processed yet */
            prev = pending;
        } else {
            /* unlink list entry first, so we don't retry on failure */
            if (prev != NULL)
                prev->next = next;
            else
                u_sess->catalog_cxt.pendingDeletes = next;
            /* do deletion if called for */
            if (pending->atCommit == isCommit) {
                if (!IsValidColForkNum(pending->forknum)) {
                    RowRelationDoDeleteFiles(
                        pending->relnode, pending->backend, pending->ownerid, pending->relOid, isCommit);

                    /*
                     * "CREATE/DROP hdfs table" will use Two-Phrases Commit Transaction,
                     * in which FinishPreparedTransactionPhase2() just does what
                     * smgrDoPendingDeletes() will do, so it is not necessary to
                     * drop hdfs directory here. FinishPreparedTransactionPhase2()
                     * will do the job.
                     * see FinishPreparedTransactionPhase2() for more details.
                     */
                } else {
                    ColumnRelationDoDeleteFiles(
                        &pending->relnode, pending->forknum, pending->backend, pending->ownerid);
#ifdef ENABLE_MULTIPLE_NODES
                    uint16 partid = ColForkNum2ColumnId(pending->forknum);
                    if (g_instance.attr.attr_common.enable_tsdb && partid >= TsConf::FIRST_PARTID && partid % 2 == 0) {
                        PartIdMgr::GetInstance().free_part_id(&pending->relnode, partid);
                    }
#endif   /* ENABLE_MULTIPLE_NODES */
                }
            }

            /* must explicitly free the list entry */
            pfree(pending);
            /* prev does not change */
        }
    }
    ColMainFileNodesDestroy();
    if (needDelete) {
        t_thrd.pgxact->delayChkpt = false;
        END_CRIT_SECTION();
    }

}

/*
 * smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
 *
 * The return value is the number of relations scheduled for termination.
 * *ptr is set to point to a freshly-palloc'd array of RelFileNodes.
 * If there are no relations to be deleted, *ptr is set to NULL.
 *
 * Only non-temporary relations are included in the returned list.	This is OK
 * because the list is used only in contexts where temporary relations don't
 * matter: we're either writing to the two-phase state file (and transactions
 * that have touched temp tables can't be prepared) or we're writing to xlog
 * (and all temporary files will be zapped if we restart anyway, so no need
 * for redo to do it also).
 *
 * Note that the list does not include anything scheduled for termination
 * by upper-level transactions.
 */
int smgrGetPendingDeletes(bool forCommit, ColFileNode** ptr, bool skipTemp, int *numTempRel)
{
    int nestLevel = GetCurrentTransactionNestLevel();
    int nrels;
    ColFileNode* rptrRel = NULL;
    PendingRelDelete* pending = NULL;

    nrels = 0;
    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = pending->next) {
        if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit && pending->backend == InvalidBackendId
            && (!skipTemp || !pending->tempTable))
            nrels++;
    }
    if (nrels == 0) {
        *ptr = NULL;
        return 0;
    }

    rptrRel = (ColFileNode*)palloc(nrels * sizeof(ColFileNode));
    *ptr = rptrRel;

    /* Obtain the temporary table first */
    if (!skipTemp) {
        for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = pending->next) {
            if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit &&
                pending->backend == InvalidBackendId && pending->tempTable) {
                rptrRel->filenode.spcNode = pending->relnode.spcNode;
                rptrRel->filenode.dbNode = pending->relnode.dbNode;
                rptrRel->filenode.relNode = pending->relnode.relNode;
                rptrRel->filenode.opt = pending->relnode.opt;
                rptrRel->forknum = pending->forknum;
                rptrRel->ownerid = pending->ownerid;
                /* Add bucketid into forknum */
                forknum_add_bucketid(rptrRel->forknum, pending->relnode.bucketNode);
                rptrRel++;
                *numTempRel += 1;
            }
        }
    }

    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = pending->next) {
        if (pending->nestLevel >= nestLevel && pending->atCommit == forCommit && pending->backend == InvalidBackendId
            && !pending->tempTable) {
            rptrRel->filenode.spcNode = pending->relnode.spcNode;
            rptrRel->filenode.dbNode = pending->relnode.dbNode;
            rptrRel->filenode.relNode = pending->relnode.relNode;
            rptrRel->filenode.opt = pending->relnode.opt;
            rptrRel->forknum = pending->forknum;
            rptrRel->ownerid = pending->ownerid;
            /* Add bucketid into forknum */
            forknum_add_bucketid(rptrRel->forknum, pending->relnode.bucketNode);
            rptrRel++;
        }
    }
    return nrels;
}

ColFileNodeRel *ConvertToOldColFileNode(ColFileNode *colFileNodes, int size, bool freeNodes)
{
    ColFileNodeRel *colRels = (ColFileNodeRel *)palloc((uint32)size * sizeof(ColFileNodeRel));
    for (int i = 0; i < size; i++) {
        colRels[i].filenode.spcNode = colFileNodes[i].filenode.spcNode;
        colRels[i].filenode.dbNode = colFileNodes[i].filenode.dbNode;
        colRels[i].filenode.relNode = colFileNodes[i].filenode.relNode;
        colRels[i].forknum = colFileNodes[i].forknum;
        colRels[i].ownerid = colFileNodes[i].ownerid;
    }
    if (freeNodes) {
        pfree(colFileNodes);
    }
    return colRels;
}

/*
 *	PostPrepare_smgr -- Clean up after a successful PREPARE
 *
 * What we have to do here is throw away the in-memory state about pending
 * relation deletes.  It's all been recorded in the 2PC state file and
 * it's no longer smgr's job to worry about it.
 */
void PostPrepare_smgr(void)
{
    PendingRelDelete* pending = NULL;
    PendingRelDelete* next = NULL;

    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;
        u_sess->catalog_cxt.pendingDeletes = next;
        /* must explicitly free the list entry */
        pfree(pending);
    }
}

/*
 * AtSubCommit_smgr() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending-deletes list to the parent transaction.
 */
void AtSubCommit_smgr(void)
{
    int nestLevel = GetCurrentTransactionNestLevel();
    PendingRelDelete* pending = NULL;

    for (pending = u_sess->catalog_cxt.pendingDeletes; pending != NULL; pending = pending->next) {
        if (pending->nestLevel >= nestLevel)
            pending->nestLevel = nestLevel - 1;
    }
}

/*
 * AtSubAbort_smgr() --- Take care of subtransaction abort.
 *
 * Delete created relations and forget about deleted relations.
 * We can execute these operations immediately because we know this
 * subtransaction will not commit.
 */
void AtSubAbort_smgr()
{
    smgrDoPendingDeletes(false);
}

void smgr_redo_create(RelFileNode rnode, ForkNumber forkNum, char *data)
{
    ForkRelFileNode key;
    bool found = false;
    HTAB *unlink_rel_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    HTAB *unlink_rel_fork_hashtbl = g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl;

    /* Look up the unlink_rel_hashtbl and invalidate the target rnodes buffers if necessary */
    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    (void)hash_search(unlink_rel_hashtbl, (void *)&rnode, HASH_FIND, &found);
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
    
    if (found) {
        drop_rel_all_forks_buffers();
    }

    /* Look up the unlink_rel_fork_hashtbl and invalidate the target rnodes buffers if necessary */
    if (forkNum != MAIN_FORKNUM) {
        key.rnode = rnode;
        key.forkNum = forkNum;
        LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_SHARED);
        (void)hash_search(unlink_rel_fork_hashtbl, (void *)&key, HASH_FIND, &found);
        LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);

        if (found) {
            drop_rel_one_fork_buffers();
        }
    }

    if (!IsValidColForkNum(forkNum)) {
        SMgrRelation reln = smgropen(rnode, InvalidBackendId, 0);
        smgrcreate(reln, forkNum, true);
    } else {
        CFileNode cFileNode(rnode, ColForkNum2ColumnId(forkNum), MAIN_FORKNUM);
        CUStorage* cuStorage = New(CurrentMemoryContext) CUStorage(cFileNode);
        Assert(cuStorage);
        TablespaceCreateDbspace(rnode.spcNode, rnode.dbNode, true);
        cuStorage->CreateStorage(0, true);
        DELETE_EX(cuStorage);
    }
}

void smgr_redo_truncate_cancel_conflicting_proc(TransactionId latest_removed_xid, XLogRecPtr lsn)
{
    if (IS_EXRTO_READ) {
        const int max_check_times = 1000;
        int check_times = 0;
        bool conflict = true;
        bool reach_max_check_times = false;
        while (conflict && check_times < max_check_times) {
            RedoInterruptCallBack();
            check_times++;
            reach_max_check_times = (check_times == max_check_times);
            conflict = proc_array_cancel_conflicting_proc(latest_removed_xid, lsn, reach_max_check_times);
        }
    }
}

void xlog_block_smgr_redo_truncate(RelFileNode rnode, BlockNumber blkno, XLogRecPtr lsn,
    TransactionId latest_removed_xid)
{
    smgr_redo_truncate_cancel_conflicting_proc(latest_removed_xid, lsn);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    smgrcreate(reln, MAIN_FORKNUM, true);
    UpdateMinRecoveryPoint(lsn, false);
    LockRelFileNode(rnode, AccessExclusiveLock);
    (void)LWLockAcquire(RedoTruncateLock, LW_EXCLUSIVE);
    smgrtruncate(reln, MAIN_FORKNUM, blkno);
    LWLockRelease(RedoTruncateLock);
    XLogTruncateRelation(rnode, MAIN_FORKNUM, blkno);
    Relation rel = CreateFakeRelcacheEntry(rnode);
    if (smgrexists(reln, FSM_FORKNUM))
        FreeSpaceMapTruncateRel(rel, blkno);
    if (smgrexists(reln, VISIBILITYMAP_FORKNUM))
        visibilitymap_truncate(rel, blkno);
    FreeFakeRelcacheEntry(rel);
    UnlockRelFileNode(rnode, AccessExclusiveLock);
}

void smgr_redo(XLogReaderState* record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    TransactionId latest_removed_xid = InvalidTransactionId;
    /* Backup blocks are not used in smgr records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_SMGR_CREATE) {
        xl_smgr_create* xlrec = (xl_smgr_create*)XLogRecGetData(record);

        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, (int2)XLogRecGetBucketId(record));
        rnode.opt = compress ? ((xl_smgr_create_compress*)(void *)XLogRecGetData(record))->pageCompressOpts : 0;
        smgr_redo_create(rnode, xlrec->forkNum, (char *)xlrec);
        /* Redo column file, attid is hidden in forkNum */
    } else if (info == XLOG_SMGR_TRUNCATE) {
        xl_smgr_truncate* xlrec = (xl_smgr_truncate*)XLogRecGetData(record);
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, (int2)XLogRecGetBucketId(record));
        rnode.opt = compress ? ((xl_smgr_truncate_compress*)(void *)XLogRecGetData(record))->pageCompressOpts : 0;
        if (XLogRecGetDataLen(record) == TRUNCATE_CONTAIN_XID_SIZE) {
            latest_removed_xid = ((xl_smgr_truncate_compress*)(void *)XLogRecGetData(record))->latest_removed_xid;
        }
        /*
         * Forcibly create relation if it doesn't exist (which suggests that
         * it was dropped somewhere later in the WAL sequence).  As in
         * XLogReadBufferForRedo, we prefer to recreate the rel and replay the
         * log as best we can until the drop is seen.
         */

        /*
         * Before we perform the truncation, update minimum recovery point
         * to cover this WAL record. Once the relation is truncated, there's
         * no going back. The buffer manager enforces the WAL-first rule
         * for normal updates to relation files, so that the minimum recovery
         * point is always updated before the corresponding change in the
         * data file is flushed to disk. We have to do the same manually
         * here.
         *
         * Doing this before the truncation means that if the truncation fails
         * for some reason, you cannot start up the system even after restart,
         * until you fix the underlying situation so that the truncation will
         * succeed. Alternatively, we could update the minimum recovery point
         * after truncation, but that would leave a small window where the
         * WAL-first rule could be violated.
         */

        /* Also tell xlogutils.c about it */
        xlog_block_smgr_redo_truncate(rnode, xlrec->blkno, lsn, latest_removed_xid);
    } else
        ereport(PANIC, (errmsg("smgr_redo: unknown op code %u", info)));
}

void smgrApplyXLogTruncateRelation(XLogReaderState* record)
{
    xl_smgr_truncate* xlrec = (xl_smgr_truncate*)XLogRecGetData(record);

    RelFileNodeBackend rbnode;
    RelFileNodeCopy(rbnode.node, xlrec->rnode, (int2)XLogRecGetBucketId(record));
    rbnode.node.opt = GetTruncateXlogFileNodeOpt(record);
    rbnode.backend = InvalidBackendId;

    smgrclosenode(rbnode);

    XLogTruncateRelation(rbnode.node, MAIN_FORKNUM, xlrec->blkno);
}


/* create Column Heap Main file list */
void ColMainFileNodesCreate(void)
{
    if (u_sess->catalog_cxt.ColMainFileNodes == NULL) {
        u_sess->catalog_cxt.ColMainFileNodes =
            (RelFileNodeBackend*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                u_sess->catalog_cxt.ColMainFileNodesMaxNum * sizeof(RelFileNodeBackend));
    } else {
        /* Rollback maybe happens during rollback transaction.
         * So reset Column Heap Main file list.
         */
        u_sess->catalog_cxt.ColMainFileNodesCurNum = 0;
    }
}

/* destroy Column Heap Main file list */
void ColMainFileNodesDestroy(void)
{
    if (u_sess->catalog_cxt.ColMainFileNodes) {
        pfree_ext(u_sess->catalog_cxt.ColMainFileNodes);
        u_sess->catalog_cxt.ColMainFileNodesCurNum = 0;
        u_sess->catalog_cxt.ColMainFileNodesMaxNum = ColMainFileNodesDefNum;
    }
}

/* expand Column Heap Main relfilenode list */
static inline void ColMainFileNodesExpand(void)
{
    if (u_sess->catalog_cxt.ColMainFileNodesCurNum == u_sess->catalog_cxt.ColMainFileNodesMaxNum) {
        u_sess->catalog_cxt.ColMainFileNodesMaxNum *= 2;
        u_sess->catalog_cxt.ColMainFileNodes = (RelFileNodeBackend*)repalloc(u_sess->catalog_cxt.ColMainFileNodes,
            u_sess->catalog_cxt.ColMainFileNodesMaxNum * sizeof(RelFileNodeBackend));
    } else {
        Assert(u_sess->catalog_cxt.ColMainFileNodesCurNum < u_sess->catalog_cxt.ColMainFileNodesMaxNum);
    }
}

/* append one filenode to Column Heap Main file list */
void ColMainFileNodesAppend(RelFileNode* bcmFileNode, BackendId backend)
{
    ColMainFileNodesExpand();
    u_sess->catalog_cxt.ColMainFileNodes[u_sess->catalog_cxt.ColMainFileNodesCurNum].node = *bcmFileNode;
    u_sess->catalog_cxt.ColMainFileNodes[u_sess->catalog_cxt.ColMainFileNodesCurNum].backend = backend;
    ++u_sess->catalog_cxt.ColMainFileNodesCurNum;
}

/* search some one in Column Heap Main file list.
 *
 * If it's found, we will put it in the front of this list.
 * We think the hit file is the same to the next expected file.
 */
static inline bool ColMainFileNodesSearch(RelFileNodeBackend* bcmFileNode)
{
    RelFileNodeBackend* node = u_sess->catalog_cxt.ColMainFileNodes;
    int idx = 0;
    bool found = false;

    for (idx = 0; idx < u_sess->catalog_cxt.ColMainFileNodesCurNum; ++idx) {
        if (RelFileNodeEquals(bcmFileNode->node, node->node) && bcmFileNode->backend == node->backend) {
            found = true;
            break;
        }
        ++node;
    }

    if (found && idx != 0) {
        /* put the hit one in the front of file list */
        RelFileNodeBackend tmpnode = u_sess->catalog_cxt.ColMainFileNodes[0];
        u_sess->catalog_cxt.ColMainFileNodes[0] = u_sess->catalog_cxt.ColMainFileNodes[idx];
        u_sess->catalog_cxt.ColMainFileNodes[idx] = tmpnode;
    }
    return found;
}

/* Delete all the physical files for column relation. */
void ColumnRelationDoDeleteFiles(RelFileNode* rnode, ForkNumber forknum, BackendId backend, Oid ownerid)
{
    /* decrease the permanent space on users' record */
    uint64 size = GetSMgrRelSize(rnode, backend, forknum);
    perm_space_decrease(ownerid, size, find_tmptable_cache_key(rnode->relNode) ? SP_TEMP : SP_PERM);

    RelFileNodeBackend mainfile = {*rnode, backend};
    int whichColumn = ColForkNum2ColumnId(forknum);

    if (ColMainFileNodesSearch(&mainfile)) {
        /* BCM shared buffers have been invalided ahead within heap main relation.
         * So here we can delete CU files and their BCM files safely.
         */
        CStore::UnlinkColDataFile(*rnode, whichColumn, true);
    } else {
        /* Invalid BCM shared buffers and delete BCM files of this column */
        SMgrRelation srel = smgropen(mainfile.node, backend, whichColumn);
        smgrdounlinkfork(srel, forknum, false);
        smgrclose(srel);

        /* Then delete CU files of this column */
        CStore::UnlinkColDataFile(*rnode, whichColumn, false);

        ereport(DEBUG5,
            (errmsg("Delete Column files[+BCM]: %u/%u/%u column(%d)",
                rnode->spcNode,
                rnode->dbNode,
                rnode->relNode,
                whichColumn)));
    }
}

/* Delete all the physical files for row relation. */
void RowRelationDoDeleteFiles(RelFileNode rnode, BackendId backend, Oid ownerid, Oid relOid, bool isCommit)
{
    /* decrease the permanent space on users' record */
    uint64 size = GetSMgrRelSize(&rnode, backend, InvalidForkNumber);
    perm_space_decrease(ownerid, size, find_tmptable_cache_key(rnode.relNode) ? SP_TEMP : SP_PERM);

    SMgrRelation srel = smgropen(rnode, backend);

    /* Before unlinking files, invalid all the shared buffers first. */
    smgrdounlink(srel, false);
    smgrclose(srel);

    if (!IsSegmentFileNode(rnode)) {
        /* clean global temp table flags when transaction commit or rollback */
        if (SmgrIsTemp(srel) && relOid != InvalidOid && gtt_storage_attached(relOid)) {
            forget_gtt_storage_info(relOid, rnode, isCommit);
        }

        /*
         * After files are deleted, append this filenode into BCM file list,
         * so that we know all the BCM shared buffers of column relation has been
         * invalided.
         */
        ColMainFileNodesAppend(&rnode, backend);

        /* do nothing for row table. or invalid space cache for column table. */
        CStore::InvalidRelSpaceCache(&rnode);
    }
}

/*
 * @Description: get total files size for given relfilenode/backend /forknum
 * @IN relfilenode: relation file node
 * @IN backend: backend id
 * @IN forkNum: fork number
 * @Return: total files size
 */
uint64 GetSMgrRelSize(RelFileNode* relfilenode, BackendId backend, ForkNumber forkNum)
{
    Assert(relfilenode);

    uint64 size = 0;

    if (forkNum == InvalidForkNumber) {
        for (int fork = 0; fork <= MAX_FORKNUM; fork++) {
            size += calculate_relation_size(relfilenode, backend, fork);
            if (relfilenode->bucketNode == SegmentBktId) {
                break;
            }
        }
    } else {
        /* Column data 's BCM */
        size = calculate_relation_size(relfilenode, backend, forkNum);

        /* Column data */
        CFileNode tmpNode(*relfilenode, ColForkNum2ColumnId(forkNum), MAIN_FORKNUM);
        CUStorage custore(tmpNode);
        char pathname[MAXPGPATH] = {'\0'};
        unsigned int segcount = 0;

        for (segcount = 0;; segcount++) {
            struct stat fst;

            CHECK_FOR_INTERRUPTS();

            custore.GetFileName(pathname, MAXPGPATH, segcount);

            if (stat(pathname, &fst) < 0) {
                if (errno == ENOENT)
                    break;
                else if (errno == EIO)
                    ereport(ERROR,
                        (errcode_for_file_access(),
                            errmsg("could not stat file \"%s\": %m", pathname),
                            errhint("I/O Error,Please Check If Your DISK Is Broken")));
                else
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
            }
            size += fst.st_size;
        }
        custore.Destroy();
    }

    return size;
}

bool IsSmgrTruncate(const XLogReaderState* record)
{
    return ((XLogRecGetRmid(record) == RM_SMGR_ID && (XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_SMGR_TRUNCATE));
}

bool IsSmgrCreate(const XLogReaderState* record)
{
    return (XLogRecGetRmid(record) == RM_SMGR_ID && (XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_SMGR_CREATE);
}
