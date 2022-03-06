/* -------------------------------------------------------------------------
 *
 * genam.cpp
 *	  general index access method routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/index/genam.cpp
 *
 * NOTES
 *	  many of the old access method routines have been turned into
 *	  macros and moved to genam.h -cim 4/30/91
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "catalog/heap.h"

/* ----------------------------------------------------------------
 *		general access method routines
 *
 *		All indexed access methods use an identical scan structure.
 *		We don't know how the various AMs do locking, however, so we don't
 *		do anything about that here.
 *
 *		The intent is that an AM implementor will define a beginscan routine
 *		that calls RelationGetIndexScan, to fill in the scan, and then does
 *		whatever kind of locking he wants.
 *
 *		At the end of a scan, the AM's endscan routine undoes the locking,
 *		but does *not* call IndexScanEnd --- the higher-level index_endscan
 *		routine does that.	(We can't do it in the AM because index_endscan
 *		still needs to touch the IndexScanDesc after calling the AM.)
 *
 *		Because of this, the AM does not have a choice whether to call
 *		RelationGetIndexScan or not; its beginscan routine must return an
 *		object made by RelationGetIndexScan.  This is kinda ugly but not
 *		worth cleaning up now.
 * ----------------------------------------------------------------
 */
/* ----------------
 *	RelationGetIndexScan -- Create and fill an IndexScanDesc.
 *
 *		This routine creates an index scan structure and sets up initial
 *		contents for it.
 *
 *		Parameters:
 *				indexRelation -- index relation for scan.
 *				nkeys -- count of scan keys (index qual conditions).
 *				norderbys -- count of index order-by operators.
 *
 *		Returns:
 *				An initialized IndexScanDesc.
 * ----------------
 */
IndexScanDesc RelationGetIndexScan(Relation index_relation, int nkeys, int norderbys)
{
    IndexScanDesc scan;

    scan = (IndexScanDesc)palloc(SizeofIndexScanDescData + MaxHeapTupleSize);

    scan->heapRelation = NULL; /* may be set later */
    scan->xs_heapfetch = NULL;
    scan->indexRelation = index_relation;
    scan->xs_snapshot = SnapshotNow; /* may be set later */
    scan->numberOfKeys = nkeys;
    scan->numberOfOrderBys = norderbys;

    /* Initializes global partition index scan's information */
    scan->xs_want_ext_oid = RelationIsGlobalIndex(index_relation);
    if (scan->xs_want_ext_oid) {
        GPIScanInit(&scan->xs_gpi_scan);
    } else {
        scan->xs_gpi_scan = NULL;
    }

    /* Initializes crossbucket index scan's information */
    scan->xs_want_bucketid = RelationIsCrossBucketIndex(index_relation);
    if (scan->xs_want_bucketid) {
        cbi_scan_init(&scan->xs_cbi_scan);
    } else {
        scan->xs_cbi_scan = NULL;
    }

    /*
     * We allocate key workspace here, but it won't get filled until amrescan.
     */
    if (nkeys > 0)
        scan->keyData = (ScanKey)palloc(sizeof(ScanKeyData) * nkeys);
    else
        scan->keyData = NULL;
    if (norderbys > 0)
        scan->orderByData = (ScanKey)palloc(sizeof(ScanKeyData) * norderbys);
    else
        scan->orderByData = NULL;

    scan->xs_want_itup = false; /* may be set later */
    scan->xs_want_xid = false; /* may be set later */
    scan->xs_recheck_itup = false; /* may be set later */

    /*
     * During recovery we ignore killed tuples and don't bother to kill them
     * either. We do this because the xmin on the primary node could easily be
     * later than the xmin on the standby node, so that what the primary
     * thinks is killed is supposed to be visible on standby. So for correct
     * MVCC for queries during recovery we must ignore these hints and check
     * all tuples. Do *not* set ignore_killed_tuples to true when running in a
     * transaction that was started during recovery. xactStartedInRecovery
     * should not be altered by index AMs.
     */
    scan->kill_prior_tuple = false;
    scan->xactStartedInRecovery = TransactionStartedDuringRecovery();
    scan->ignore_killed_tuples = !scan->xactStartedInRecovery;

    scan->opaque = NULL;

    scan->xs_itup = NULL;
    scan->xs_itupdesc = NULL;

    scan->xs_ctup.tupTableType = HEAP_TUPLE;
    ItemPointerSetInvalid(&scan->xs_ctup.t_self);
    scan->xs_ctup.t_data = NULL;
    scan->xs_cbuf = InvalidBuffer;
    scan->xs_continue_hot = false;

    return scan;
}

/* ----------------
 *	IndexScanEnd -- End an index scan.
 *
 *		This routine just releases the storage acquired by
 *		RelationGetIndexScan().  Any AM-level resources are
 *		assumed to already have been released by the AM's
 *		endscan routine.
 *
 *	Returns:
 *		None.
 * ----------------
 */
void IndexScanEnd(IndexScanDesc scan)
{
    if (scan->keyData != NULL)
        pfree(scan->keyData);
    if (scan->orderByData != NULL)
        pfree(scan->orderByData);

    pfree(scan);
}

/*
 * BuildIndexValueDescription
 *
 * Construct a string describing the contents of an index entry, in the
 * form "(key_name, ...)=(key_value, ...)".  This is currently used
 * for building unique-constraint and exclusion-constraint error messages,
 * so only key columns of the index are checked and printed.
 *
 * Note that if the user does not have permissions to view all of the
 * columns involved then a NULL is returned.  Returning a partial key seems
 * unlikely to be useful and we have no way to know which of the columns the
 * user provided (unlike in ExecBuildSlotValueDescription).
 *
 * The passed-in values/nulls arrays are the "raw" input to the index AM,
 * e.g. results of FormIndexDatum --- this is not necessarily what is stored
 * in the index, but it's what the user perceives to be stored.
 */
char *BuildIndexValueDescription(Relation index_relation, Datum *values, const bool *isnull)
{
    StringInfoData buf;
    Form_pg_index idxrec;
    HeapTuple ht_idx;
    int indnkeyatts;
    int i;
    int keyno;
    Oid indexrelid;
    Oid indrelid;
    AclResult aclresult;

    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index_relation);
    /*
     * if this relation is a construct from a partition ,we
     * should use the parent Oid of Relation
     */
    indexrelid = RelationIsPartition(index_relation) ? index_relation->parentId : RelationGetRelid(index_relation);

    /*
     * Check permissions- if the user does not have access to view all of the
     * key columns then return NULL to avoid leaking data.
     *
     * First we need to check table-level SELECT access and then, if
     * there is no access there, check column-level permissions.
     */
    /*
     * Fetch the pg_index tuple by the Oid of the index
     */
    ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idx)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", indexrelid)));
    }
    idxrec = (Form_pg_index)GETSTRUCT(ht_idx);

    indrelid = idxrec->indrelid;
    Assert(indexrelid == idxrec->indexrelid);
    int tuplekeyatts = GetIndexKeyAttsByTuple(NULL, ht_idx);
    /* Table-level SELECT is enough, if the user has it */
    aclresult = pg_class_aclcheck(indrelid, GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK) {
        /*
         * No table-level access, so step through the columns in the
         * index and make sure the user has SELECT rights on all of them.
         */
        for (keyno = 0; keyno < tuplekeyatts; keyno++) {
            AttrNumber attnum = idxrec->indkey.values[keyno];
            aclresult = pg_attribute_aclcheck(indrelid, attnum, GetUserId(), ACL_SELECT);
            if (aclresult != ACLCHECK_OK) {
                /* No access, so clean up and return */
                ReleaseSysCache(ht_idx);
                return NULL;
            }
        }
    }
    ReleaseSysCache(ht_idx);

    initStringInfo(&buf);

    appendStringInfo(&buf, "(%s)=(", pg_get_indexdef_columns(indexrelid, true));

    for (i = 0; i < indnkeyatts; i++) {
        char* val = NULL;

        if (isnull[i]) {
            val = "null";
        } else {
            Oid foutoid;
            bool typisvarlena = false;

            /*
             * The provided data is not necessarily of the type stored in the
             * index; rather it is of the index opclass's input type. So look
             * at rd_opcintype not the index tupdesc.
             *
             * Note: this is a bit shaky for opclasses that have pseudotype
             * input types such as ANYARRAY or RECORD.	Currently, the
             * typoutput functions associated with the pseudotypes will work
             * okay, but we might have to try harder in future.
             */
            getTypeOutputInfo(index_relation->rd_opcintype[i], &foutoid, &typisvarlena);
            val = OidOutputFunctionCall(foutoid, values[i]);
        }

        if (i > 0) {
            appendStringInfoString(&buf, ", ");
        }
        appendStringInfoString(&buf, val);
    }

    appendStringInfoChar(&buf, ')');

    return buf.data;
}

/* ----------------------------------------------------------------
 *		heap-or-index-scan access to system catalogs
 *
 *		These functions support system catalog accesses that normally use
 *		an index but need to be capable of being switched to heap scans
 *		if the system indexes are unavailable.
 *
 *		The specified scan keys must be compatible with the named index.
 *		Generally this means that they must constrain either all columns
 *		of the index, or the first K columns of an N-column index.
 *
 *		These routines could work with non-system tables, actually,
 *		but they're only useful when there is a known index to use with
 *		the given scan keys; so in practice they're only good for
 *		predetermined types of scans of system catalogs.
 * ----------------------------------------------------------------
 */
/*
 * systable_beginscan --- set up for heap-or-index scan
 *
 *	rel: catalog to scan, already opened and suitably locked
 *	indexId: OID of index to conditionally use
 *	indexOK: if false, forces a heap scan (see notes below)
 *	snapshot: time qual to use (usually should be SnapshotNow)
 *	nkeys, key: scan keys
 *
 * The attribute numbers in the scan key should be set for the heap case.
 * If we choose to index, we reset them to 1..n to reference the index
 * columns.  Note this means there must be one scankey qualification per
 * index column!  This is checked by the Asserts in the normal, index-using
 * case, but won't be checked if the heapscan path is taken.
 *
 * The routine checks the normal cases for whether an indexscan is safe,
 * but caller can make additional checks and pass indexOK=false if needed.
 * In standard case indexOK can simply be constant TRUE.
 */
SysScanDesc systable_beginscan(Relation heap_relation, Oid index_id, bool index_ok, Snapshot snapshot, int nkeys,
                               ScanKey key)
{
    SysScanDesc sysscan;
    Relation irel;
    int2 bucketid = InvalidBktId;


    if (index_ok && !u_sess->attr.attr_common.IgnoreSystemIndexes && !ReindexIsProcessingIndex(index_id)) {
        /*
         * we may use systable index scan to search chunk_id for a
         * buckted toast table, so open the index bucket first.
         */
        if (RelationIsBucket(heap_relation)) {
            Assert(IsToastNamespace(RelationGetNamespace(heap_relation)));
            bucketid = heap_relation->rd_node.bucketNode;
        }
        irel = index_open(index_id, AccessShareLock, bucketid);
    } else
        irel = NULL;

    if (snapshot == NULL) {
        snapshot = GetCatalogSnapshot();
    }

    sysscan = (SysScanDesc)palloc(sizeof(SysScanDescData));

    sysscan->heap_rel = heap_relation;
    sysscan->irel = irel;

    if (irel) {
        int i;

        /* Change attribute numbers to be index column numbers. */
        for (i = 0; i < nkeys; i++) {
            int j;

            for (j = 0; j < IndexRelationGetNumberOfAttributes(irel); j++) {
                if (key[i].sk_attno == irel->rd_index->indkey.values[j]) {
                    key[i].sk_attno = j + 1;
                    break;
                }
            }
            if (j == IndexRelationGetNumberOfAttributes(irel))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("column is not in index")));
        }

        sysscan->iscan = index_beginscan(heap_relation, irel, snapshot, nkeys, 0);
        index_rescan(sysscan->iscan, key, nkeys, NULL, 0);
        sysscan->scan = NULL;
    } else {
        /*
         * We disallow synchronized scans when forced to use a heapscan on a
         * catalog.  In most cases the desired rows are near the front, so
         * that the unpredictable start point of a syncscan is a serious
         * disadvantage; and there are no compensating advantages, because
         * it's unlikely that such scans will occur in parallel.
         */
        sysscan->scan = (HeapScanDesc)heap_beginscan_strat(heap_relation, snapshot, nkeys, key, true, false);
        sysscan->iscan = NULL;
    }

    return sysscan;
}

/*
 * systable_getnext --- get next tuple in a heap-or-index scan
 *
 * Returns NULL if no more tuples available.
 *
 * Note that returned tuple is a reference to data in a disk buffer;
 * it must not be modified, and should be presumed inaccessible after
 * next getnext() or endscan() call.
 */
HeapTuple systable_getnext(SysScanDesc sysscan)
{
    HeapTuple htup;

    if (sysscan->irel) {
        htup = (HeapTuple)index_getnext(sysscan->iscan, ForwardScanDirection);
        /*
         * We currently don't need to support lossy index operators for any
         * system catalog scan.  It could be done here, using the scan keys to
         * drive the operator calls, if we arranged to save the heap attnums
         * during systable_beginscan(); this is practical because we still
         * wouldn't need to support indexes on expressions.
         */
        if (htup && sysscan->iscan->xs_recheck)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("system catalog scans with lossy index conditions are not implemented")));
    } else
        htup = heap_getnext((TableScanDesc) (sysscan->scan), ForwardScanDirection);

    return htup;
}

/*
 * systable_recheck_tuple --- recheck visibility of most-recently-fetched tuple
 *
 * This is useful to test whether an object was deleted while we waited to
 * acquire lock on it.
 *
 * Note: we don't actually *need* the tuple to be passed in, but it's a
 * good crosscheck that the caller is interested in the right tuple.
 */
bool systable_recheck_tuple(SysScanDesc sysscan, HeapTuple tup)
{
    bool result = false;

    if (sysscan->irel) {
        IndexScanDesc scan = sysscan->iscan;

        Assert(tup == &scan->xs_ctup);
        Assert(BufferIsValid(scan->xs_cbuf));
        /* must hold a buffer lock to call HeapTupleSatisfiesVisibility */
        LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
        result = HeapTupleSatisfiesVisibility(tup, scan->xs_snapshot, scan->xs_cbuf);
        LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
    } else {
        HeapScanDesc scan = sysscan->scan;

        Assert(tup == &scan->rs_ctup);
        Assert(BufferIsValid(scan->rs_base.rs_cbuf));
        /* must hold a buffer lock to call HeapTupleSatisfiesVisibility */
        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);
        result = HeapTupleSatisfiesVisibility(tup, scan->rs_base.rs_snapshot, scan->rs_base.rs_cbuf);
        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
    }
    return result;
}

/*
 * systable_endscan --- close scan, release resources
 *
 * Note that it's still up to the caller to close the heap relation.
 */
void systable_endscan(SysScanDesc sysscan)
{
    if (sysscan->irel) {
        index_endscan(sysscan->iscan);
        index_close(sysscan->irel, AccessShareLock);
    } else
        heap_endscan((TableScanDesc)(sysscan->scan));

    pfree(sysscan);
}

/*
 * systable_beginscan_ordered --- set up for ordered catalog scan
 *
 * These routines have essentially the same API as systable_beginscan etc,
 * except that they guarantee to return multiple matching tuples in
 * index order.  Also, for largely historical reasons, the index to use
 * is opened and locked by the caller, not here.
 *
 * Currently we do not support non-index-based scans here.	(In principle
 * we could do a heapscan and sort, but the uses are in places that
 * probably don't need to still work with corrupted catalog indexes.)
 * For the moment, therefore, these functions are merely the thinnest of
 * wrappers around index_beginscan/index_getnext.  The main reason for their
 * existence is to centralize possible future support of lossy operators
 * in catalog scans.
 */
SysScanDesc systable_beginscan_ordered(Relation heap_relation, Relation index_relation, Snapshot snapshot, int nkeys,
                                       ScanKey key)
{
    SysScanDesc sysscan;
    int i;

    /* REINDEX can probably be a hard error here ... */
    if (ReindexIsProcessingIndex(RelationGetRelid(index_relation)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("cannot do ordered scan on index \"%s\", because it is being reindexed",
                               RelationGetRelationName(index_relation))));
    /* ... but we only throw a warning about violating IgnoreSystemIndexes */
    if (u_sess->attr.attr_common.IgnoreSystemIndexes) {
        elog(WARNING, "using index \"%s\" despite IgnoreSystemIndexes", RelationGetRelationName(index_relation));
    }

    sysscan = (SysScanDesc)palloc(sizeof(SysScanDescData));

    sysscan->heap_rel = heap_relation;
    sysscan->irel = index_relation;

    /* Change attribute numbers to be index column numbers. */
    for (i = 0; i < nkeys; i++) {
        int j;

        for (j = 0; j < IndexRelationGetNumberOfAttributes(index_relation); j++) {
            if (key[i].sk_attno == index_relation->rd_index->indkey.values[j]) {
                key[i].sk_attno = j + 1;
                break;
            }
        }
        if (j == IndexRelationGetNumberOfAttributes(index_relation))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("column is not in index")));
    }

    sysscan->iscan = (IndexScanDesc)index_beginscan(heap_relation, index_relation, snapshot, nkeys, 0);
    index_rescan(sysscan->iscan, key, nkeys, NULL, 0);
    sysscan->scan = NULL;

    return sysscan;
}

/*
 * systable_getnext_ordered --- get next tuple in an ordered catalog scan
 */
HeapTuple systable_getnext_ordered(SysScanDesc sysscan, ScanDirection direction)
{
    HeapTuple htup;

    Assert(sysscan->irel);
    htup = (HeapTuple)index_getnext(sysscan->iscan, direction);
    /* See notes in systable_getnext */
    if (htup && sysscan->iscan->xs_recheck)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("system catalog scans with lossy index conditions are not implemented")));

    return htup;
}

/*
 * systable_endscan_ordered --- close scan, release resources
 */
void systable_endscan_ordered(SysScanDesc sysscan)
{
    Assert(sysscan->irel);
    index_endscan(sysscan->iscan);
    pfree(sysscan);
}

/* Database Security: Support password complexity
 * Brief			: Get the records from systable according to the reverse order of the index
 * Description		:
 * Notes			:
 */
HeapTuple systable_getnext_back(SysScanDesc sysscan)
{
    HeapTuple htup;

    if (sysscan->irel)
        htup = (HeapTuple)index_getnext(sysscan->iscan, BackwardScanDirection);
    else
        htup = heap_getnext((TableScanDesc) (sysscan->scan), BackwardScanDirection);

    return htup;
}

#define InitFakeRelTableCommon(ctl, cxt)                                    \
    do {                                                                    \
        errno_t errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));      \
        securec_check_c(errorno, "\0", "\0");                               \
        ctl.keysize = sizeof(PartRelIdCacheKey);                            \
        ctl.entrysize = sizeof(PartRelIdCacheEnt);                          \
        ctl.hash = tag_hash;                                                \
        ctl.hcxt = cxt;                                                     \
    } while (0)

/* Use global-partition-index-scan access to partition tables */
/* Create hash table for global partition index scan */
static void GPIInitFakeRelTable(GPIScanDesc gpiScan, MemoryContext cxt)
{
    HASHCTL ctl;
    InitFakeRelTableCommon(ctl, cxt);
    gpiScan->fakeRelationTable = hash_create(
        "GPI fakeRelationCache by OID", FAKERELATIONCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/* Lookup partition information from hash table use key for global partition index scan */
static void GPILookupFakeRelCache(GPIScanDesc gpiScan, PartRelIdCacheKey fakeRelKey)
{
    HTAB* fakeRels = gpiScan->fakeRelationTable;
    FakeRelationIdCacheLookup(fakeRels, fakeRelKey, gpiScan->fakePartRelation, gpiScan->partition);
}

static void GPIInsertFakeParentRelCacheForSubpartition(GPIScanDesc gpiScan, MemoryContext cxt, LOCKMODE lmode)
{
    HTAB* fakeRels = gpiScan->fakeRelationTable;
    Relation parentRel = gpiScan->parentRelation;
    Oid parentPartOid = partid_get_parentid(gpiScan->currPartOid);
    if (parentPartOid != parentRel->rd_id) {
        PartRelIdCacheKey fakeRelKey = {parentPartOid, InvalidBktId};
        Partition parentPartition = NULL;
        FakeRelationIdCacheLookup(fakeRels, fakeRelKey, parentRel, parentPartition);
        if (!RelationIsValid(parentRel)) {
            /* add current parentRel into fakeRelationTable */
            Oid baseRelOid = partid_get_parentid(parentPartOid);
            Relation baseRel = relation_open(baseRelOid, lmode);
            searchFakeReationForPartitionOid(fakeRels, cxt, baseRel, parentPartOid, parentRel, parentPartition, lmode);
            relation_close(baseRel, NoLock);
        }
        gpiScan->parentRelation = parentRel;
    }
}

/* Lookup partition information from hash table use key for global partition index scan */
static void GPIInsertFakeRelCache(GPIScanDesc gpiScan, MemoryContext cxt, LOCKMODE lmode)
{
    Oid currPartOid = gpiScan->currPartOid;
    HTAB* fakeRels = gpiScan->fakeRelationTable;
    Partition partition = NULL;

    GPIInsertFakeParentRelCacheForSubpartition(gpiScan, cxt, lmode);

    Relation parentRel = gpiScan->parentRelation;
    /* Save search fake relation in gpiScan->fakeRelation */
    searchFakeReationForPartitionOid(
        fakeRels, cxt, parentRel, currPartOid, gpiScan->fakePartRelation, partition, lmode);

    /* save partition */
    gpiScan->partition = partition;
}

/* destroy partition information from hash table */
static void GPIDestroyFakeRelCache(GPIScanDesc gpiScan)
{
    FakeRelationCacheDestroy(gpiScan->fakeRelationTable);
    gpiScan->fakeRelationTable = NULL;
}

/* Create and fill an GPIScanDesc */
void GPIScanInit(GPIScanDesc* gpiScan)
{
    GPIScanDesc gpiInfo = (GPIScanDesc)palloc(sizeof(GPIScanDescData));

    gpiInfo->currPartOid = InvalidOid;
    gpiInfo->fakePartRelation = NULL;
    gpiInfo->invisiblePartTree = NULL;
    gpiInfo->invisiblePartTreeForVacuum = CreateOidRBTree();
    gpiInfo->parentRelation = NULL;
    gpiInfo->fakeRelationTable = NULL;
    gpiInfo->partition = NULL;

    *gpiScan = gpiInfo;
}

/* Release fake-relation's hash table and GPIScanDesc */
void GPIScanEnd(GPIScanDesc gpiScan)
{
    if (gpiScan == NULL) {
        return;
    }

    if (gpiScan->fakeRelationTable != NULL) {
        GPIDestroyFakeRelCache(gpiScan);
    }

    DestroyOidRBTree(&gpiScan->invisiblePartTree);
    DestroyOidRBTree(&gpiScan->invisiblePartTreeForVacuum);
    pfree_ext(gpiScan);
}

/* Set global partition index work partition oid */
void GPISetCurrPartOid(GPIScanDesc gpiScan, Oid partOid)
{
    if (gpiScan == NULL) {
        ereport(ERROR, (errmsg("gpiScan is null, when set partition oid")));
    }
    gpiScan->currPartOid = partOid;
}

/* Get global partition index work partition oid */
Oid GPIGetCurrPartOid(const GPIScanDesc gpiScan)
{
    if (gpiScan == NULL) {
        ereport(ERROR, (errmsg("gpiScan is null, when get partition oid")));
    }
    return gpiScan->currPartOid;
}

/*
 * This gpiScan is used to switch the fake-relation of a partition
 * based on the partoid in the GPI when the GPI is used to scan data.
 *
 * Notes: return true means partition's fake-relation can use gpiScan->fakeRelationTable switch,
 *        return false means current parition is invisible, shoud not switch.
 */
bool GPIGetNextPartRelation(GPIScanDesc gpiScan, MemoryContext cxt, LOCKMODE lmode)
{
    bool result = true;
    PartStatus currStatus;
    PartRelIdCacheKey fakeRelKey = {gpiScan->currPartOid, InvalidBktId};

    Assert(OidIsValid(gpiScan->currPartOid));

    if (!PointerIsValid(gpiScan->fakeRelationTable)) {
        GPIInitFakeRelTable(gpiScan, cxt);
    }

    if (!PointerIsValid(gpiScan->invisiblePartTree)) {
        gpiScan->invisiblePartTree = CreateOidRBTree();
    }

    /* First check invisible partition oid's bitmapset */
    if (OidRBTreeMemberOid(gpiScan->invisiblePartTree, gpiScan->currPartOid)) {
        gpiScan->fakePartRelation = NULL;
        gpiScan->partition = NULL;
        gpiScan->currPartOid = InvalidOid;
        return false;
    }

    /* Obtains information about the current partition from the hash table */
    GPILookupFakeRelCache(gpiScan, fakeRelKey);

    /* If the fakePartRelation field is empty, need get partition information from pg_partition */
    if (!RelationIsValid(gpiScan->fakePartRelation)) {
        Assert(gpiScan->partition == NULL);
        /* Get current partition status in GPI */
        currStatus = PartitionGetMetadataStatus(gpiScan->currPartOid, false);
        /* Just save partition status if current partition metadata is invisible */
        if (currStatus == PART_METADATA_INVISIBLE) {
            /* If current partition metadata is invisible, add current partition oid into invisiblePartMap */
            (void)OidRBTreeInsertOid(gpiScan->invisiblePartTree, gpiScan->currPartOid);
            gpiScan->currPartOid = InvalidOid;
            result = false;
        } else {
            /* If current partition metadata is invisible, add current partition oid into fakeRelationTable */
            GPIInsertFakeRelCache(gpiScan, cxt, lmode);
            result = true;
        }
    }

    return result;
}

/* Create and fill an CBIScanDesc */
void cbi_scan_init(CBIScanDesc* cbiScan)
{
    CBIScanDesc cbiInfo = (CBIScanDesc)palloc(sizeof(CBIScanDescData));
    cbiInfo->bucketid = InvalidBktId;
    cbiInfo->fakeBucketRelation = NULL;
    cbiInfo->parentRelation = NULL;
    cbiInfo->fakeRelationTable = NULL;
    cbiInfo->partition = NULL;
    cbiInfo->mergingBktId = InvalidBktId;
    *cbiScan = cbiInfo;
}

/* Release fake-relation's hash table and GPIScanDesc */
void cbi_scan_end(CBIScanDesc cbiScan)
{
    if (cbiScan == NULL) {
        return;
    }
    if (cbiScan->fakeRelationTable != NULL) {
        FakeRelationCacheDestroy(cbiScan->fakeRelationTable);
        cbiScan->fakeRelationTable = NULL;
    }

    pfree_ext(cbiScan);
}

/* Use global-partition-index-scan access to partition tables */
/* Create hash table for global partition index scan */
static inline void cbi_init_fake_reltable(CBIScanDesc cbiScan, MemoryContext cxt)
{
    HASHCTL ctl;
    InitFakeRelTableCommon(ctl, cxt);
    cbiScan->fakeRelationTable = hash_create(
        "CBI fakeRelationCache by OID", FAKERELATIONCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/* Lookup partition information from hash table use key for global partition index scan */
static inline void cbi_lookup_fake_relcache(CBIScanDesc cbiScan, PartRelIdCacheKey fakeRelKey)
{
    HTAB* fakeRels = cbiScan->fakeRelationTable;
    FakeRelationIdCacheLookup(fakeRels, fakeRelKey, cbiScan->fakeBucketRelation, cbiScan->partition);
}

/* Set global bucket index bucket id. */
void cbi_set_bucketid(CBIScanDesc cbiScan, int2 butcketid)
{
    if (cbiScan == NULL) {
        ereport(ERROR, (errmsg("cbiScan is null, when set bucket id")));
    }
    cbiScan->bucketid = butcketid;
}

/* Get cross-bucket index working bucketid */
int2 cbi_get_current_bucketid(const CBIScanDesc cbiScan)
{
    if (cbiScan == NULL) {
        ereport(ERROR, (errmsg("cbiScan is null, when get bucketid")));
    }
    return cbiScan->bucketid;
}

bool cbi_get_bucket_relation(CBIScanDesc cbiScan, MemoryContext cxt)
{
    PartRelIdCacheKey fakeRelKey = {cbiScan->parentRelation->rd_id, cbiScan->bucketid};
    Assert(cbiScan->bucketid != InvalidBktId);
    if (!PointerIsValid(cbiScan->fakeRelationTable)) {
        cbi_init_fake_reltable(cbiScan, cxt);
    }

    /* Obtains information about the current partition from the hash table */
    cbi_lookup_fake_relcache(cbiScan, fakeRelKey);
    if (!RelationIsValid(cbiScan->fakeBucketRelation)) {
        searchHBucketFakeRelation(cbiScan->fakeRelationTable, cxt, cbiScan->parentRelation, cbiScan->bucketid,
            cbiScan->fakeBucketRelation);
    }

    return RelationIsValid(cbiScan->fakeBucketRelation);
}
