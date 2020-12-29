/* -------------------------------------------------------------------------
 *
 * toasting.cpp
 *	  This file contains routines to support creation of toast tables
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/toasting.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_hashbucket_fn.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/partitionmap.h"

/* Potentially set by contrib/pg_upgrade_support functions */
static bool create_toast_table(Relation rel, Oid toastOid, Oid toastIndexOid,
    Datum reloptions, bool isPartition, List *filenodelist);
static bool needs_toast_table(Relation rel);

static void updateCatalogToastRelid(Oid relOid, Oid toast_relid, bool isPartition);

static bool createToastTableForPartitionedTable(Relation rel, Datum reloptions, List *flienodelist);

static Oid binary_upgrade_get_next_part_toast_pg_type_oid();
static bool binary_upgrade_is_next_part_toast_pg_type_oid_valid();
extern bool binary_upgrade_is_next_part_toast_pg_class_oid_valid();

/*
 * AlterTableCreateToastTable
 *		If the table needs a toast table, and doesn't already have one,
 *		then create a toast table for it.
 *
 * reloptions for the toast table can be passed, too.  Pass (Datum) 0
 * for default reloptions.
 *
 * We expect the caller to have verified that the relation is a table and have
 * already done any necessary permission checks.  Callers expect this function
 * to end with CommandCounterIncrement if it makes any changes.
 */
void AlterTableCreateToastTable(Oid relOid, Datum reloptions, List *filenodelist)
{
    Relation rel;
    bool rel_is_partitioned = check_rel_is_partitioned(relOid);
    if (!rel_is_partitioned) {
        /*
         * Grab an exclusive lock on the target table, since we'll update its
         * pg_class tuple. This is redundant for all present uses, since caller
         * will have such a lock already.  But the lock is needed to ensure that
         * concurrent readers of the pg_class tuple won't have visibility issues,
         * so let's be safe.
         */
        rel = heap_open(relOid, AccessExclusiveLock);
        if (needs_toast_table(rel))
            (void)create_toast_table(rel, InvalidOid, InvalidOid, reloptions, false, filenodelist);
    } else {
        rel = heap_open(relOid, AccessShareLock);
        if (needs_toast_table(rel))
            (void)createToastTableForPartitionedTable(rel, reloptions, filenodelist);
    }

    heap_close(rel, NoLock);
}

/*
 * Create a toast table during bootstrap
 *
 * Here we need to prespecify the OIDs of the toast table and its index
 */
void BootstrapToastTable(char* relName, Oid toastOid, Oid toastIndexOid)
{
    Relation rel;

    rel = heap_openrv(makeRangeVar(NULL, relName, -1), AccessExclusiveLock);
    if (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_MATVIEW)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a table or materialized view", relName)));

    /* create_toast_table does all the work */
    if (!create_toast_table(rel, toastOid, toastIndexOid, (Datum)0, false, NULL))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("\"%s\" does not require a toast table", relName)));

    heap_close(rel, NoLock);
}

/*
 * create_toast_table --- internal workhorse
 *
 * rel is already opened and locked
 * toastOid and toastIndexOid are normally InvalidOid, but during
 * bootstrap they can be nonzero to specify hand-assigned OIDs
 */
static bool create_toast_table(Relation rel, Oid toastOid, Oid toastIndexOid, Datum reloptions,
    bool isPartition, List *filenodelist)
{
    Oid relOid = RelationGetRelid(rel);
    TupleDesc tupdesc;
    bool shared_relation = false;
    bool mapped_relation = false;
    Relation toast_rel;
    Oid toast_relid;
    Oid toast_typid = InvalidOid;
    Oid namespaceid;
    char toast_relname[NAMEDATALEN];
    char toast_idxname[NAMEDATALEN];
    IndexInfo* indexInfo = NULL;
    Oid collationObjectId[2];
    Oid classObjectId[2];
    int16 coloptions[2];
    ObjectAddress baseobject, toastobject;
    errno_t rc = EOK;
    HashBucketInfo bucketinfo;
    Oid indexfilenode = InvalidOid;
    List *relfilenode = NULL;

    /*
     * Is it already toasted?
     */
    if (rel->rd_rel->reltoastrelid != InvalidOid)
        return false;

    /*
     * Check to see whether the table actually needs a TOAST table.
     *
     * If an update-in-place toast relfilenode is specified, force toast file
     * creation even if it seems not to need one.
     */
    if (!needs_toast_table(rel) &&
        (!u_sess->proc_cxt.IsBinaryUpgrade ||
            !(((!isPartition) && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid)) ||
                (isPartition && binary_upgrade_is_next_part_toast_pg_class_oid_valid()))))
        return false;

    /*
     * Toast table is shared if and only if its parent is.
     *
     * We cannot allow toasting a shared relation after initdb (because
     * there's no way to mark it toasted in other databases' pg_class).
     */
    shared_relation = rel->rd_rel->relisshared;
    if (shared_relation && !u_sess->attr.attr_common.IsInplaceUpgrade && !IsBootstrapProcessingMode())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("shared tables cannot be toasted after gs_initdb")));

    /* It's mapped if and only if its parent is, too */
    mapped_relation = RelationIsMapped(rel);

    /*
     * Create the toast table and its index
     */
    if (!isPartition) {
        rc = snprintf_s(toast_relname, sizeof(toast_relname), sizeof(toast_relname) - 1, "pg_toast_%u", relOid);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(toast_idxname, sizeof(toast_idxname), sizeof(toast_idxname) - 1, "pg_toast_%u_index", relOid);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(toast_relname, sizeof(toast_relname), sizeof(toast_relname) - 1, "pg_toast_part_%u", relOid);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(
            toast_idxname, sizeof(toast_idxname), sizeof(toast_idxname) - 1, "pg_toast_part_%u_index", relOid);
        securec_check_ss(rc, "\0", "\0");
    }

    /* this is pretty painful...  need a tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "chunk_id", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "chunk_seq", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "chunk_data", BYTEAOID, -1, 0);

    /*
     * Ensure that the toast table doesn't itself get toasted, or we'll be
     * toast :-(.  This is essential for chunk_data because type bytea is
     * toastable; hit the other two just to be sure.
     */
    tupdesc->attrs[0]->attstorage = 'p';
    tupdesc->attrs[1]->attstorage = 'p';
    tupdesc->attrs[2]->attstorage = 'p';

    /*
     * Toast tables for regular relations go in pg_toast; those for temp
     * relations go into the per-backend temp-toast-table namespace.
     */
    if (isTempOrToastNamespace(rel->rd_rel->relnamespace))
        namespaceid = GetTempToastNamespace();
    else
        namespaceid = PG_TOAST_NAMESPACE;

    /* Use binary-upgrade override for pg_type.oid, if supplied. */
    if (!isPartition && u_sess->proc_cxt.IsBinaryUpgrade &&
        OidIsValid(u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid)) {
        toast_typid = u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid;
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid = InvalidOid;
    } else if (isPartition && u_sess->proc_cxt.IsBinaryUpgrade &&
               binary_upgrade_is_next_part_toast_pg_type_oid_valid()) {
        toast_typid = binary_upgrade_get_next_part_toast_pg_type_oid();
    }

    if (t_thrd.xact_cxt.inheritFileNode) {
        relfilenode  = lappend_oid(relfilenode,  list_nth_oid(filenodelist, 0));		
        indexfilenode = list_nth_oid(filenodelist, 1);
    }

    bucketinfo.bucketOid = RelationGetBucketOid(rel);
    toast_relid = heap_create_with_catalog(toast_relname,
        namespaceid,
        rel->rd_rel->reltablespace,
        toastOid,
        toast_typid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        RELKIND_TOASTVALUE,
        rel->rd_rel->relpersistence,
        shared_relation,
        mapped_relation,
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        relfilenode,
        RELATION_CREATE_BUCKET(rel) ? &bucketinfo : NULL);
    Assert(toast_relid != InvalidOid);
    list_free_ext(relfilenode);

    /* make the toast relation visible, else heap_open will fail */
    CommandCounterIncrement();

    /* ShareLock is not really needed here, but take it anyway */
    toast_rel = heap_open(toast_relid, ShareLock);

    /*
     * Create unique index on chunk_id, chunk_seq.
     *
     * NOTE: the normal TOAST access routines could actually function with a
     * single-column index on chunk_id only. However, the slice access
     * routines use both columns for faster access to an individual chunk. In
     * addition, we want it to be unique as a check against the possibility of
     * duplicate TOAST chunk OIDs. The index might also be a little more
     * efficient this way, since btree isn't all that happy with large numbers
     * of equal keys.
     */
    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 2;
    indexInfo->ii_NumIndexKeyAttrs = indexInfo->ii_NumIndexAttrs;
    indexInfo->ii_KeyAttrNumbers[0] = 1;
    indexInfo->ii_KeyAttrNumbers[1] = 2;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_PgClassAttrId = 0;

    collationObjectId[0] = InvalidOid;
    collationObjectId[1] = InvalidOid;

    classObjectId[0] = OID_BTREE_OPS_OID;
    classObjectId[1] = INT4_BTREE_OPS_OID;

    coloptions[0] = 0;
    coloptions[1] = 0;

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    index_create(toast_rel,
        toast_idxname,
        toastIndexOid,
        indexfilenode,
        indexInfo,
        list_make2((void*)"chunk_id", (void*)"chunk_seq"),
        BTREE_AM_OID,
        rel->rd_rel->reltablespace,
        collationObjectId,
        classObjectId,
        coloptions,
        (Datum)0,
        true,
        false,
        false,
        false,
        true,
        !u_sess->upg_cxt.new_catalog_need_storage,
        false,
        &extra);

    heap_close(toast_rel, NoLock);

    /*
     * Store the toast table's OID in the parent relation's catalog row
     */
    updateCatalogToastRelid(relOid, toast_relid, isPartition);

    /*
     * Register dependency from the toast table to the master, so that the
     * toast table will be deleted if the master is.  Skip this in bootstrap
     * mode. OTOH, record pinned dependency during inplace upgrade.
     */
    if (!IsBootstrapProcessingMode() && !isPartition) {
        toastobject.classId = RelationRelationId;
        toastobject.objectId = toast_relid;
        toastobject.objectSubId = 0;

        if (u_sess->attr.attr_common.IsInplaceUpgrade && toastobject.objectId < FirstBootstrapObjectId)
            recordPinnedDependency(&toastobject);
        else {
            baseobject.classId = RelationRelationId;
            baseobject.objectId = relOid;
            baseobject.objectSubId = 0;

            recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
        }
    }

    /*
     * Make changes visible
     */
    CommandCounterIncrement();

    return true;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: ceate a toast for a special partition
 * Description	:
 * Input		: relOid: partitioned table's oid
 *			: partOid: table partition's oid
 *			: reloptions: options for partition, inherits from partitioned table
 * Output	:
 * Return		: If succeed in creating a toast table, return true; else return false
 * Notes		:
 */
bool createToastTableForPartition(Oid relOid, Oid partOid, Datum reloptions, List *relfilenode)
{
    Relation partRel = NULL;
    Relation rel = NULL;
    Partition partition = NULL;
    bool result = false;

    /* already toasted? */
    if (partitionHasToast(partOid)) {
        return false;
    }

    rel = relation_open(relOid, NoLock);
    partition = partitionOpen(rel, partOid, AccessExclusiveLock);

    /*
     * create toast table for the special table partition
     * fake a relation and then invoke create_toast_table to
     * create the toast table
     */
    partRel = partitionGetRelation(rel, partition);

    Assert(PointerIsValid(partRel));
    result = create_toast_table(partRel, InvalidOid, InvalidOid, reloptions, true, relfilenode);

    releaseDummyRelation(&partRel);

    partitionClose(rel, partition, NoLock);
    relation_close(rel, NoLock);

    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:  create toast table for partitioned table when table is being created.
 * Description	:
 * Input		: rel: Relation for partitioned table
 *			: reloptions: options for toast table.
 * Output	:
 * Return		: If some one toast table is created successfully, return true, else false.
 * Notes		:
 */
static bool createToastTableForPartitionedTable(Relation rel, Datum reloptions, List *filenodelist)
{
    Oid partition = InvalidOid;
    List* partitionList = NIL;
    ListCell* cell = NULL;
    List *relfilenode = NIL;
    bool result = false;
    int  iter = 0;

    Assert(RELATION_IS_PARTITIONED(rel));

    /*
     * Check to see whether the table actually needs a TOAST table.
     *
     * If an update-in-place toast relfilenode is specified, force toast file
     * creation even if it seems not to need one.
     */
    if (!needs_toast_table(rel) &&
        (!u_sess->proc_cxt.IsBinaryUpgrade || !OidIsValid(u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid))) {
        return false;
    }

    partitionList = relationGetPartitionOidList(rel);

    foreach (cell, partitionList) {
        partition = DatumGetObjectId(lfirst(cell));

        if (t_thrd.xact_cxt.inheritFileNode) {
	    /* toast relfilenode */
	    relfilenode = lappend_oid(relfilenode, list_nth_oid(filenodelist, iter++));
	    /* toast index relfilenode */
	    relfilenode = lappend_oid(relfilenode, list_nth_oid(filenodelist, iter++));		
        }
        result = createToastTableForPartition(rel->rd_id, partition, reloptions, relfilenode);
        list_free_ext(relfilenode);
    }

    if (partitionList != NIL) {
        releasePartitionOidList(&partitionList);
    }

    return result;
}
/*
 * Check to see whether the table needs a TOAST table.	It does only if
 * (1) there are any toastable attributes, and (2) the maximum length
 * of a tuple could exceed TOAST_TUPLE_THRESHOLD.  (We don't want to
 * create a toast table for something like "f1 varchar(20)".)
 */
static bool needs_toast_table(Relation rel)
{
    int32 data_length = 0;
    bool maxlength_unknown = false;
    bool has_toastable_attrs = false;
    TupleDesc tupdesc;
    Form_pg_attribute* att = NULL;
    int32 tuple_length;
    int i;

    /*
     * The existence of newly added catalog toast tables should be
     * explicitly pointed out with predetermined oid during inplace upgrade.
     */
    if (u_sess->attr.attr_common.IsInplaceUpgrade && rel->rd_id < FirstBootstrapObjectId) {
        if (OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid))
            return true;
        else
            return false;
    }

    // column-store relations don't need any toast tables.
    if (RelationIsColStore(rel))
        return false;

    tupdesc = rel->rd_att;
    att = tupdesc->attrs;

    for (i = 0; i < tupdesc->natts; i++) {
        if (att[i]->attisdropped)
            continue;
        data_length = att_align_nominal(data_length, att[i]->attalign);
        if (att[i]->attlen > 0) {
            /* Fixed-length types are never toastable */
            data_length += att[i]->attlen;
        } else {
            int32 maxlen = type_maximum_size(att[i]->atttypid, att[i]->atttypmod);
            if (maxlen < 0)
                maxlength_unknown = true;
            else
                data_length += maxlen;
            if (att[i]->attstorage != 'p')
                has_toastable_attrs = true;
        }
    }
    if (!has_toastable_attrs) {
        return false; /* nothing to toast? */
    }
    if (maxlength_unknown) {
        return true; /* any unlimited-length attrs? */
    }
    tuple_length = MAXALIGN(offsetof(HeapTupleHeaderData, t_bits) + BITMAPLEN(tupdesc->natts)) + MAXALIGN(data_length);
    return ((unsigned long)tuple_length > TOAST_TUPLE_THRESHOLD);
}

static void updateCatalogToastRelid(Oid relOid, Oid toast_relid, bool isPartition)
{
    Relation rel = NULL;
    HeapTuple reltup = NULL;
    Oid catalogRelId;
    enum SysCacheIdentifier catalogIndex;

    if (!isPartition) {
        catalogRelId = RelationRelationId;
        catalogIndex = RELOID;
    } else {
        catalogRelId = PartitionRelationId;
        catalogIndex = PARTRELID;
    }

    rel = heap_open(catalogRelId, RowExclusiveLock);

    reltup = SearchSysCacheCopy1(catalogIndex, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(reltup)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation or partition %u", relOid)));
        return;
    }

    if (!isPartition) {
        ((Form_pg_class)GETSTRUCT(reltup))->reltoastrelid = toast_relid;
    } else {
        ((Form_pg_partition)GETSTRUCT(reltup))->reltoastrelid = toast_relid;
    }

    if (!IsBootstrapProcessingMode()) {
        /* normal case, use a transactional update */
        simple_heap_update(rel, &reltup->t_self, reltup);

        /* Keep catalog indexes current */
        CatalogUpdateIndexes(rel, reltup);
    } else {
        /* While bootstrapping, we cannot UPDATE, so overwrite in-place */
        heap_inplace_update(rel, reltup);
    }

    heap_freetuple(reltup);

    heap_close(rel, RowExclusiveLock);
}

static bool binary_upgrade_is_next_part_toast_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_type_oid == NULL) {
        return false;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid >=
        u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_type_oid
                        [u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid binary_upgrade_get_next_part_toast_pg_type_oid()
{
    Oid old_part_toast_pg_type_oid;
    if (false == binary_upgrade_is_next_part_toast_pg_type_oid_valid()) {
        return InvalidOid;
    }

    old_part_toast_pg_type_oid =
        u_sess->upg_cxt
            .binary_upgrade_next_part_toast_pg_type_oid[u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_toast_pg_type_oid[u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid++;

    return old_part_toast_pg_type_oid;
}
