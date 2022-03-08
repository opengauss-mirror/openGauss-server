/* -------------------------------------------------------------------------
 *
 * index.cpp
 *	  code to create and destroy openGauss index relations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/index.cpp
 *
 *
 * INTERFACE ROUTINES
 *		index_create()			- Create a cataloged index relation
 *		index_drop()			- Removes index relation from catalogs
 *		BuildIndexInfo()		- Prepare to insert index tuples
 *		FormIndexDatum()		- Construct datum vector for one index tuple
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cstore_delta.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/ustore/knl_uscan.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/knl_uheap.h"
#include "bootstrap/bootstrap.h"
#include "catalog/catalog.h"
#include "catalog/cstore_ctlg.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "storage/tcap.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "storage/lock/lock.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_hashbucket_fn.h"
#include "pgxc/pgxc.h"
#include "optimizer/planner.h"
#include "postmaster/bgworker.h"
#include "utils/rel.h"
#include "pgxc/redistrib.h"

#ifdef ENABLE_MOT
#include "foreign/fdwapi.h"
#endif


/* non-export function prototypes */
static bool relationHasPrimaryKey(Relation rel);
static TupleDesc ConstructTupleDescriptor(Relation heapRelation, IndexInfo* indexInfo, List* indexColNames,
    Oid accessMethodObjectId, Oid* collationObjectId, Oid* classObjectId);
static void InitializeAttributeOids(Relation indexRelation, int numatts, Oid indexoid);
static void AppendAttributeTuples(Relation indexRelation, int numatts);
static void UpdateIndexRelation(Oid indexoid, Oid heapoid, IndexInfo* indexInfo, Oid* collationOids, Oid* classOids,
    int16* coloptions, bool primary, bool isexclusion, bool immediate, bool isvalid);
static void IndexCheckExclusion(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo);
static void IndexCheckExclusionForBucket(Relation heapRelation, Partition heapPartition, Relation indexRelation,
    Partition indexPartition, IndexInfo* indexInfo);
static bool validate_index_callback(ItemPointer itemptr, void* opaque, Oid partOid = InvalidOid,
    int2 bktId = InvalidBktId);
static bool ReindexIsCurrentlyProcessingIndex(Oid indexOid);
static void SetReindexProcessing(Oid heapOid, Oid indexOid);
static void ResetReindexProcessing(void);
static void SetReindexPending(List* indexes);
static void RemoveReindexPending(Oid indexOid);
static void ResetReindexPending(void);

static void reindexPartIndex(Oid indexId, Oid partOid, bool flag);

Oid psort_create(const char* indexRelationName, Relation indexRelation, Oid tablespaceId, Datum indexRelOptions);

extern char* ChoosePSortIndexName(const char* tabname, Oid namespaceId, List* colnames);

static bool binary_upgrade_is_next_part_index_pg_class_oid_valid();
static Oid binary_upgrade_get_next_part_index_pg_class_oid();

static Oid bupgrade_get_next_psort_pg_class_oid();
static bool binary_upgrade_is_next_psort_pg_class_oid_valid();
static Oid bupgrade_get_next_psort_pg_type_oid();
static bool binary_upgrade_is_next_psort_pg_type_oid_valid();
static Oid bupgrade_get_next_psort_array_pg_type_oid();
static bool binary_upgrade_is_next_psort_array_pg_type_oid_valid();
static Oid binary_upgrade_get_next_part_index_pg_class_rfoid();
static Oid bupgrade_get_next_psort_pg_class_rfoid();

static const int max_hashbucket_index_worker = 32;

static inline int get_parallel_workers(Relation heap)
{
    int parallel_workers = RelationGetParallelWorkers(heap, 0);

    if (parallel_workers != 0) {
        parallel_workers = Min(max_hashbucket_index_worker, parallel_workers);
    }

    return parallel_workers;
}

/*
 * relationHasPrimaryKey
 *		See whether an existing relation has a primary key.
 *
 * Caller must have suitable lock on the relation.
 *
 * Note: we intentionally do not check IndexIsValid here; that's because this
 * is used to enforce the rule that there can be only one indisprimary index,
 * and we want that to be true even if said index is invalid.
 */
static bool relationHasPrimaryKey(Relation rel)
{
    bool result = false;
    List* indexoidlist = NIL;
    ListCell* indexoidscan = NULL;

    /*
     * Get the list of index OIDs for the table from the relcache, and look up
     * each one in the pg_index syscache until we find one marked primary key
     * (hopefully there isn't more than one such).
     */
    indexoidlist = RelationGetIndexList(rel);

    foreach (indexoidscan, indexoidlist) {
        Oid indexoid = lfirst_oid(indexoidscan);
        HeapTuple indexTuple;

        indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if (!HeapTupleIsValid(indexTuple)) /* should not happen */
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexoid)));
        result = ((Form_pg_index)GETSTRUCT(indexTuple))->indisprimary;
        ReleaseSysCache(indexTuple);
        if (result)
            break;
    }

    list_free(indexoidlist);

    return result;
}

/*
 * index_check_primary_key
 *		Apply special checks needed before creating a PRIMARY KEY index
 *
 * This processing used to be in DefineIndex(), but has been split out
 * so that it can be applied during ALTER TABLE ADD PRIMARY KEY USING INDEX.
 *
 * We check for a pre-existing primary key, and that all columns of the index
 * are simple column references (not expressions), and that all those
 * columns are marked NOT NULL.  If they aren't (which can only happen during
 * ALTER TABLE ADD CONSTRAINT, since the parser forces such columns to be
 * created NOT NULL during CREATE TABLE), do an ALTER SET NOT NULL to mark
 * them so --- or fail if they are not in fact nonnull.
 *
 * Caller had better have at least ShareLock on the table, else the not-null
 * checking isn't trustworthy.
 */
void index_check_primary_key(Relation heapRel, IndexInfo* indexInfo, bool is_alter_table)
{
    List* cmds = NIL;
    int i;

    /*
     * If ALTER TABLE, check that there isn't already a PRIMARY KEY. In CREATE
     * TABLE, we have faith that the parser rejected multiple pkey clauses;
     * and CREATE INDEX doesn't have a way to say PRIMARY KEY, so it's no
     * problem either.
     */
    if (is_alter_table && relationHasPrimaryKey(heapRel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("multiple primary keys for table \"%s\" are not allowed", RelationGetRelationName(heapRel))));
    }

    /*
     * Check that all of the attributes in a primary key are marked as not
     * null, otherwise attempt to ALTER TABLE .. SET NOT NULL
     */
    cmds = NIL;
    for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++) {
        AttrNumber attnum = indexInfo->ii_KeyAttrNumbers[i];
        HeapTuple atttuple;
        Form_pg_attribute attform;

        if (attnum == 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("primary keys cannot be expressions")));

        /* System attributes are never null, so no need to check */
        if (attnum < 0)
            continue;

        atttuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(RelationGetRelid(heapRel)), Int16GetDatum(attnum));
        if (!HeapTupleIsValid(atttuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for attribute %d of relation %u", attnum, RelationGetRelid(heapRel))));
        attform = (Form_pg_attribute)GETSTRUCT(atttuple);

        if (!attform->attnotnull) {
            /* Add a subcommand to make this one NOT NULL */
            AlterTableCmd* cmd = makeNode(AlterTableCmd);

            cmd->subtype = AT_SetNotNull;
            cmd->name = pstrdup(NameStr(attform->attname));
            cmds = lappend(cmds, cmd);
        }

        ReleaseSysCache(atttuple);
    }

    /*
     * XXX: Shouldn't the ALTER TABLE .. SET NOT NULL cascade to child tables?
     * Currently, since the PRIMARY KEY itself doesn't cascade, we don't
     * cascade the notnull constraint(s) either; but this is pretty debatable.
     *
     * XXX: possible future improvement: when being called from ALTER TABLE,
     * it would be more efficient to merge this with the outer ALTER TABLE, so
     * as to avoid two scans.  But that seems to complicate DefineIndex's API
     * unduly.
     */
    if (cmds != NULL)
        AlterTableInternal(RelationGetRelid(heapRel), cmds, false);
}

/*
 *		ConstructTupleDescriptor
 *
 * Build an index tuple descriptor for a new index
 */
static TupleDesc ConstructTupleDescriptor(Relation heapRelation, IndexInfo* indexInfo, List* indexColNames,
    Oid accessMethodObjectId, Oid* collationObjectId, Oid* classObjectId)
{
    int numatts = indexInfo->ii_NumIndexAttrs;
    int numkeyatts = indexInfo->ii_NumIndexKeyAttrs;
    ListCell* colnames_item = list_head(indexColNames);
    ListCell* indexpr_item = list_head(indexInfo->ii_Expressions);
    HeapTuple amtuple;
    Form_pg_am amform;
    TupleDesc heapTupDesc;
    TupleDesc indexTupDesc;
    int natts; /* #atts in heap rel --- for error checks */
    int i;
    errno_t rc = EOK;

    /* We need access to the index AM's pg_am tuple */
    amtuple = SearchSysCache1(AMOID, ObjectIdGetDatum(accessMethodObjectId));
    if (!HeapTupleIsValid(amtuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for access method %u", accessMethodObjectId)));
    amform = (Form_pg_am)GETSTRUCT(amtuple);

    /* ... and to the table's tuple descriptor */
    heapTupDesc = RelationGetDescr(heapRelation);
    natts = RelationGetForm(heapRelation)->relnatts;

    /*
     * allocate the new tuple descriptor
     */
    indexTupDesc = CreateTemplateTupleDesc(numatts, false, TAM_HEAP);

    /*
     * For simple index columns, we copy the pg_attribute row from the parent
     * relation and modify it as necessary.  For expressions we have to cons
     * up a pg_attribute row the hard way.
     */
    for (i = 0; i < numatts; i++) {
        AttrNumber atnum = indexInfo->ii_KeyAttrNumbers[i];
        Form_pg_attribute to = indexTupDesc->attrs[i];
        HeapTuple tuple;
        Form_pg_type typeTup;
        Form_pg_opclass opclassTup;
        Oid keyType;

        if (atnum != 0) {
            /* Simple index column */
            Form_pg_attribute from;

            if (atnum < 0) {
                /*
                 * here we are indexing on a system attribute (-1...-n)
                 */
                from = SystemAttributeDefinition(atnum, heapRelation->rd_rel->relhasoids,
                    RELATION_HAS_BUCKET(heapRelation), RELATION_HAS_UIDS(heapRelation));
            } else {
                /*
                 * here we are indexing on a normal attribute (1...n)
                 */
                if (atnum > natts) /* safety check */
                    ereport(
                        ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid column number %d", atnum)));
                from = heapTupDesc->attrs[AttrNumberGetAttrOffset(atnum)];
            }

            /*
             * now that we've determined the "from", let's copy the tuple desc
             * data...
             */
            rc = memcpy_s(to, ATTRIBUTE_FIXED_PART_SIZE, from, ATTRIBUTE_FIXED_PART_SIZE);
            securec_check(rc, "\0", "\0");

            /*
             * Fix the stuff that should not be the same as the underlying
             * attr
             */
            to->attnum = i + 1;

            to->attstattarget = -1;
            to->attcacheoff = -1;
            to->attnotnull = false;
            to->atthasdef = false;
            to->attislocal = true;
            to->attinhcount = 0;
            to->attcollation = (i < numkeyatts) ? collationObjectId[i] : InvalidOid;
        } else {
            /* Expressional index */
            Node* indexkey = NULL;

            rc = memset_s(to, ATTRIBUTE_FIXED_PART_SIZE, 0, ATTRIBUTE_FIXED_PART_SIZE);
            securec_check(rc, "", "");

            if (indexpr_item == NULL) /* shouldn't happen */
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too few entries in indexprs list")));
            indexkey = (Node*)lfirst(indexpr_item);
            indexpr_item = lnext(indexpr_item);

            /*
             * Lookup the expression type in pg_type for the type length etc.
             */
            keyType = exprType(indexkey);
            tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
            if (!HeapTupleIsValid(tuple))
                ereport(
                    ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", keyType)));
            typeTup = (Form_pg_type)GETSTRUCT(tuple);

            /*
             * Assign some of the attributes values. Leave the rest as 0.
             */
            to->attnum = i + 1;
            to->atttypid = keyType;
            to->attlen = typeTup->typlen;
            to->attbyval = typeTup->typbyval;
            to->attstorage = typeTup->typstorage;
            to->attalign = typeTup->typalign;
            to->attstattarget = -1;
            to->attcacheoff = -1;
            to->atttypmod = -1;
            to->attislocal = true;
            to->attcollation = (i < numkeyatts) ? collationObjectId[i] : InvalidOid;

            ReleaseSysCache(tuple);

            /*
             * Make sure the expression yields a type that's safe to store in
             * an index.  We need this defense because we have index opclasses
             * for pseudo-types such as "record", and the actually stored type
             * had better be safe; eg, a named composite type is okay, an
             * anonymous record type is not.  The test is the same as for
             * whether a table column is of a safe type (which is why we
             * needn't check for the non-expression case).
             */
            CheckAttributeType(NameStr(to->attname), to->atttypid, to->attcollation, NIL, false);
        }

        /*
         * We do not yet have the correct relation OID for the index, so just
         * set it invalid for now.	InitializeAttributeOids() will fix it
         * later.
         */
        to->attrelid = InvalidOid;

        /*
         * Set the attribute name as specified by caller.
         */
        if (colnames_item == NULL) /* shouldn't happen */
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too few entries in colnames list")));
        (void)namestrcpy(&to->attname, (const char*)lfirst(colnames_item));
        colnames_item = lnext(colnames_item);

        /*
         * Check the opclass and index AM to see if either provides a keytype
         * (overriding the attribute type).  Opclass (if exists) takes
         * precedence.
         */
        keyType = amform->amkeytype;

        /*
         * Code below is concerned to the opclasses which are not used with
         * the included columns.
         */
        if (i < indexInfo->ii_NumIndexKeyAttrs) {

            tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(classObjectId[i]));
            if (!HeapTupleIsValid(tuple)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for opclass %u", classObjectId[i])));
            }
            opclassTup = (Form_pg_opclass)GETSTRUCT(tuple);
            if (OidIsValid(opclassTup->opckeytype)) {
                keyType = opclassTup->opckeytype;
            }

            /*
             * If keytype is specified as ANYELEMENT, and opcintype is
             * ANYARRAY, then the attribute type must be an array (else it'd
             * not have matched this opclass); use its element type.
             */
            if (keyType == ANYELEMENTOID && opclassTup->opcintype == ANYARRAYOID) {
                keyType = get_base_element_type(to->atttypid);
                if (!OidIsValid(keyType)) {
                    ereport(ERROR, (errmsg("could not get element type of array type %u", to->atttypid)));
                }
            }
            ReleaseSysCache(tuple);
        }

        if (OidIsValid(keyType) && keyType != to->atttypid) {
            /* index value and heap value have different types */
            tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
            if (!HeapTupleIsValid(tuple))
                ereport(
                    ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", keyType)));
            typeTup = (Form_pg_type)GETSTRUCT(tuple);

            to->atttypid = keyType;
            to->atttypmod = -1;
            to->attlen = typeTup->typlen;
            to->attbyval = typeTup->typbyval;
            to->attalign = typeTup->typalign;
            to->attstorage = typeTup->typstorage;

            ReleaseSysCache(tuple);
        }
    }

    ReleaseSysCache(amtuple);

    return indexTupDesc;
}

/* ----------------------------------------------------------------
 *		InitializeAttributeOids
 * ----------------------------------------------------------------
 */
static void InitializeAttributeOids(Relation indexRelation, int numatts, Oid indexoid)
{
    TupleDesc tupleDescriptor;
    int i;

    tupleDescriptor = RelationGetDescr(indexRelation);

    for (i = 0; i < numatts; i += 1)
        tupleDescriptor->attrs[i]->attrelid = indexoid;
}

/* ----------------------------------------------------------------
 *		AppendAttributeTuples
 * ----------------------------------------------------------------
 */
static void AppendAttributeTuples(Relation indexRelation, int numatts)
{
    Relation pg_attribute;
    CatalogIndexState indstate;
    TupleDesc indexTupDesc;
    int i;

    /*
     * open the attribute relation and its indexes
     */
    pg_attribute = heap_open(AttributeRelationId, RowExclusiveLock);

    indstate = CatalogOpenIndexes(pg_attribute);

    /*
     * insert data from new index's tupdesc into pg_attribute
     */
    indexTupDesc = RelationGetDescr(indexRelation);

    for (i = 0; i < numatts; i++) {
        /*
         * There used to be very grotty code here to set these fields, but I
         * think it's unnecessary.  They should be set already.
         */
        Assert(indexTupDesc->attrs[i]->attnum == i + 1);
        Assert(indexTupDesc->attrs[i]->attcacheoff == -1);

        InsertPgAttributeTuple(pg_attribute, indexTupDesc->attrs[i], indstate);
    }

    CatalogCloseIndexes(indstate);

    heap_close(pg_attribute, RowExclusiveLock);
}

/* ----------------------------------------------------------------
 *		UpdateIndexRelation
 *
 * Construct and insert a new entry in the pg_index catalog
 * ----------------------------------------------------------------
 */
static void UpdateIndexRelation(Oid indexoid, Oid heapoid, IndexInfo* indexInfo, Oid* collationOids, Oid* classOids,
    int16* coloptions, bool primary, bool isexclusion, bool immediate, bool isvalid)
{
    int2vector* indkey = NULL;
    oidvector* indcollation = NULL;
    oidvector* indclass = NULL;
    int2vector* indoption = NULL;
    Datum exprsDatum;
    Datum predDatum;
    Datum values[Natts_pg_index];
    bool nulls[Natts_pg_index];
    Relation pg_index;
    HeapTuple tuple;
    int i;
    errno_t rc = EOK;

    /*
     * Copy the index key, opclass, and indoption info into arrays (should we
     * make the caller pass them like this to start with?)
     */
    indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
    for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
        indkey->values[i] = indexInfo->ii_KeyAttrNumbers[i];
    indcollation = buildoidvector(collationOids, indexInfo->ii_NumIndexKeyAttrs);
    indclass = buildoidvector(classOids, indexInfo->ii_NumIndexKeyAttrs);
    indoption = buildint2vector(coloptions, indexInfo->ii_NumIndexKeyAttrs);

    /*
     * Convert the index expressions (if any) to a text datum
     */
    if (indexInfo->ii_Expressions != NIL) {
        char* exprsString = NULL;

        exprsString = nodeToString(indexInfo->ii_Expressions);
        exprsDatum = CStringGetTextDatum(exprsString);
        pfree(exprsString);
    } else
        exprsDatum = (Datum)0;

    /*
     * Convert the index predicate (if any) to a text datum.  Note we convert
     * implicit-AND format to normal explicit-AND for storage.
     */
    if (indexInfo->ii_Predicate != NIL) {
        char* predString = NULL;

        predString = nodeToString(make_ands_explicit(indexInfo->ii_Predicate));
        predDatum = CStringGetTextDatum(predString);
        pfree(predString);
    } else
        predDatum = (Datum)0;

    /*
     * open the system catalog index relation
     */
    pg_index = heap_open(IndexRelationId, RowExclusiveLock);

    /*
     * Build a pg_index tuple
     */
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");

    values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
    values[Anum_pg_index_indrelid - 1] = ObjectIdGetDatum(heapoid);
    values[Anum_pg_index_indnatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexAttrs);
    values[Anum_pg_index_indisunique - 1] = BoolGetDatum(indexInfo->ii_Unique);
    values[Anum_pg_index_indisprimary - 1] = BoolGetDatum(primary);
    values[Anum_pg_index_indisexclusion - 1] = BoolGetDatum(isexclusion);
    values[Anum_pg_index_indimmediate - 1] = BoolGetDatum(immediate);
    values[Anum_pg_index_indisclustered - 1] = BoolGetDatum(false);
    values[Anum_pg_index_indisusable - 1] = BoolGetDatum(true);
    values[Anum_pg_index_indisvalid - 1] = BoolGetDatum(isvalid);
    values[Anum_pg_index_indcheckxmin - 1] = BoolGetDatum(false);
    /* we set isvalid and isready the same way */
    values[Anum_pg_index_indisready - 1] = BoolGetDatum(isvalid);
    values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);
    values[Anum_pg_index_indcollation - 1] = PointerGetDatum(indcollation);
    values[Anum_pg_index_indclass - 1] = PointerGetDatum(indclass);
    values[Anum_pg_index_indoption - 1] = PointerGetDatum(indoption);
    values[Anum_pg_index_indexprs - 1] = exprsDatum;
    if (exprsDatum == (Datum)0)
        nulls[Anum_pg_index_indexprs - 1] = true;
    values[Anum_pg_index_indpred - 1] = predDatum;
    if (predDatum == (Datum)0)
        nulls[Anum_pg_index_indpred - 1] = true;

    values[Anum_pg_index_indisreplident - 1] = BoolGetDatum(false);
    values[Anum_pg_index_indnkeyatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexKeyAttrs);
    tuple = heap_form_tuple(RelationGetDescr(pg_index), values, nulls);

    /*
     * insert the tuple into the pg_index catalog
     */
    (void)simple_heap_insert(pg_index, tuple);

    /* update the indexes on pg_index */
    CatalogUpdateIndexes(pg_index, tuple);

    /*
     * close the relation and free the tuple
     */
    heap_close(pg_index, RowExclusiveLock);
    heap_freetuple(tuple);
}

/*
 * index_build_init_fork
 *
 * If this is an unlogged index, we may need to write out an init fork for
 * it -- but we must first check whether one already exists.  If, for
 * example, an unlogged relation is truncated in the transaction that
 * created it, or truncated twice in a subsequent transaction, the
 * relfilenode won't change, and nothing needs to be done here.
 */
static void index_build_init_fork(Relation heapRelation, Relation indexRelation)
{

    /* here rd_smgr may be NULL. if so, reopen it */
    RelationOpenSmgr(indexRelation);

    if (!smgrexists(indexRelation->rd_smgr, INIT_FORKNUM)) {
        if (RelationIsColStore(heapRelation) && indexRelation->rd_rel->relam == PSORT_AM_OID) {
            /* for psort index relation based on column heap relation,
             * because no btbuildempty-like method is provided, so call
             * heap_create_init_fork() to handle INIT_FORKNUM file.
             */
            heap_create_init_fork(indexRelation);
        } else {
            /* logic for handle index relation based on row heap relation */
            RegProcedure ambuildempty = indexRelation->rd_am->ambuildempty;

            RelationOpenSmgr(indexRelation);
            /* first create INIT_FORKNUM file */
            smgrcreate(indexRelation->rd_smgr, INIT_FORKNUM, false);
            /* then callback will log and sync INIT_FORKNUM file content */
            OidFunctionCall1(ambuildempty, PointerGetDatum(indexRelation));
        }
    }
}

/*
 * index_create
 *
 * heapRelation: table to build index on (suitably locked by caller)
 * indexRelationName: what it say
 * indexRelationId: normally, pass InvalidOid to let this routine
 *		generate an OID for the index.	During bootstrap this may be
 *		nonzero to specify a preselected OID.
 * relFileNode: normally, pass InvalidOid to get new storage.  May be
 *		nonzero to attach an existing valid build.
 * indexInfo: same info executor uses to insert into the index
 * indexColNames: column names to use for index (List of char *)
 * accessMethodObjectId: OID of index AM to use
 * tableSpaceId: OID of tablespace to use
 * collationObjectId: array of collation OIDs, one per index column
 * classObjectId: array of index opclass OIDs, one per index column
 * coloptions: array of per-index-column indoption settings
 * reloptions: AM-specific options
 * isprimary: index is a PRIMARY KEY
 * isconstraint: index is owned by PRIMARY KEY, UNIQUE, or EXCLUSION constraint
 * deferrable: constraint is DEFERRABLE
 * initdeferred: constraint is INITIALLY DEFERRED
 * allow_system_table_mods: allow table to be a system catalog
 * skip_build: true to skip the index_build() step for the moment; caller
 *		must do it later (typically via reindex_index())
 * concurrent: if true, do not lock the table against writers.	The index
 *		will be marked "invalid" and the caller must take additional steps
 *		to fix it up.
 *
 * Returns the OID of the created index.
 */
Oid index_create(Relation heapRelation, const char *indexRelationName, Oid indexRelationId, Oid relFileNode,
    IndexInfo *indexInfo, List *indexColNames, Oid accessMethodObjectId, Oid tableSpaceId, Oid *collationObjectId,
    Oid *classObjectId, int16 *coloptions, Datum reloptions, bool isprimary, bool isconstraint, bool deferrable,
    bool initdeferred, bool allow_system_table_mods, bool skip_build, bool concurrent, IndexCreateExtraArgs *extra,
    bool useLowLockLevel, int8 relindexsplit)
{
    Oid heapRelationId = RelationGetRelid(heapRelation);
    Relation pg_class;
    Relation indexRelation;
    TupleDesc indexTupDesc;
    bool shared_relation = false;
    bool mapped_relation = false;
    bool is_exclusion = false;
    Oid namespaceId;
    int i;
    char relpersistence;
    Oid psortRelationId = InvalidOid;
    bool skip_create_storage = false;

    if (RELATION_IS_GLOBAL_TEMP(heapRelation) && !gtt_storage_attached(RelationGetRelid(heapRelation))) {
        skip_create_storage = true;
    }

    is_exclusion = (indexInfo->ii_ExclusionOps != NULL);
    bool isUstore = RelationIsUstoreFormat(heapRelation);

    pg_class = heap_open(RelationRelationId, RowExclusiveLock);

    /*
     * The index will be in the same namespace as its parent table, and is
     * shared across databases if and only if the parent is.  Likewise, it
     * will use the relfilenode map if and only if the parent does; and it
     * inherits the parent's relpersistence.
     */
    namespaceId = RelationGetNamespace(heapRelation);
    shared_relation = heapRelation->rd_rel->relisshared;
    mapped_relation = RelationIsMapped(heapRelation);
    relpersistence = heapRelation->rd_rel->relpersistence;

    /*
     * check parameters
     */
    if (indexInfo->ii_NumIndexAttrs < 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("must index at least one column")));

    if (indexInfo->ii_NumIndexKeyAttrs > INDEX_MAX_KEYS) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("must index at most %u column", INDEX_MAX_KEYS)));
    }

    if (!allow_system_table_mods && IsSystemRelation(heapRelation) && IsNormalProcessingMode())
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("user-defined indexes on system catalog tables are not supported")));

    /*
     * The index columns are forbidden to be created on partition clumns for dfs table.
     */
    if (RelationIsValuePartitioned(heapRelation)) {
        List* partList = ((ValuePartitionMap*)heapRelation->partMap)->partList;
        for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
            if (list_member_int(partList, indexInfo->ii_KeyAttrNumbers[i]))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("user-defined indexes on value partition columns are not supported")));
        }
    }


    /*
     * concurrent index not yet supported
     */
    if (isUstore && concurrent)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent index creation is not supported yet in ustore")));

    /*
     * concurrent index build on a system catalog is unsafe because we tend to
     * release locks before committing in catalogs
     */
    if (concurrent && IsSystemRelation(heapRelation))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent index creation on system catalog tables is not supported")));

    /*
     * This case is currently not supported, but there's no way to ask for it
     * in the grammar anyway, so it can't happen.
     */
    if (concurrent && is_exclusion)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg_internal("concurrent index creation for exclusion constraints is not supported")));

    /*
     * We cannot allow indexing a shared relation after initdb (because
     * there's no way to make the entry in other databases' pg_class).
     */
    if (shared_relation && !u_sess->attr.attr_common.IsInplaceUpgrade && !IsBootstrapProcessingMode())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("shared indexes cannot be created after gs_initdb")));

    /*
     * Shared relations must be in pg_global, too (last-ditch check)
     */
    if (shared_relation && tableSpaceId != GLOBALTABLESPACE_OID)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("shared relations must be placed in pg_global tablespace")));

    if (get_relname_relid(indexRelationName, namespaceId))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE), errmsg("relation \"%s\" already exists", indexRelationName)));

    if (RELATION_IS_GLOBAL_TEMP(heapRelation)) {
        /* No support create index on global temp table use concurrent mode yet */
        if (concurrent) {
            ereport(
                ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot reindex global temporary tables concurrently")));
        }

        /* if global temp table not init storage, then skip build index */
        if (!gtt_storage_attached(RelationGetRelid(heapRelation))) {
            skip_build = true;
        }
    }

    /*
     * construct tuple descriptor for index tuples
     */
    indexTupDesc = ConstructTupleDescriptor(
        heapRelation, indexInfo, indexColNames, accessMethodObjectId, collationObjectId, classObjectId);

    /*
     * Allocate an OID for the index, unless we were told what to use.
     *
     * The OID will be the relfilenode as well, so make sure it doesn't
     * collide with either pg_class OIDs or existing physical files.
     */
    if (!OidIsValid(indexRelationId)) {
        bool isPartition_toast_idx = ((0 == strncmp(indexRelationName, "pg_toast_part", strlen("pg_toast_part"))));

        /*
         * Use binary-upgrade override for pg_class.oid/relfilenode, if
         * supplied.
         */
        if ((!isPartition_toast_idx) &&
            (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid))) {
            indexRelationId = u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid;
            u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = InvalidOid;
            relFileNode = u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = InvalidOid;
        } else if (isPartition_toast_idx &&
                   (u_sess->proc_cxt.IsBinaryUpgrade && (binary_upgrade_is_next_part_index_pg_class_oid_valid()))) {
            indexRelationId = binary_upgrade_get_next_part_index_pg_class_oid();
            relFileNode = binary_upgrade_get_next_part_index_pg_class_rfoid();
        } else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
                   OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_index_pg_class_oid)) {
            /* So far, system catalogs are not partitioned. */
            Assert(!isPartition_toast_idx);

            indexRelationId = u_sess->upg_cxt.Inplace_upgrade_next_index_pg_class_oid;
            u_sess->upg_cxt.Inplace_upgrade_next_index_pg_class_oid = InvalidOid;
        } else {
            indexRelationId = GetNewRelFileNode(tableSpaceId, pg_class, relpersistence);
        }
    }

    char relKind = RELKIND_INDEX;
    bool isLocalPart = false;

    if (extra->isGlobalPartitionedIndex) {
        relKind = RELKIND_GLOBAL_INDEX;
    }
    /* 
     * for normal relation index and global partition index, isLocalPart should be false.
     * more description refers to defination of IndexCreateExtraArgs;
     */
    if (extra->isPartitionedIndex && !extra->isGlobalPartitionedIndex) {
        isLocalPart = true;
    }
    /*
     * create the index relation's relcache entry and physical disk file. (If
     * we fail further down, it's the smgr's responsibility to remove the disk
     * file again.)
     */
    StorageType storage_type = RelationGetStorageType(heapRelation);
    indexRelation = heap_create(indexRelationName, namespaceId, tableSpaceId, indexRelationId, relFileNode,
        RELATION_CREATE_BUCKET(heapRelation) ? heapRelation->rd_bucketoid : InvalidOid, indexTupDesc, relKind,
        relpersistence, isLocalPart, false, shared_relation, mapped_relation, allow_system_table_mods,
        REL_CMPRS_NOT_SUPPORT, (Datum)reloptions, heapRelation->rd_rel->relowner, skip_create_storage,
        isUstore ? TAM_USTORE : TAM_HEAP, /* XXX: Index tables are by default HEAP Table Type */
        relindexsplit, storage_type, extra->crossBucket, accessMethodObjectId);

    Assert(indexRelationId == RelationGetRelid(indexRelation));

    /*
     * Obtain exclusive lock on it.  Although no other backends can see it
     * until we commit, this prevents deadlock-risk complaints from lock
     * manager in cases such as CLUSTER.
     */
    LockRelation(indexRelation, useLowLockLevel ? AccessShareLock : AccessExclusiveLock);

    /*
     * Fill in fields of the index's pg_class entry that are not set correctly
     * by heap_create.
     *
     * XXX should have a cleaner way to create cataloged indexes
     */
    indexRelation->rd_rel->relowner = heapRelation->rd_rel->relowner;
    indexRelation->rd_rel->relhasoids = false;

    if (accessMethodObjectId == PSORT_AM_OID) {
        if (extra && OidIsValid(extra->existingPSortOid)) {
            // reuse the existing psort oid, if any.
            psortRelationId = extra->existingPSortOid;
        } else {
            // create psort relation
            char psortNamePrefix[NAMEDATALEN] = {0};
            errno_t rc = sprintf_s(psortNamePrefix, (NAMEDATALEN - 1), "psort_%u", indexRelationId);
            securec_check_ss(rc, "", "");

            char* psortIdxName = ChoosePSortIndexName(psortNamePrefix, CSTORE_NAMESPACE, indexColNames);
            psortRelationId = psort_create(psortIdxName, indexRelation, tableSpaceId, reloptions);
        }
        // using index relation 's relcudescrelid store psort relation id
        indexRelation->rd_rel->relcudescrelid = psortRelationId;
    }

    indexRelation->rd_bucketoid = heapRelation->rd_bucketoid;

    /*
     * store index's pg_class entry
     */
    InsertPgClassTuple(
        pg_class, indexRelation, RelationGetRelid(indexRelation), (Datum)0, reloptions, relKind, NULL);

    /* done with pg_class */
    heap_close(pg_class, RowExclusiveLock);

    /*
     * now update the object id's of all the attribute tuple forms in the
     * index relation's tuple descriptor
     */
    InitializeAttributeOids(indexRelation, indexInfo->ii_NumIndexAttrs, indexRelationId);

    /*
     * append ATTRIBUTE tuples for the index
     */
    AppendAttributeTuples(indexRelation, indexInfo->ii_NumIndexAttrs);

    /* ----------------
     *	  update pg_index
     *	  (append INDEX tuple)
     *
     *	  Note that this stows away a representation of "predicate".
     *	  (Or, could define a rule to maintain the predicate) --Nels, Feb '92
     * ----------------
     */
    UpdateIndexRelation(indexRelationId,
        heapRelationId,
        indexInfo,
        collationObjectId,
        classObjectId,
        coloptions,
        isprimary,
        is_exclusion,
        !deferrable,
        !concurrent);

    /*
     * Register constraint and dependencies for the index.
     *
     * If the index is from a CONSTRAINT clause, construct a pg_constraint
     * entry.  The index will be linked to the constraint, which in turn is
     * linked to the table.  If it's not a CONSTRAINT, we need to make a
     * dependency directly on the table.
     *
     * We don't need a dependency on the namespace, because there'll be an
     * indirect dependency via our parent table.
     *
     * During bootstrap we can't register any dependencies, and we don't try
     * to make a constraint either.
     *
     * During inplace/grey upgrade, we only record pinned dependency for the new index.
     */
    if (!IsBootstrapProcessingMode()) {
        ObjectAddress myself;

        myself.classId = RelationRelationId;
        myself.objectId = indexRelationId;
        myself.objectSubId = 0;

        if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId)
            recordPinnedDependency(&myself);
        else {
            ObjectAddress referenced;

            if (isconstraint) {
                char constraintType;

                if (isprimary)
                    constraintType = CONSTRAINT_PRIMARY;
                else if (indexInfo->ii_Unique)
                    constraintType = CONSTRAINT_UNIQUE;
                else if (is_exclusion)
                    constraintType = CONSTRAINT_EXCLUSION;
                else {
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("constraint must be PRIMARY, UNIQUE or EXCLUDE")));
                    constraintType = 0; /* keep compiler quiet */
                }

                index_constraint_create(heapRelation,
                    indexRelationId,
                    indexInfo,
                    indexRelationName,
                    constraintType,
                    deferrable,
                    initdeferred,
                    false, /* already marked primary */
                    false, /* pg_index entry is OK */
                    false, /* no old dependencies */
                    allow_system_table_mods);
            } else {
                bool have_simple_col = false;

                /* Create auto dependencies on simply-referenced columns */
                for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
                    if (indexInfo->ii_KeyAttrNumbers[i] != 0) {
                        referenced.classId = RelationRelationId;
                        referenced.objectId = heapRelationId;
                        referenced.objectSubId = indexInfo->ii_KeyAttrNumbers[i];

                        recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

                        have_simple_col = true;
                    }
                }

                /*
                 * If there are no simply-referenced columns, give the index an
                 * auto dependency on the whole table.	In most cases, this will
                 * be redundant, but it might not be if the index expressions and
                 * predicate contain no Vars or only whole-row Vars.
                 */
                if (!have_simple_col) {
                    referenced.classId = RelationRelationId;
                    referenced.objectId = heapRelationId;
                    referenced.objectSubId = 0;

                    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
                }

                /* Non-constraint indexes can't be deferrable */
                Assert(!deferrable);
                Assert(!initdeferred);
            }

            /* Store dependency on collations */
            /* The default collation is pinned, so don't bother recording it */
            for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++) {
                if (OidIsValid(collationObjectId[i]) && collationObjectId[i] != DEFAULT_COLLATION_OID) {
                    referenced.classId = CollationRelationId;
                    referenced.objectId = collationObjectId[i];
                    referenced.objectSubId = 0;

                    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
                }
            }

            /* Store dependency on operator classes */
            for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++) {
                referenced.classId = OperatorClassRelationId;
                referenced.objectId = classObjectId[i];
                referenced.objectSubId = 0;

                recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
            }

            /* Store dependencies on anything mentioned in index expressions */
            if (indexInfo->ii_Expressions) {
                recordDependencyOnSingleRelExpr(
                    &myself, (Node*)indexInfo->ii_Expressions, heapRelationId, DEPENDENCY_NORMAL, DEPENDENCY_AUTO);
            }

            /* Store dependencies on anything mentioned in predicate */
            if (indexInfo->ii_Predicate) {
                recordDependencyOnSingleRelExpr(
                    &myself, (Node*)indexInfo->ii_Predicate, heapRelationId, DEPENDENCY_NORMAL, DEPENDENCY_AUTO);
            }

            /* Store dependency on psort */
            if (accessMethodObjectId == PSORT_AM_OID && psortRelationId != InvalidOid) {
                referenced.classId = RelationRelationId;
                referenced.objectId = psortRelationId;
                referenced.objectSubId = 0;

                recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
            }
        }
    } else {
        /* Bootstrap mode - assert we weren't asked for constraint support */
        Assert(!isconstraint);
        Assert(!deferrable);
        Assert(!initdeferred);
    }

    /*
     * Advance the command counter so that we can see the newly-entered
     * catalog tuples for the index.
     */
    CommandCounterIncrement();

    /*
     * In bootstrap mode, we have to fill in the index strategy structure with
     * information from the catalogs.  If we aren't bootstrapping, then the
     * relcache entry has already been rebuilt thanks to sinval update during
     * CommandCounterIncrement.
     */
    if (IsBootstrapProcessingMode())
        RelationInitIndexAccessInfo(indexRelation);
    else
        Assert(indexRelation->rd_indexcxt != NULL);

    indexRelation->rd_indnkeyatts = indexInfo->ii_NumIndexKeyAttrs;
    /*
     * If this is bootstrap (initdb) time, then we don't actually fill in the
     * index yet.  We'll be creating more indexes and classes later, so we
     * delay filling them in until just before we're done with bootstrapping.
     * Similarly, if the caller specified skip_build then filling the index is
     * delayed till later (ALTER TABLE can save work in some cases with this).
     * Otherwise, we call the AM routine that constructs the index.
     */
    if (IsBootstrapProcessingMode()) {
        index_register(heapRelationId, indexRelationId, indexInfo);
    } else if (skip_build) {
        /*
         * Caller is responsible for filling the index later on.  However,
         * we'd better make sure that the heap relation is correctly marked as
         * having an index.
         */
        index_update_stats(heapRelation,
            true,
            isprimary,
            (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation) : InvalidOid,
            InvalidOid,
            -1.0);
        /* Make the above update visible */
        CommandCounterIncrement();
    } else if (extra && (!extra->isPartitionedIndex || extra->isGlobalPartitionedIndex)) {
        /* support regular index or GLOBAL partition index */
        index_build(heapRelation, NULL, indexRelation, NULL, indexInfo, isprimary, false, PARTITION_TYPE(extra));
    }

    /* Recode the index create time. */
    if (OidIsValid(indexRelationId)) {
        PgObjectOption objectOpt = {true, true, true, true};
        CreatePgObject(indexRelationId, OBJECT_TYPE_INDEX, indexRelation->rd_rel->relowner, objectOpt);
    }

    /*
     * Close the index; but we keep the lock that we acquired above until end
     * of transaction.	Closing the heap is caller's responsibility.
     */
    index_close(indexRelation, NoLock);
    CacheInvalidateRelcache(heapRelation);

    return indexRelationId;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: create a partiton index ,insert tuple to pg_partition,and build
 *			: the index, keep RowExclusiveLock on pg_partition table
 * Description	:
 * Notes		:
 * Input		: partIndexFileNode:
                  whether an existing index definition is compatible with
                  a prospective index definition, such that the existing
                  index storage could become the storage of the new index,
                  avoiding a rebuild.
 */
Oid partition_index_create(const char* partIndexName, /* the name of partition index*/
    Oid partIndexFileNode,                            /* index file node */
	Partition partition,                              /* the partition create index for */
    Oid tspid,                                        /* the tablespace id to create partition index in */
    Relation parentIndex,                             /* relation of partitioned index */
    Relation partitionedTable,                        /* relation of partitioned table */
    Relation pg_partition_rel, IndexInfo* indexInfo, List* indexColNames, Datum indexRelOptions, bool skipBuild,
    PartIndexCreateExtraArgs* extra)
{

    Oid indexid = InvalidOid;
    Partition partitionIndex = NULL;
    Oid parentIndexId = parentIndex->rd_id;

    if (u_sess->proc_cxt.IsBinaryUpgrade && (binary_upgrade_is_next_part_index_pg_class_oid_valid())) {
        indexid = binary_upgrade_get_next_part_index_pg_class_oid();
        partIndexFileNode = binary_upgrade_get_next_part_index_pg_class_rfoid();
    } else {
        /* get a new Oid for index partition */
        indexid = GetNewRelFileNode(tspid, pg_partition_rel, RELPERSISTENCE_PERMANENT);
        if (OidIsValid(partIndexFileNode) && !skipBuild) {
            ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("A valid partIndexFileNode implies that we already have a built form of the index.")));
        }
    }

    /* create the indexPartition */
    partitionIndex = heapCreatePartition(partIndexName,
        false,
        tspid,
        indexid,
        partIndexFileNode,
        parentIndex->rd_bucketoid,
        parentIndex->rd_rel->relowner,
        RelationGetStorageType(parentIndex),
        extra->crossbucket,
        indexRelOptions);
    partitionIndex->pd_part->parttype = PART_OBJ_TYPE_INDEX_PARTITION;
    partitionIndex->pd_part->rangenum = 0;
    partitionIndex->pd_part->parentid = parentIndexId;
    partitionIndex->pd_part->intervalnum = 0;
    partitionIndex->pd_part->partstrategy = PART_STRATEGY_INVALID;
    partitionIndex->pd_part->reltoastrelid = InvalidOid;
    partitionIndex->pd_part->reltoastidxid = InvalidOid;
    partitionIndex->pd_part->indextblid = PartitionGetPartid(partition);
    partitionIndex->pd_part->indisusable = partition->pd_part->indisusable;

    // We create psort index table if partitionedTable is a CStore table
    //
    if (RelationIsColStore(partitionedTable) && parentIndex->rd_rel->relam == PSORT_AM_OID) {
        if (extra && OidIsValid(extra->existingPSortOid)) {
            // reuse the existing psort oid.
            partitionIndex->pd_part->relcudescrelid = extra->existingPSortOid;
        } else {
            // create psort relation
            char psortNamePrefix[NAMEDATALEN] = {0};
            errno_t rc = sprintf_s(psortNamePrefix, (NAMEDATALEN - 1), "psort_%u", parentIndex->rd_id);
            securec_check_ss(rc, "", "");

            char* psortIdxName = ChoosePSortIndexName(psortNamePrefix, CSTORE_NAMESPACE, indexColNames);
            // using index relation 's relcudescrelid store psort relation id
            partitionIndex->pd_part->relcudescrelid = psort_create(psortIdxName, parentIndex, tspid, indexRelOptions);
        }
    }

    /* lock Partition */
    LockPartition(parentIndexId, partitionIndex->pd_id, AccessExclusiveLock, PARTITION_LOCK);

    partitionIndex->pd_part->relpages = 0;
    partitionIndex->pd_part->reltuples = 0;
    partitionIndex->pd_part->relallvisible = 0;
    partitionIndex->pd_part->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

    /* insert into pg_partition */
#ifndef ENABLE_MULTIPLE_NODES
    insertPartitionEntry(pg_partition_rel, partitionIndex, partitionIndex->pd_id, NULL, NULL, 0, 0, 0, indexRelOptions,
                         PART_OBJ_TYPE_INDEX_PARTITION);
#else
    insertPartitionEntry(
        pg_partition_rel, partitionIndex, partitionIndex->pd_id, NULL, NULL, 0, 0, 0, 0, PART_OBJ_TYPE_INDEX_PARTITION);
#endif
    /* Make the above change visible */
    CommandCounterIncrement();

    /* build the index */
    if (!skipBuild) {
        index_build(partitionedTable,
            partition,
            parentIndex,
            partitionIndex,
            indexInfo,
            false,
            false,
            INDEX_CREATE_LOCAL_PARTITION);
    }

    partitionClose(parentIndex, partitionIndex, NoLock);

    return indexid;
}

/*
 * index_constraint_create
 *
 * Set up a constraint associated with an index
 *
 * heapRelation: table owning the index (must be suitably locked by caller)
 * indexRelationId: OID of the index
 * indexInfo: same info executor uses to insert into the index
 * constraintName: what it say (generally, should match name of index)
 * constraintType: one of CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, or
 *		CONSTRAINT_EXCLUSION
 * deferrable: constraint is DEFERRABLE
 * initdeferred: constraint is INITIALLY DEFERRED
 * mark_as_primary: if true, set flags to mark index as primary key
 * update_pgindex: if true, update pg_index row (else caller's done that)
 * remove_old_dependencies: if true, remove existing dependencies of index
 *		on table's columns
 * allow_system_table_mods: allow table to be a system catalog
 */
void index_constraint_create(Relation heapRelation, Oid indexRelationId, IndexInfo* indexInfo,
    const char* constraintName, char constraintType, bool deferrable, bool initdeferred, bool mark_as_primary,
    bool update_pgindex, bool remove_old_dependencies, bool allow_system_table_mods)
{
    Oid namespaceId = RelationGetNamespace(heapRelation);
    ObjectAddress myself, referenced;
    Oid conOid;

    /* constraint creation support doesn't work while bootstrapping */
    Assert(!IsBootstrapProcessingMode());

    /* enforce system-table restriction */
    if (!allow_system_table_mods && IsSystemRelation(heapRelation) && IsNormalProcessingMode())
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("user-defined indexes on system catalog tables are not supported")));

    /* primary/unique constraints shouldn't have any expressions */
    if (indexInfo->ii_Expressions && constraintType != CONSTRAINT_EXCLUSION)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("constraints cannot have index expressions")));

    /*
     * If we're manufacturing a constraint for a pre-existing index, we need
     * to get rid of the existing auto dependencies for the index (the ones
     * that index_create() would have made instead of calling this function).
     *
     * Note: this code would not necessarily do the right thing if the index
     * has any expressions or predicate, but we'd never be turning such an
     * index into a UNIQUE or PRIMARY KEY constraint.
     */
    if (remove_old_dependencies) {
        (void)deleteDependencyRecordsForClass(RelationRelationId, indexRelationId, 
            RelationRelationId, DEPENDENCY_AUTO);
    }

    /*
     * Construct a pg_constraint entry.
     */
    conOid = CreateConstraintEntry(constraintName,
        namespaceId,
        constraintType,
        deferrable,
        initdeferred,
        true,
        RelationGetRelid(heapRelation),
        indexInfo->ii_KeyAttrNumbers,
        indexInfo->ii_NumIndexKeyAttrs,
        indexInfo->ii_NumIndexAttrs,
        InvalidOid,      /* no domain */
        indexRelationId, /* index OID */
        InvalidOid,      /* no foreign key */
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        ' ',
        ' ',
        ' ',
        indexInfo->ii_ExclusionOps,
        NULL, /* no check constraint */
        NULL,
        NULL,
        true,  /* islocal */
        0,     /* inhcount */
        true,  /* noinherit */
        NULL); /* @hdfs informational constraint */

    /*
     * Register the index as internally dependent on the constraint.
     *
     * Note that the constraint has a dependency on the table, so we don't
     * need (or want) any direct dependency from the index to the table.
     */
    myself.classId = RelationRelationId;
    myself.objectId = indexRelationId;
    myself.objectSubId = 0;

    referenced.classId = ConstraintRelationId;
    referenced.objectId = conOid;
    referenced.objectSubId = 0;

    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);

    /*
     * If the constraint is deferrable, create the deferred uniqueness
     * checking trigger.  (The trigger will be given an internal dependency on
     * the constraint by CreateTrigger.)
     */
    if (deferrable) {
        CreateTrigStmt* trigger = NULL;

        trigger = makeNode(CreateTrigStmt);
        trigger->trigname =
            (char*)((constraintType == CONSTRAINT_PRIMARY) ? "PK_ConstraintTrigger" : "Unique_ConstraintTrigger");
        trigger->relation = NULL;
        trigger->funcname = SystemFuncName("unique_key_recheck");
        trigger->args = NIL;
        trigger->row = true;
        trigger->timing = TRIGGER_TYPE_AFTER;
        trigger->events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE;
        trigger->columns = NIL;
        trigger->whenClause = NULL;
        trigger->isconstraint = true;
        trigger->deferrable = true;
        trigger->initdeferred = initdeferred;
        trigger->constrrel = NULL;

        (void)CreateTrigger(trigger, NULL, RelationGetRelid(heapRelation), InvalidOid, conOid, indexRelationId, true);
    }

    /*
     * If needed, mark the table as having a primary key.  We assume it can't
     * have been so marked already, so no need to clear the flag in the other
     * case.
     *
     * Note: this might better be done by callers.	We do it here to avoid
     * exposing index_update_stats() globally, but that wouldn't be necessary
     * if relhaspkey went away.
     */
    if (mark_as_primary)
        index_update_stats(heapRelation, true, true, InvalidOid, InvalidOid, -1.0);

    /*
     * If needed, mark the index as primary and/or deferred in pg_index.
     *
     * Note: since this is a transactional update, it's unsafe against
     * concurrent SnapshotNow scans of pg_index.  When making an existing
     * index into a constraint, caller must have a table lock that prevents
     * concurrent table updates; if it's less than a full exclusive lock,
     * there is a risk that concurrent readers of the table will miss seeing
     * this index at all.
     */
    if (update_pgindex && (mark_as_primary || deferrable)) {
        Relation pg_index;
        HeapTuple indexTuple;
        Form_pg_index indexForm;
        bool dirty = false;

        pg_index = heap_open(IndexRelationId, RowExclusiveLock);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexRelationId));
        if (!HeapTupleIsValid(indexTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexRelationId)));
        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        if (mark_as_primary && !indexForm->indisprimary) {
            indexForm->indisprimary = true;
            dirty = true;
        }

        if (deferrable && indexForm->indimmediate) {
            indexForm->indimmediate = false;
            dirty = true;
        }

        if (dirty) {
            simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
            CatalogUpdateIndexes(pg_index, indexTuple);
        }

        heap_freetuple(indexTuple);
        heap_close(pg_index, RowExclusiveLock);
    }
}

#ifdef ENABLE_MOT
static void MotFdwDropForeignIndex(Relation userHeapRelation, Relation userIndexRelation)
{
    /* Forward drop stmt to MOT FDW. */
    if (RelationIsForeignTable(userHeapRelation) && isMOTFromTblOid(RelationGetRelid(userHeapRelation))) {
        FdwRoutine* fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(userHeapRelation));
        if (fdwroutine->ValidateTableDef != NULL) {
            DropForeignStmt stmt;
            stmt.type = T_DropForeignStmt;
            stmt.relkind = RELKIND_INDEX;
            stmt.reloid = RelationGetRelid(userHeapRelation);
            stmt.indexoid = RelationGetRelid(userIndexRelation);
            stmt.name = userIndexRelation->rd_rel->relname.data;

            fdwroutine->ValidateTableDef((Node*)&stmt);
        }
    }
}
#endif

/*
 *		index_drop
 *
 * NOTE: this routine should now only be called through performDeletion(),
 * else associated dependencies won't be cleaned up.
 */
void index_drop(Oid indexId, bool concurrent)
{
    Oid heapId;
    Relation userHeapRelation;
    Relation userIndexRelation;
    Relation indexRelation;
    HeapTuple tuple;
    bool hasexprs = false;
    LockRelId heaprelid, indexrelid;
    LOCKTAG heaplocktag;
    LOCKMODE lockmode;
    VirtualTransactionId* old_lockholders = NULL;

    List* partIndexlist = NIL;

    /*
     * A temporary relation uses a non-concurrent DROP.  Other backends can't
     * access a temporary relation, so there's no harm in grabbing a stronger
     * lock (see comments in RemoveRelations), and a non-concurrent DROP is
     * more efficient.
     */
    Assert(!(get_rel_persistence(indexId) == RELPERSISTENCE_TEMP
           || get_rel_persistence(indexId) == RELPERSISTENCE_GLOBAL_TEMP)
           || (!concurrent));

    /*
     * To drop an index safely, we must grab exclusive lock on its parent
     * table.  Exclusive lock on the index alone is insufficient because
     * another backend might be about to execute a query on the parent table.
     * If it relies on a previously cached list of index OIDs, then it could
     * attempt to access the just-dropped index.  We must therefore take a
     * table lock strong enough to prevent all queries on the table from
     * proceeding until we commit and send out a shared-cache-inval notice
     * that will make them update their index lists.
     *
     * In the concurrent case we avoid this requirement by disabling index use
     * in multiple steps and waiting out any transactions that might be using
     * the index, so we don't need exclusive lock on the parent table. Instead
     * we take ShareUpdateExclusiveLock, to ensure that two sessions aren't
     * doing CREATE/DROP INDEX CONCURRENTLY on the same index.	(We will get
     * AccessExclusiveLock on the index below, once we're sure nobody else is
     * using it.)
     */
    heapId = IndexGetRelation(indexId, false);
    lockmode = concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;
    userHeapRelation = heap_open(heapId, lockmode);
    userIndexRelation = index_open(indexId, lockmode);

    /*
     * We might still have open queries using it in our own session, which the
     * above locking won't prevent, so test explicitly.
     */
    CheckTableNotInUse(userIndexRelation, "DROP INDEX");

    /* We allow to drop index on global temp table only this session use it */
    CheckGttTableInUse(userHeapRelation);

    /*
     * Drop Index Concurrently is more or less the reverse process of Create
     * Index Concurrently.
     *
     * First we unset indisvalid so queries starting afterwards don't use the
     * index to answer queries anymore.  We have to keep indisready = true so
     * transactions that are still scanning the index can continue to see
     * valid index contents.  For instance, if they are using READ COMMITTED
     * mode, and another transaction makes changes and commits, they need to
     * see those new tuples in the index.
     *
     * After all transactions that could possibly have used the index for
     * queries end, we can unset indisready and set indisvalid, then wait till
     * nobody could be touching it anymore.  (Note: we use this illogical
     * combination because this state must be distinct from the initial state
     * during CREATE INDEX CONCURRENTLY, which has indisready and indisvalid
     * both false.  That's because in that state, transactions must examine
     * the index for HOT-safety decisions, while in this state we don't want
     * them to open it at all.)
     *
     * Since all predicate locks on the index are about to be made invalid, we
     * must promote them to predicate locks on the heap.  In the
     * non-concurrent case we can just do that now.  In the concurrent case
     * it's a bit trickier.  The predicate locks must be moved when there are
     * no index scans in progress on the index and no more can subsequently
     * start, so that no new predicate locks can be made on the index.	Also,
     * they must be moved before heap inserts stop maintaining the index, else
     * the conflict with the predicate lock on the index gap could be missed
     * before the lock on the heap relation is in place to detect a conflict
     * based on the heap tuple insert.
     */
    if (concurrent) {
        /*
         * We must commit our transaction in order to make the first pg_index
         * state update visible to other sessions.	If the DROP machinery has
         * already performed any other actions (removal of other objects,
         * pg_depend entries, etc), the commit would make those actions
         * permanent, which would leave us with inconsistent catalog state if
         * we fail partway through the following sequence.	Since DROP INDEX
         * CONCURRENTLY is restricted to dropping just one index that has no
         * dependencies, we should get here before anything's been done ---
         * but let's check that to be sure.  We can verify that the current
         * transaction has not executed any transactional updates by checking
         * that no XID has been assigned.
         */
        if (GetTopTransactionIdIfAny() != InvalidTransactionId)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DROP INDEX CONCURRENTLY must be first action in transaction")));

        /*
         * Mark index invalid by updating its pg_index entry
         */
        index_set_state_flags(indexId, INDEX_DROP_CLEAR_VALID);

        /*
         * Invalidate the relcache for the table, so that after this commit
         * all sessions will refresh any cached plans that might reference the
         * index.
         */
        CacheInvalidateRelcache(userHeapRelation);

        /* save lockrelid and locktag for below, then close but keep locks */
        heaprelid = userHeapRelation->rd_lockInfo.lockRelId;
        SET_LOCKTAG_RELATION(heaplocktag, heaprelid.dbId, heaprelid.relId);
        indexrelid = userIndexRelation->rd_lockInfo.lockRelId;

        heap_close(userHeapRelation, NoLock);
        index_close(userIndexRelation, NoLock);

        /*
         * We must commit our current transaction so that the indisvalid
         * update becomes visible to other transactions; then start another.
         * Note that any previously-built data structures are lost in the
         * commit.	The only data we keep past here are the relation IDs.
         *
         * Before committing, get a session-level lock on the table, to ensure
         * that neither it nor the index can be dropped before we finish. This
         * cannot block, even if someone else is waiting for access, because
         * we already have the same lock within our transaction.
         */
        LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
        LockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);

        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();

        /*
         * Now we must wait until no running transaction could be using the
         * index for a query.  To do this, inquire which xacts currently would
         * conflict with AccessExclusiveLock on the table -- ie, which ones
         * have a lock of any kind on the table. Then wait for each of these
         * xacts to commit or abort. Note we do not need to worry about xacts
         * that open the table for reading after this point; they will see the
         * index as invalid when they open the relation.
         *
         * Note: the reason we use actual lock acquisition here, rather than
         * just checking the ProcArray and sleeping, is that deadlock is
         * possible if one of the transactions in question is blocked trying
         * to acquire an exclusive lock on our table.  The lock code will
         * detect deadlock and error out properly.
         *
         * Note: GetLockConflicts() never reports our own xid, hence we need
         * not check for that.	Also, prepared xacts are not reported, which
         * is fine since they certainly aren't going to do anything more.
         */
        old_lockholders = GetLockConflicts(&heaplocktag, AccessExclusiveLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }

        /*
         * No more predicate locks will be acquired on this index, and we're
         * about to stop doing inserts into the index which could show
         * conflicts with existing predicate locks, so now is the time to move
         * them to the heap relation.
         */
        userHeapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
        userIndexRelation = index_open(indexId, ShareUpdateExclusiveLock);
        TransferPredicateLocksToHeapRelation(userIndexRelation);

        /*
         * Now we are sure that nobody uses the index for queries; they just
         * might have it open for updating it.	So now we can unset indisready
         * and set indisvalid, then wait till nobody could be using it at all
         * anymore.
         */
        index_set_state_flags(indexId, INDEX_DROP_SET_DEAD);

        /*
         * Invalidate the relcache for the table, so that after this commit
         * all sessions will refresh the table's index list.  Forgetting just
         * the index's relcache entry is not enough.
         */
        CacheInvalidateRelcache(userHeapRelation);

        /*
         * Close the relations again, though still holding session lock.
         */
        heap_close(userHeapRelation, NoLock);
        index_close(userIndexRelation, NoLock);

        /*
         * Again, commit the transaction to make the pg_index update visible
         * to other sessions.
         */
        CommitTransactionCommand();
        StartTransactionCommand();

        /*
         * Wait till every transaction that saw the old index state has
         * finished.  The logic here is the same as above.
         */
        old_lockholders = GetLockConflicts(&heaplocktag, AccessExclusiveLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }

        /*
         * Re-open relations to allow us to complete our actions.
         *
         * At this point, nothing should be accessing the index, but lets
         * leave nothing to chance and grab AccessExclusiveLock on the index
         * before the physical deletion.
         */
        userHeapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
        userIndexRelation = index_open(indexId, AccessExclusiveLock);
    } else {
        /* Not concurrent, so just transfer predicate locks and we're good */
        TransferPredicateLocksToHeapRelation(userIndexRelation);
    }

    /*
     * Schedule physical removal of the files
     */
    RelationDropStorage(userIndexRelation);

    /* if index is a partitioned index, drop the partition index */
    if (RELATION_IS_PARTITIONED(userHeapRelation)) {
        ListCell* cell = NULL;
        HeapTuple indexTuple = NULL;

        partIndexlist = searchPgPartitionByParentId(PART_OBJ_TYPE_INDEX_PARTITION, indexId);

        foreach (cell, partIndexlist) {
            indexTuple = (HeapTuple)lfirst(cell);
            if (HeapTupleIsValid(indexTuple)) {
                Form_pg_partition rd_part = (Form_pg_partition)GETSTRUCT(indexTuple);

                heapDropPartitionIndex(userIndexRelation, HeapTupleGetOid(indexTuple));

                CacheInvalidatePartcacheByPartid(rd_part->indextblid);
            }
        }

        freePartList(partIndexlist);
    }
#ifndef ENABLE_MULTIPLE_NODES
    /* if index is a `unique` index of `cstore` table, drop index of delta table */
    if (!RELATION_IS_PARTITIONED(userHeapRelation) && RelationIsCUFormat(userHeapRelation) &&
        userIndexRelation->rd_index != NULL && userIndexRelation->rd_index->indisunique) {
        /* Delete index on delta. */
        Oid objectId = GetDeltaIdxFromCUIdx(indexId, false, true);
        if (objectId != InvalidOid) {
            ObjectAddress obj;
            obj.classId = RelationRelationId;
            obj.objectId = objectId;
            obj.objectSubId = 0;
            performDeletion(&obj, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
        }
    }
#endif

#ifdef ENABLE_MOT
    /* Forward drop stmt to MOT FDW. */
    MotFdwDropForeignIndex(userHeapRelation, userIndexRelation);
#endif

    /*
     * Close and flush the index's relcache entry, to ensure relcache doesn't
     * try to rebuild it while we're deleting catalog entries. We keep the
     * lock though.
     */
    index_close(userIndexRelation, NoLock);

    RelationForgetRelation(indexId);

    /*
     * fix INDEX relation, and check for expressional index
     */
    indexRelation = heap_open(IndexRelationId, RowExclusiveLock);

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));

    hasexprs = !heap_attisnull(tuple, Anum_pg_index_indexprs, NULL);

    simple_heap_delete(indexRelation, &tuple->t_self);

    ReleaseSysCache(tuple);
    heap_close(indexRelation, RowExclusiveLock);

    /* Recode time of delete index. */
    DeletePgObject(indexId, OBJECT_TYPE_INDEX);
    UpdatePgObjectChangecsn(heapId, userHeapRelation->rd_rel->relkind);

    /*
     * if it has any expression columns, we might have stored statistics about
     * them.
     */
    if (hasexprs)
        RemoveStatistics<'c'>(indexId, 0);

    /*
     * fix ATTRIBUTE relation
     */
    DeleteAttributeTuples(indexId);

    /*
     * fix RELATION relation
     */
    DeleteRelationTuple(indexId);

    /*
     * We are presently too lazy to attempt to compute the new correct value
     * of relhasindex (the next VACUUM will fix it if necessary). So there is
     * no need to update the pg_class tuple for the owning relation. But we
     * must send out a shared-cache-inval notice on the owning relation to
     * ensure other backends update their relcache lists of indexes.  (In the
     * concurrent case, this is redundant but harmless.)
     */
    CacheInvalidateRelcache(userHeapRelation);

    /*
     * Close owning rel, but keep lock
     */
    heap_close(userHeapRelation, NoLock);

    /*
     * Release the session locks before we go.
     */
    if (concurrent) {
        UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
        UnlockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);
    }
}

/* ----------------------------------------------------------------
 *						index_build support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		BuildIndexInfo
 *			Construct an IndexInfo record for an open index
 *
 * IndexInfo stores the information about the index that's needed by
 * FormIndexDatum, which is used for both index_build() and later insertion
 * of individual index tuples.	Normally we build an IndexInfo for an index
 * just once per command, and then use it for (potentially) many tuples.
 * ----------------
 */
IndexInfo* BuildIndexInfo(Relation index)
{
    Form_pg_index indexStruct = index->rd_index;
    int i;
    int numAtts;

    /* check the number of keys, and copy attr numbers into the IndexInfo */
    numAtts = indexStruct->indnatts;
    if (numAtts < 1 || numAtts > INDEX_MAX_KEYS)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("invalid indnatts %d for index %u", numAtts, RelationGetRelid(index))));

    IndexInfo* ii = makeIndexInfo(indexStruct->indnatts,
                       RelationGetIndexExpressions(index),
                       RelationGetIndexPredicate(index),
                       indexStruct->indisunique,
                       IndexIsReady(indexStruct),
                       false);

    ii->ii_NumIndexAttrs = numAtts;
    ii->ii_NumIndexKeyAttrs = IndexRelationGetNumberOfKeyAttributes(index);
    Assert(ii->ii_NumIndexKeyAttrs != 0);
    Assert(ii->ii_NumIndexKeyAttrs <= ii->ii_NumIndexAttrs);
    /* fill in attribute numbers */
    for (i = 0; i < numAtts; i++) {
        ii->ii_KeyAttrNumbers[i] = indexStruct->indkey.values[i];
    }

    /* fetch exclusion constraint info if any */
    if (indexStruct->indisexclusion) {
        RelationGetExclusionInfo(index, &ii->ii_ExclusionOps, &ii->ii_ExclusionProcs, &ii->ii_ExclusionStrats);
    }

    /* not doing speculative insertion here */
    ii->ii_UniqueOps = NULL;
    ii->ii_UniqueProcs = NULL;
    ii->ii_UniqueStrats = NULL;

    return ii;
}

/* ----------------
 *		BuildDummyIndexInfo
 *			Construct a dummy IndexInfo record for an open index
 *
 * This differs from the real BuildIndexInfo in that it will never run any
 * user-defined code that might exist in index expressions or predicates.
 * Instead of the real index expressions, we return null constants that have
 * the right types/typmods/collations.  Predicates and exclusion clauses are
 * just ignored.  This is sufficient for the purpose of truncating an index,
 * since we will not need to actually evaluate the expressions or predicates;
 * the only thing that's likely to be done with the data is construction of
 * a tupdesc describing the index's rowtype.
 * ----------------
 */
IndexInfo* BuildDummyIndexInfo(Relation index)
{
    IndexInfo* ii;
    Form_pg_index indexStruct = index->rd_index;
    int i;
    int numAtts;

    /* check the number of keys, and copy attr numbers into the IndexInfo */
    numAtts = indexStruct->indnatts;
    if (numAtts < 1 || numAtts > INDEX_MAX_KEYS) {
        elog(ERROR, "invalid indnatts %d for index %u", numAtts, RelationGetRelid(index));
    }

    /*
     * Create the node, using dummy index expressions, and pretending there is
     * no predicate.
     */
    ii = makeIndexInfo(indexStruct->indnatts,
                       RelationGetDummyIndexExpressions(index),
                       NIL,
                       indexStruct->indisunique,
                       indexStruct->indisready,
                       false);

    /* not doing speculative insertion here */
    ii->ii_UniqueOps = NULL;
    ii->ii_UniqueProcs = NULL;
    ii->ii_UniqueStrats = NULL;
    /* initialize index-build state to default */
    ii->ii_Concurrent = false;
    ii->ii_ParallelWorkers = 0;
    ii->ii_BrokenHotChain = false;
    ii->ii_PgClassAttrId = 0;

    /* fill in attribute numbers */
    for (i = 0; i < numAtts; i++) {
        ii->ii_KeyAttrNumbers[i] = indexStruct->indkey.values[i];
    }

    /* We ignore the exclusion constraint if any */
    return ii;
}

void BuildSpeculativeIndexInfo(Relation index, IndexInfo* ii)
{
    int ncols = index->rd_rel->relnatts;
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    int i;

    if (indnkeyatts > 0 && indnkeyatts != ncols) {
        /* make sure no memory leak when containing INCLUDE keys */
        ncols = indnkeyatts;
    }

    /*
     * fetch info for checking unique indexes
     */
    Assert(ii->ii_Unique);

    if (!OID_IS_BTREE(index->rd_rel->relam)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("unexpected non-btree speculative unique index")));
    }

    ii->ii_UniqueOps = (Oid*) palloc(sizeof(Oid) * ncols);
    ii->ii_UniqueProcs = (Oid*) palloc(sizeof(Oid) * ncols);
    ii->ii_UniqueStrats = (uint16*) palloc(sizeof(uint16) * ncols);

    /*
     * We have to look up the operator's strategy number.  This
     * provides a cross-check that the operator does match the index.
     *
     * We need the func OIDs and strategy numbers too
     */
    for (i = 0; i < ncols; i++) {
        ii->ii_UniqueStrats[i] = BTEqualStrategyNumber;
        ii->ii_UniqueOps[i] = get_opfamily_member(index->rd_opfamily[i],
            index->rd_opcintype[i],
            index->rd_opcintype[i],
            ii->ii_UniqueStrats[i]);
        ii->ii_UniqueProcs[i] = get_opcode(ii->ii_UniqueOps[i]);
    }
}

void FormIndexDatumForRedis(const TupleTableSlot* slot, Datum* values, bool* isnull)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    /* the tuple's width */
    uint32 len = tuple->t_len;
    /* the tuple header pointer */
    HeapTupleHeader tup = tuple->t_data;
    char* tp = (char*)tup;
    /* get the last column info, its type is bigint */
    values[1] = *((Datum*)(tp + len - sizeof(int64)));
    isnull[1] = false;
    /* get the last but one column info, its type is bigint */
    values[0] = *((Datum*)(tp + len - sizeof(int64) * 2));
    isnull[0] = false;
}

/* ----------------
 *		FormIndexDatum
 *			Construct values[] and isnull[] arrays for a new index tuple.
 *
 *	indexInfo		Info about the index
 *	slot			Heap tuple for which we must prepare an index entry
 *	estate			executor state for evaluating any index expressions
 *	values			Array of index Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * When there are no index expressions, estate may be NULL.  Otherwise it
 * must be supplied, *and* the ecxt_scantuple slot of its per-tuple expr
 * context must point to the heap tuple passed in.
 *
 * Notice we don't actually call index_form_tuple() here; we just prepare
 * its input arrays values[] and isnull[].	This is because the index AM
 * may wish to alter the data before storage.
 * ----------------
 */
void FormIndexDatum(IndexInfo* indexInfo, TupleTableSlot* slot, EState* estate, Datum* values, bool* isnull)
{
    ListCell* indexpr_item = NULL;
    int i;

    if (indexInfo->ii_Expressions != NIL && indexInfo->ii_ExpressionsState == NIL) {
        /* First time through, set up expression evaluation state */
        indexInfo->ii_ExpressionsState = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Expressions, estate);
        /* Check caller has set up context correctly */
        Assert(GetPerTupleExprContext(estate)->ecxt_scantuple == slot);
    }
    indexpr_item = list_head(indexInfo->ii_ExpressionsState);

    for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        int keycol = indexInfo->ii_KeyAttrNumbers[i];
        Datum iDatum;
        bool isNull = false;

        if (keycol != 0) {
            /*
             * Plain index column; get the value we need directly from the
             * heap tuple.
             */
        	/* Get the Table Accessor Method*/
            iDatum = tableam_tslot_getattr(slot, keycol, &isNull);
        } else {
            /*
             * Index expression --- need to evaluate it.
             */
            if (indexpr_item == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("wrong number of index expressions")));
            iDatum = ExecEvalExprSwitchContext(
                (ExprState*)lfirst(indexpr_item), GetPerTupleExprContext(estate), &isNull, NULL);
            indexpr_item = lnext(indexpr_item);
        }
        values[i] = iDatum;
        isnull[i] = isNull;
    }

    if (indexpr_item != NULL)
        ereport(
            ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("wrong number of index expressions")));
}

/*
 * index_update_stats --- update pg_class entry after CREATE INDEX or REINDEX
 *
 * This routine updates the pg_class row of either an index or its parent
 * relation after CREATE INDEX or REINDEX.	Its rather bizarre API is designed
 * to ensure we can do all the necessary work in just one update.
 *
 * hasindex: set relhasindex to this value
 * isprimary: if true, set relhaspkey true; else no change
 * reltoastidxid: if not InvalidOid, set reltoastidxid to this value;
 *		else no change
 * reltuples: if >= 0, set reltuples to this value; else no change
 *
 * If reltuples >= 0, relpages and relallvisible are also updated (using
 * RelationGetNumberOfBlocks() and visibilitymap_count()).
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new data.	This must happen even
 * if we find that no change is needed in the pg_class row.  When updating
 * a heap entry, this ensures that other backends find out about the new
 * index.  When updating an index, it's important because some index AMs
 * expect a relcache flush to occur after REINDEX.
 */
/* change index_update_stats static to extern */
void index_update_stats(
    Relation rel, bool hasindex, bool isprimary, Oid reltoastidxid, Oid relcudescidx, double reltuples)
{
    Oid relid = RelationGetRelid(rel);
    Relation pg_class;
    HeapTuple tuple;
    Form_pg_class rd_rel;
    bool dirty = false;
    bool is_gtt = false;

    /* update index stats into localhash and rel_rd_rel for global temp table */
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        is_gtt = true;
    }

    /*
     * We always update the pg_class row using a non-transactional,
     * overwrite-in-place update.  There are several reasons for this:
     *
     * 1. In bootstrap mode, we have no choice --- UPDATE wouldn't work.
     *
     * 2. We could be reindexing pg_class itself, in which case we can't move
     * its pg_class row because CatalogUpdateIndexes might not know about all
     * the indexes yet (see ReindexRelation).
     *
     * 3. Because we execute CREATE INDEX with just share lock on the parent
     * rel (to allow concurrent index creations), an ordinary update could
     * suffer a tuple-concurrently-updated failure against another CREATE
     * INDEX committing at about the same time.  We can avoid that by having
     * them both do nontransactional updates (we assume they will both be
     * trying to change the pg_class row to the same thing, so it doesn't
     * matter which goes first).
     *
     * 4. Even with just a single CREATE INDEX, there's a risk factor because
     * someone else might be trying to open the rel while we commit, and this
     * creates a race condition as to whether he will see both or neither of
     * the pg_class row versions as valid.	Again, a non-transactional update
     * avoids the risk.  It is indeterminate which state of the row the other
     * process will see, but it doesn't matter (if he's only taking
     * AccessShareLock, then it's not critical that he see relhasindex true).
     *
     * It is safe to use a non-transactional update even though our
     * transaction could still fail before committing.	Setting relhasindex
     * true is safe even if there are no indexes (VACUUM will eventually fix
     * it), likewise for relhaspkey.  And of course the new relpages and
     * reltuples counts are correct regardless.  However, we don't want to
     * change relpages (or relallvisible) if the caller isn't providing an
     * updated reltuples count, because that would bollix the
     * reltuples/relpages ratio which is what's really important.
     */

    pg_class = heap_open(RelationRelationId, RowExclusiveLock);

    /*
     * Make a copy of the tuple to update.	Normally we use the syscache, but
     * we can't rely on that during bootstrap or while reindexing pg_class
     * itself.
     */
    if (IsBootstrapProcessingMode() || ReindexIsProcessingHeap(RelationRelationId)) {
        /* don't assume syscache will work */
        TableScanDesc pg_class_scan;
        ScanKeyData key[1];

        ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

        pg_class_scan = heap_beginscan(pg_class, SnapshotNow, 1, key);
        tuple = heap_getnext(pg_class_scan, ForwardScanDirection);
        tuple = heap_copytuple(tuple);
        heap_endscan(pg_class_scan);
    } else {
        /* normal case, use syscache */
        tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    }

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("could not find tuple for relation %u", relid)));
    }
    rd_rel = (Form_pg_class)GETSTRUCT(tuple);

    /* Apply required updates, if any, to copied tuple */

    dirty = false;
    if (rd_rel->relhasindex != hasindex) {
        rd_rel->relhasindex = hasindex;
        dirty = true;
    }
    if (isprimary) {
        if (!rd_rel->relhaspkey) {
            rd_rel->relhaspkey = true;
            dirty = true;
        }
    }
    if (OidIsValid(reltoastidxid)) {
        Assert(rd_rel->relkind == RELKIND_TOASTVALUE);
        if (rd_rel->reltoastidxid != reltoastidxid) {
            rd_rel->reltoastidxid = reltoastidxid;
            dirty = true;
        }
    }
    if (OidIsValid(relcudescidx)) {
        if (rd_rel->relcudescidx != relcudescidx) {
            rd_rel->relcudescidx = relcudescidx;
            dirty = true;
        }
    }

    // if it's a datanode, both system and user tables are updated.
    // if it's a coordinator, only system tables are updated, becuause
    // there is no data for user tables in coordinator.
#ifdef PGXC
    if (IS_PGXC_DATANODE || IsSystemRelation(rel)) {
#endif
        if (reltuples >= 0) {
            BlockNumber relpages = RelationGetNumberOfBlocks(rel);
            BlockNumber relallvisible;

            if (rd_rel->relkind != RELKIND_INDEX &&  rd_rel->relkind != RELKIND_GLOBAL_INDEX) {
                relallvisible = visibilitymap_count(rel, NULL);
            } else { /* don't bother for indexes */
                relallvisible = 0;
            }
            if (is_gtt) {
                rel->rd_rel->relpages = static_cast<int32>(relpages);
            } else if (rd_rel->relpages != (float8)relpages) {
                rd_rel->relpages = (float8)relpages;
                dirty = true;
            }

            if (is_gtt) {
                rel->rd_rel->reltuples = (float4) reltuples;
            } else if (rd_rel->reltuples != (float8)reltuples) {
                rd_rel->reltuples = (float8)reltuples;
                dirty = true;
            }

            if (is_gtt) {
                rel->rd_rel->relallvisible = (int32) relallvisible;
            } else if (rd_rel->relallvisible != (int32)relallvisible) {
                rd_rel->relallvisible = (int32)relallvisible;
                dirty = true;
            }

            if (is_gtt) {
                up_gtt_relstats(rel, relpages, reltuples, relallvisible, InvalidTransactionId);
            }
        }
#ifdef PGXC
    }
#endif

    /*
     * If anything changed, write out the tuple
     */
    if (dirty) {
        heap_inplace_update(pg_class, tuple);
        /* the above sends a cache inval message */
    } else {
        /* no need to change tuple, but force relcache inval anyway */
        CacheInvalidateRelcacheByTuple(tuple);
    }

    heap_freetuple(tuple);

    heap_close(pg_class, RowExclusiveLock);
}

static Relation part_get_parent(Partition rel)
{
    Oid parentOid = InvalidOid;
    Relation parent = NULL;

    Assert(rel != NULL);

    if (PartitionIsTableSubPartition(rel)) {
        parentOid = partid_get_parentid(rel->pd_part->parentid);
        parent = relation_open(parentOid, NoLock);
    } else {
        parent = relation_open(rel->pd_part->parentid, NoLock);
    }
    parentOid = partid_get_parentid(rel->pd_part->parentid);

    return parent;
}

static void partition_index_update_stats(
    Partition rel, bool hasindex, bool isprimary, Oid reltoastidxid, Oid relcudescidx, double reltuples)
{

    Oid relid = rel->pd_id;
    Relation pg_partition;
    HeapTuple tuple;
    Form_pg_partition rd_rel;
    bool dirty = false;
    Relation parent = part_get_parent(rel);

    /*
     * We always update the pg_class row using a non-transactional,
     * overwrite-in-place update.  There are several reasons for this:
     *
     * 1. In bootstrap mode, we have no choice --- UPDATE wouldn't work.
     *
     * 2. We could be reindexing pg_class itself, in which case we can't move
     * its pg_class row because CatalogUpdateIndexes might not know about all
     * the indexes yet (see ReindexRelation).
     *
     * 3. Because we execute CREATE INDEX with just share lock on the parent
     * rel (to allow concurrent index creations), an ordinary update could
     * suffer a tuple-concurrently-updated failure against another CREATE
     * INDEX committing at about the same time.  We can avoid that by having
     * them both do nontransactional updates (we assume they will both be
     * trying to change the pg_class row to the same thing, so it doesn't
     * matter which goes first).
     *
     * 4. Even with just a single CREATE INDEX, there's a risk factor because
     * someone else might be trying to open the rel while we commit, and this
     * creates a race condition as to whether he will see both or neither of
     * the pg_class row versions as valid.	Again, a non-transactional update
     * avoids the risk.  It is indeterminate which state of the row the other
     * process will see, but it doesn't matter (if he's only taking
     * AccessShareLock, then it's not critical that he see relhasindex true).
     *
     * It is safe to use a non-transactional update even though our
     * transaction could still fail before committing.	Setting relhasindex
     * true is safe even if there are no indexes (VACUUM will eventually fix
     * it), likewise for relhaspkey.  And of course the new relpages and
     * reltuples counts are correct regardless.  However, we don't want to
     * change relpages (or relallvisible) if the caller isn't providing an
     * updated reltuples count, because that would bollix the
     * reltuples/relpages ratio which is what's really important.
     */
    pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);

    /*
     * Make a copy of the tuple to update.	Normally we use the syscache, but
     * we can't rely on that during bootstrap or while reindexing pg_class
     * itself.
     */
    if (IsBootstrapProcessingMode() || ReindexIsProcessingHeap(RelationRelationId)) {
        /* don't assume syscache will work */
        TableScanDesc pg_class_scan;
        ScanKeyData key[1];

        ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

        pg_class_scan = heap_beginscan(pg_partition, SnapshotNow, 1, key);
        tuple = heap_getnext(pg_class_scan, ForwardScanDirection);
        tuple = heap_copytuple(tuple);
        heap_endscan(pg_class_scan);
    } else {
        /* normal case, use syscache */
        tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(relid));
    }

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("could not find tuple for partition %u", relid)));
    }
    rd_rel = (Form_pg_partition)GETSTRUCT(tuple);

    /* Apply required updates, if any, to copied tuple */

    dirty = false;
    if (OidIsValid(reltoastidxid)) {
        Assert(rd_rel->parttype == PART_OBJ_TYPE_TOAST_TABLE);
        if (rd_rel->reltoastidxid != reltoastidxid) {
            rd_rel->reltoastidxid = reltoastidxid;
            dirty = true;
        }
    }

    if (OidIsValid(relcudescidx)) {
        Assert(rd_rel->parttype == PART_OBJ_TYPE_TOAST_TABLE);
        if (rd_rel->relcudescidx != relcudescidx) {
            rd_rel->relcudescidx = relcudescidx;
            dirty = true;
        }
    }

    // if it's a datanode, both system and user tables are updated.
    // if it's a coordinator, only system tables are updated, becuause
    // there is no data for user tables in coordinator.
#ifdef PGXC
    if (IS_PGXC_DATANODE) {
#endif
        if (reltuples >= 0) {
            /* We record the real data pages for grenerate right explain
             */
            BlockNumber relpages = PartitionGetNumberOfBlocks(parent, rel);
            BlockNumber relallvisible;

            if (rd_rel->parttype != PART_OBJ_TYPE_INDEX_PARTITION) {
                relallvisible = visibilitymap_count(parent, rel);
            } else /* don't bother for indexes */
            {
                relallvisible = 0;
            }

            if (rd_rel->relpages != (float8)relpages) {
                rd_rel->relpages = (float8)relpages;
                dirty = true;
            }
            if (rd_rel->reltuples != (float8)reltuples) {
                rd_rel->reltuples = (float8)reltuples;
                dirty = true;
            }
            if (rd_rel->relallvisible != (int32)relallvisible) {
                rd_rel->relallvisible = (int32)relallvisible;
                dirty = true;
            }
        }
#ifdef PGXC
    }
#endif

    /*
     * If anything changed, write out the tuple
     */
    if (dirty) {
        heap_inplace_update(pg_partition, tuple);
        /* the above sends a cache inval message */
    } else {
        /* no need to change tuple, but force relcache inval anyway */
        CacheInvalidatePartcache(rel);
    }

    heap_freetuple(tuple);

    heap_close(pg_partition, RowExclusiveLock);

    relation_close(parent, NoLock);
}

static IndexBuildResult* index_build_storage(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo)
{
    RegProcedure procedure = indexRelation->rd_am->ambuild;
    Assert(RegProcedureIsValid(procedure));

    IndexBuildResult* stats = (IndexBuildResult*)DatumGetPointer(OidFunctionCall3(
        procedure, PointerGetDatum(heapRelation), PointerGetDatum(indexRelation), PointerGetDatum(indexInfo)));
    Assert(PointerIsValid(stats));
    if (RELPERSISTENCE_UNLOGGED == heapRelation->rd_rel->relpersistence) {
        index_build_init_fork(heapRelation, indexRelation);
    }
    return stats;
}

static void index_build_bucketstorage_main(const BgWorkerContext *bwc)
{
    IndexBucketShared *ibshared = (IndexBucketShared *)bwc->bgshared;
    Relation heapRel  = heap_open(ibshared->heaprelid, NoLock);
    Relation indexRel = index_open(ibshared->indexrelid, NoLock);
    IndexInfo* indexInfo = NULL;
    oidvector *bucketlist = searchHashBucketByOid(indexRel->rd_bucketoid);
    MemoryContext indexbuildContext = NULL;
    MemoryContext old_context = NULL;
    Partition heapPart = NULL;
    Partition indexPart = NULL;
    double indexTuples = 0;
    double heapTuples = 0;

    Assert(OidIsValid(ibshared->heappartid) == OidIsValid(ibshared->indexpartid));
    indexbuildContext = AllocSetContextCreate(CurrentMemoryContext, "indexbucketbuild", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    if (OidIsValid(ibshared->heappartid)) {
         indexPart = partitionOpen(indexRel, ibshared->indexpartid, NoLock);
         heapPart  = partitionOpen(heapRel,  ibshared->heappartid,  NoLock);
    }
    while (true) {
        int bucketid = pg_atomic_fetch_add_u32(&ibshared->curiter, 1);
        if (bucketid >= bucketlist->dim1) {
            break;
        }

        bucketid = bucketlist->values[bucketid];
        Relation indexBucketRel = bucketGetRelation(indexRel, indexPart, bucketid);
        Relation heapBucketRel = bucketGetRelation(heapRel, heapPart, bucketid);
        indexInfo = BuildIndexInfo(indexBucketRel);
        old_context = MemoryContextSwitchTo(indexbuildContext);

        IndexBuildResult* results = index_build_storage(heapBucketRel, indexBucketRel, indexInfo);
        indexTuples += results->index_tuples;
        heapTuples += results->heap_tuples;

        MemoryContextReset(indexbuildContext);
        MemoryContextSwitchTo(old_context);

        bucketCloseRelation(heapBucketRel);
        bucketCloseRelation(indexBucketRel);
    }

    SpinLockAcquire(&ibshared->mutex);
    ibshared->indresult.heap_tuples += heapTuples;
    ibshared->indresult.index_tuples += indexTuples;
    SpinLockRelease(&ibshared->mutex);

    if (OidIsValid(ibshared->heappartid)) {
        partitionClose(indexRel, indexPart, NoLock);
        partitionClose(heapRel, heapPart, NoLock);
    }
    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
    MemoryContextDelete(indexbuildContext);
}

static IndexBuildResult* index_build_bucketstorage_end(IndexBucketShared *ibshared)
{
    BgworkerListWaitFinish(&ibshared->nparticipants);

    if (ibshared->nparticipants <= 0) {
        /* no bgworker is available to do parallel index build */
        return NULL;
    }

    IndexBuildResult* stats = (IndexBuildResult*)palloc0(sizeof(IndexBuildResult));

    stats->index_tuples += ibshared->indresult.index_tuples;
    stats->heap_tuples += ibshared->indresult.heap_tuples;

    BgworkerListSyncQuit();
    return stats;
}

static IndexBuildResult* index_build_bucketstorage_parallel(Relation heapRelation, Relation indexRelation,
    Partition heapPartition, Partition indexPartition, int parallel_workers)
{
    IndexBucketShared *ibshared;

    /* Store shared build state, for which we reserved space */
    ibshared = (IndexBucketShared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
        sizeof(IndexBucketShared));
    /* Initialize immutable state */
    ibshared->heaprelid  = RelationGetRelid(heapRelation);
    ibshared->indexrelid = RelationGetRelid(indexRelation);
    ibshared->heappartid  = heapPartition ? PartitionGetPartid(heapPartition) : InvalidOid;
    ibshared->indexpartid = indexPartition ? PartitionGetPartid(indexPartition) : InvalidOid;
    ibshared->nparticipants = parallel_workers;
    SpinLockInit(&ibshared->mutex);

    LaunchBackgroundWorkers(parallel_workers, ibshared, index_build_bucketstorage_main, NULL);
    IndexBuildResult* stats = index_build_bucketstorage_end(ibshared);

    return stats;
}

static IndexBuildResult* index_build_bucketstorage(Relation heapRelation, Relation indexRelation,
    Partition heapPartition, Partition indexPartition, IndexInfo* indexInfo)
{
    IndexBuildResult* stats;

    if (RelationAmIsBtree(indexRelation)) {
        /* We do parallel index build for hashbucket table during cluster redistribution */
        int parallel_workers = get_parallel_workers(heapRelation);
        if (parallel_workers > 0) {
            stats = index_build_bucketstorage_parallel(heapRelation,
                indexRelation,
                heapPartition,
                indexPartition,
                parallel_workers);
            if (stats != NULL) {
                return stats;
            }
        }
    }

    oidvector* bucketlist = searchHashBucketByOid(indexRelation->rd_bucketoid);
    stats = (IndexBuildResult*)palloc0(sizeof(IndexBuildResult));
    for (int i = 0; i < bucketlist->dim1; i++) {
        Relation heapBucketRel = bucketGetRelation(heapRelation, heapPartition, bucketlist->values[i]);
        Relation indexBucketRel = bucketGetRelation(indexRelation, indexPartition, bucketlist->values[i]);

        IndexBuildResult* results = index_build_storage(heapBucketRel, indexBucketRel, indexInfo);

        bucketCloseRelation(heapBucketRel);
        bucketCloseRelation(indexBucketRel);

        stats->index_tuples += results->index_tuples;
        stats->heap_tuples += results->heap_tuples;
        pfree_ext(results);
    }
    return stats;
}

void UpdateStatsForGlobalIndex(
    Relation heapRelation, Relation indexRelation, IndexBuildResult* stats, bool isPrimary, Oid cudescIdxOid)
{
    ListCell* partitionCell = NULL;
    Oid partitionOid;
    Partition partition = NULL;
    List* partitionOidList = NIL;
    partitionOidList = relationGetPartitionOidList(heapRelation);
    int partitionIdx = 0;

    foreach (partitionCell, partitionOidList) {
        partitionOid = lfirst_oid(partitionCell);
        partition = partitionOpen(heapRelation, partitionOid, ShareLock);
        if (RelationIsSubPartitioned(heapRelation)) {
            Relation partRel = partitionGetRelation(heapRelation, partition);
            List* subPartitionOidList = relationGetPartitionOidList(partRel);
            ListCell* subPartitionCell = NULL;
            foreach (subPartitionCell, subPartitionOidList) {
                Oid subPartitionOid = lfirst_oid(subPartitionCell);
                Partition subPart = partitionOpen(partRel, subPartitionOid, ShareLock);
                partition_index_update_stats(
                    subPart, true, isPrimary,
                    (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation)
                                                                          : InvalidOid,
                    cudescIdxOid, (stats->all_part_tuples != NULL) ? stats->all_part_tuples[partitionIdx] : 0);
                partitionClose(partRel, subPart, NoLock);
                partitionIdx++;
            }
            releasePartitionOidList(&subPartitionOidList);
            releaseDummyRelation(&partRel);
            partitionClose(heapRelation, partition, ShareLock);
        } else {
            partition_index_update_stats(
                partition, true, isPrimary,
                (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation) : InvalidOid,
                cudescIdxOid, (stats->all_part_tuples != NULL) ? stats->all_part_tuples[partitionIdx] : 0);
            partitionClose(heapRelation, partition, NoLock);
            partitionIdx++;
        }
    }
    releasePartitionOidList(&partitionOidList);
    index_update_stats(heapRelation,
        true,
        isPrimary,
        (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation) : InvalidOid,
        InvalidOid,
        -1);

    index_update_stats(indexRelation, false, false, InvalidOid, InvalidOid, stats->index_tuples);
}

/*
 * index_build - invoke access-method-specific index build procedure
 *
 * On entry, the index's catalog entries are valid, and its physical disk
 * file has been created but is empty.	We call the AM-specific build
 * procedure to fill in the index contents.  We then update the pg_class
 * entries of the index and heap relation as needed, using statistics
 * returned by ambuild as well as data passed by the caller.
 *
 * isPrimary tells whether to mark the index as a primary-key index.
 * isreindex indicates we are recreating a previously-existing index.
 *
 * Note: when reindexing an existing index, isPrimary can be false even if
 * the index is a PK; it's already properly marked and need not be re-marked.
 *
 * Note: before Postgres 8.2, the passed-in heap and index Relations
 * were automatically closed by this routine.  This is no longer the case.
 * The caller opened 'em, and the caller should close 'em.
 */
void index_build(Relation heapRelation, Partition heapPartition, Relation indexRelation, Partition indexPartition,
    IndexInfo* indexInfo, bool isPrimary, bool isreindex, IndexCreatePartitionType partitionType, bool parallel)
{
    double indextuples;
    double heaptuples;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;
    Relation heapPartRel = NULL;
    Relation indexPartRel = NULL;
    Relation targetHeapRelation = heapRelation;
    Relation targetIndexRelation = indexRelation;
    bool hasbucket = false;

    /*
     * sanity checks
     */
    Assert(RelationIsValid(heapRelation));
    Assert(RelationIsValid(indexRelation));
    Assert(PointerIsValid(indexRelation->rd_am));

    Oid parentOid = partid_get_parentid(RelationGetRelid(heapRelation));

    /*
     * Determine worker process details for parallel CREATE INDEX.  Currently,
     * only btree has support for parallel builds.
     *
     * Note that planner considers parallel safety for us.
     */
    if (parallel && IsNormalProcessingMode() && indexRelation->rd_rel->relam == BTREE_AM_OID && !IS_PGXC_COORDINATOR) {
        int parallel_workers = get_parallel_workers(heapRelation);

        /* The check order should not be changed. */
        if (RelationIsCrossBucketIndex(indexRelation)) {
            /* crossbucket index */
            indexInfo->ii_ParallelWorkers = RELATION_OWN_BUCKET(heapRelation) ? parallel_workers : 0;
        } else if (RelationIsGlobalIndex(indexRelation)) {
            /* pure global partitioned index */
            int nparts = 0;
            if (RelationIsSubPartitioned(heapRelation)) {
                nparts = GetSubPartitionNumber(heapRelation);
            } else {
                nparts = getPartitionNumber(heapRelation->partMap);
            }
            indexInfo->ii_ParallelWorkers = Min(parallel_workers, nparts);
        } else if (parentOid != InvalidOid) {
            /* sub-partition */
            indexInfo->ii_ParallelWorkers = 0;
        } else if (!RELATION_OWN_BUCKET(heapRelation)) {
            indexInfo->ii_ParallelWorkers = parallel_workers;
        }

        /* disable parallel building index for system table */
        if (IsCatalogRelation(heapRelation)) {
            indexInfo->ii_ParallelWorkers = 0;
        }
    }

    if (indexInfo->ii_ParallelWorkers == 0) {
        ereport(DEBUG1, (errmsg("building index \"%s\" on table \"%s\" serially",
            RelationGetRelationName(indexRelation), RelationGetRelationName(heapRelation))));
    } else {
        ereport(DEBUG1, (errmsg_plural("building index \"%s\" on table \"%s\" with request for %d parallel worker",
            "building index \"%s\" on table \"%s\" with request for %d parallel workers", indexInfo->ii_ParallelWorkers,
            RelationGetRelationName(indexRelation), RelationGetRelationName(heapRelation),
            indexInfo->ii_ParallelWorkers)));
    }

    if (partitionType == INDEX_CREATE_NONE_PARTITION) {
        Assert(!PointerIsValid(heapPartition));
        Assert(!PointerIsValid(indexPartition));
        Assert(!RELATION_IS_PARTITIONED(heapRelation));
    } else if (partitionType == INDEX_CREATE_GLOBAL_PARTITION) {
        Assert(!PointerIsValid(heapPartition));
        Assert(!PointerIsValid(indexPartition));
        Assert(RELATION_IS_PARTITIONED(heapRelation));
    } else {
        Assert(PointerIsValid(heapPartition));
        Assert(PointerIsValid(indexPartition));
        Assert(RELATION_IS_PARTITIONED(heapRelation));
    }

    if (partitionType == INDEX_CREATE_LOCAL_PARTITION) {
        heapPartRel = partitionGetRelation(heapRelation, heapPartition);
        indexPartRel = partitionGetRelation(indexRelation, indexPartition);
        targetHeapRelation = heapPartRel;
        targetIndexRelation = indexPartRel;
    }

    ereport(DEBUG1,
        (errmsg("building index \"%s\" on table/partition \"%s\"",
            RelationGetRelationName(targetIndexRelation),
            RelationGetRelationName(targetHeapRelation))));

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(targetHeapRelation->rd_rel->relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();

    // If PSort index and is reindex, we need set new relfilenode
    //
    if (isreindex && OidIsValid(targetIndexRelation->rd_rel->relcudescrelid)) {
        Oid psortRelId = targetIndexRelation->rd_rel->relcudescrelid;
        Relation psortRel = relation_open(psortRelId, AccessExclusiveLock);

        RelationSetNewRelfilenode(psortRel, u_sess->utils_cxt.RecentXmin, InvalidMultiXactId);
        relation_close(psortRel, NoLock);
    }

    if (RELATION_IS_GLOBAL_TEMP(indexRelation)) {
        if (indexRelation->rd_smgr == NULL) {
            /* Open it at the smgr level if not already done */
            RelationOpenSmgr(indexRelation);
        }
        if (!gtt_storage_attached(RelationGetRelid(indexRelation))
            || !smgrexists(indexRelation->rd_smgr, MAIN_FORKNUM)) {
            gtt_force_enable_index(indexRelation);
            RelationCreateStorage(indexRelation->rd_node,
                RELPERSISTENCE_GLOBAL_TEMP,
                indexRelation->rd_rel->relowner,
                indexRelation->rd_bucketoid, indexRelation);
        }
    }

    /*
     * Call the access method's build procedure
     */
    hasbucket = (partitionType == INDEX_CREATE_NONE_PARTITION && RELATION_CREATE_BUCKET_COMMON(heapRelation)) ||
                (partitionType != INDEX_CREATE_NONE_PARTITION && RELATION_OWN_BUCKETKEY_COMMON(heapRelation));

    IndexBuildResult* stats = NULL;

    if (hasbucket && !RelationIsCrossBucketIndex(indexRelation)) {
        stats = index_build_bucketstorage(heapRelation,
            indexRelation,
            heapPartition,
            indexPartition,
            indexInfo);
    } else {
        stats = index_build_storage(targetHeapRelation, targetIndexRelation, indexInfo);
    }
    indextuples = stats->index_tuples;
    heaptuples = stats->heap_tuples;

    /*
     * If we found any potentially broken HOT chains, mark the index as not
     * being usable until the current transaction is below the event horizon.
     * See src/backend/access/heap/README.HOT for discussion.
     *
     * However, when reindexing an existing index, we should do nothing here.
     * Any HOT chains that are broken with respect to the index must predate
     * the index's original creation, so there is no need to change the
     * index's usability horizon.  Moreover, we *must not* try to change the
     * index's pg_index entry while reindexing pg_index itself, and this
     * optimization nicely prevents that.
     *
     * We also need not set indcheckxmin during a concurrent index build,
     * because we won't set indisvalid true until all transactions that care
     * about the broken HOT chains are gone.
     *
     * Therefore, this code path can only be taken during non-concurrent
     * CREATE INDEX.  Thus the fact that heap_update will set the pg_index
     * tuple's xmin doesn't matter, because that tuple was created in the
     * current transaction anyway.	That also means we don't need to worry
     * about any concurrent readers of the tuple; no other transaction can see
     * it yet.
     */
    if (indexInfo->ii_BrokenHotChain && (!isreindex || RelationIsUstoreIndex(indexRelation)) &&
        partitionType != INDEX_CREATE_LOCAL_PARTITION && !indexInfo->ii_Concurrent) {
        Oid indexId = RelationGetRelid(indexRelation);
        Relation pg_index;
        HeapTuple indexTuple;
        Form_pg_index indexForm;

        pg_index = heap_open(IndexRelationId, RowExclusiveLock);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexId));
        if (!HeapTupleIsValid(indexTuple))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));
        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        /* If it's a new index, indcheckxmin shouldn't be set ... */
        Assert(RelationIsUstoreIndex(indexRelation) || !indexForm->indcheckxmin);

        indexForm->indcheckxmin = true;
        simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
        CatalogUpdateIndexes(pg_index, indexTuple);

        heap_freetuple(indexTuple);
        heap_close(pg_index, RowExclusiveLock);
    }

    Oid cudescIdxOid = InvalidOid;
    switch (indexInfo->ii_PgClassAttrId) {
        case Anum_pg_class_relcudescidx: {
            cudescIdxOid = RelationGetRelid(indexRelation);
            break;
        }
        default:
            break;
    }

    /*
     * Update heap and index pg_class rows
     */
    if (partitionType == INDEX_CREATE_NONE_PARTITION) {
        index_update_stats(heapRelation,
            true,
            isPrimary,
            (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation) : InvalidOid,
            cudescIdxOid,
            heaptuples);

        index_update_stats(indexRelation, false, false, InvalidOid, InvalidOid, indextuples);
    } else if (partitionType == INDEX_CREATE_LOCAL_PARTITION) {
        /*
         * if the build partition index, the heapRelation is faked from Parent RelationData and PartitionData,
         * so we reopen the Partition , it seems weird.
         */
        partition_index_update_stats(heapPartition,
            true,
            isPrimary,
            (heapRelation->rd_rel->relkind == RELKIND_TOASTVALUE) ? RelationGetRelid(indexRelation) : InvalidOid,
            cudescIdxOid,
            heaptuples);

        partition_index_update_stats(indexPartition, false, false, InvalidOid, cudescIdxOid, indextuples);
    } else if (partitionType == INDEX_CREATE_GLOBAL_PARTITION) {
        UpdateStatsForGlobalIndex(heapRelation, indexRelation, stats, isPrimary, cudescIdxOid);
        pfree_ext(stats->all_part_tuples);
    }
    pfree(stats);

    /* Make the updated catalog row versions visible */
    CommandCounterIncrement();

    /*
     * If it's for an exclusion constraint, make a second pass over the heap
     * to verify that the constraint is satisfied.	We must not do this until
     * the index is fully valid.  (Broken HOT chains shouldn't matter, though;
     * see comments for IndexCheckExclusion.)
     */
    if (indexInfo->ii_ExclusionOps != NULL) {
        if (hasbucket && !RelationIsCrossBucketIndex(indexRelation)) {
            IndexCheckExclusionForBucket(heapRelation, heapPartition, indexRelation, indexPartition, indexInfo);
        } else {
            IndexCheckExclusion(targetHeapRelation, targetIndexRelation, indexInfo);
        }
    }
    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    if (partitionType == INDEX_CREATE_LOCAL_PARTITION) {
        releaseDummyRelation(&heapPartRel);
        releaseDummyRelation(&indexPartRel);
    }
}

static inline bool IsRedisExtraIndex(const IndexInfo* indexInfo, const TupleTableSlot* slot, bool inRedistribute)
{
    return (inRedistribute && indexInfo->ii_NumIndexAttrs == 2 &&
        (indexInfo->ii_KeyAttrNumbers[1] == slot->tts_tupleDescriptor->natts) &&
        (indexInfo->ii_KeyAttrNumbers[0] == (slot->tts_tupleDescriptor->natts - 1)));
}

static TransactionId GetCatalogOldestXmin(Relation heapRelation)
{
    TransactionId oldestXmin = GetOldestXmin(heapRelation);
    if (IsCatalogRelation(heapRelation) || RelationIsAccessibleInLogicalDecoding(heapRelation)) {
        TransactionId catalogXmin = GetReplicationSlotCatalogXmin();
        if (TransactionIdIsNormal(catalogXmin) && TransactionIdPrecedes(catalogXmin, oldestXmin)) {
            oldestXmin = catalogXmin;
        }
    }
    return oldestXmin;
}

/*
 * IndexBuildHeapScan - scan the heap relation to find tuples to be indexed
 *
 * This is called back from an access-method-specific index build procedure
 * after the AM has done whatever setup it needs.  The parent heap relation
 * is scanned to find tuples that should be entered into the index.  Each
 * such tuple is passed to the AM's callback routine, which does the right
 * things to add it to the new index.  After we return, the AM's index
 * build procedure does whatever cleanup it needs.
 *
 * The total count of heap tuples is returned.	This is for updating pg_class
 * statistics.	(It's annoying not to be able to do that here, but we want
 * to merge that update with others; see index_update_stats.)  Note that the
 * index AM itself must keep track of the number of index tuples; we don't do
 * so here because the AM might reject some of the tuples for its own reasons,
 * such as being unable to store NULLs.
 *
 * A side effect is to set indexInfo->ii_BrokenHotChain to true if we detect
 * any potentially broken HOT chains.  Currently, we set this if there are
 * any RECENTLY_DEAD or DELETE_IN_PROGRESS entries in a HOT chain, without
 * trying very hard to detect whether they're really incompatible with the
 * chain tip.
 */
double IndexBuildHeapScan(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo, bool allow_sync,
    IndexBuildCallback callback, void* callbackState, TableScanDesc scan)
{
    bool is_system_catalog = false;
    bool checking_uniqueness = false;
    bool inRedistribution = false;
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    double reltuples;
    List* predicate = NIL;
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    Snapshot snapshot;
    TransactionId OldestXmin;
    BlockNumber root_blkno = InvalidBlockNumber;
    OffsetNumber root_offsets[MaxHeapTuplesPerPage];

    /*
     * sanity checks
     */
    Assert(OidIsValid(indexRelation->rd_rel->relam));

    /* Remember if it's a system catalog */
    is_system_catalog = IsSystemRelation(heapRelation);

    /* See whether we're verifying uniqueness/exclusion properties */
    checking_uniqueness = (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL);

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.	Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);

    /*
     * Prepare for scan of the base relation.  In a normal index build, we use
     * SnapshotAny because we must retrieve all tuples and do our own time
     * qual checks (because we have to index RECENTLY_DEAD tuples). In a
     * concurrent build, we take a regular MVCC snapshot and index whatever's
     * live according to that.	During bootstrap we just use SnapshotNow.
     */

    if (!scan) {
        if (IsBootstrapProcessingMode()) {
            snapshot = SnapshotNow;
            OldestXmin = InvalidTransactionId; /* not used */
        } else if (indexInfo->ii_Concurrent) {
            snapshot = RegisterSnapshot(GetTransactionSnapshot());
            OldestXmin = InvalidTransactionId; /* not used */
        } else {
            snapshot = SnapshotAny;
            /* okay to ignore lazy VACUUMs here */
            OldestXmin = GetCatalogOldestXmin(heapRelation);
        }

        scan = heap_beginscan_strat(heapRelation, /* relation */
            snapshot,                             /* snapshot */
            0,                                    /* number of keys */
            NULL,                                 /* scan key */
            true,                                 /* buffer access strategy OK */
            allow_sync);                          /* syncscan OK? */
    } else {
        /*
         * Parallel index build.
         *
         * Parallel case never registers/unregisters own snapshot.	Snapshot
         * is taken from parallel heap scan, and is SnapshotAny or an MVCC
         * snapshot, based on same criteria as serial case.
         */
        Assert(!IsBootstrapProcessingMode());
        Assert(allow_sync);
        snapshot = SnapshotAny;
        OldestXmin = GetOldestXmin(heapRelation);
    }
    reltuples = 0;

    /*
     * Scan all tuples in the base relation.
     */
    while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        bool tupleIsAlive = false;
        HTSV_Result result = HEAPTUPLE_LIVE;

        CHECK_FOR_INTERRUPTS();

        /* IO collector and IO scheduler for creating index. -- for read */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);

        /*
         * When dealing with a HOT-chain of updated tuples, we want to index
         * the values of the live tuple (if any), but index it under the TID
         * of the chain's root tuple.  This approach is necessary to preserve
         * the HOT-chain structure in the heap. So we need to be able to find
         * the root item offset for every tuple that's in a HOT-chain.  When
         * first reaching a new page of the relation, call
         * heap_get_root_tuples() to build a map of root item offsets on the
         * page.
         *
         * It might look unsafe to use this information across buffer
         * lock/unlock.  However, we hold ShareLock on the table so no
         * ordinary insert/update/delete should occur; and we hold pin on the
         * buffer continuously while visiting the page, so no pruning
         * operation can occur either.
         *
         * Also, although our opinions about tuple liveness could change while
         * we scan the page (due to concurrent transaction commits/aborts),
         * the chain root locations won't, so this info doesn't need to be
         * rebuilt after waiting for another transaction.
         *
         * Note the implied assumption that there is no more than one live
         * tuple per HOT-chain --- else we could create more than one index
         * entry pointing to the same root tuple.
         */
        if (scan->rs_cblock != root_blkno) {
            Page page = BufferGetPage(scan->rs_cbuf);

            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets);
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

            root_blkno = scan->rs_cblock;
        }

        if (snapshot == SnapshotAny) {
            /* do our own time qual check */
            bool indexIt = false;
            TransactionId xwait;

        recheck:

            /*
             * We could possibly get away with not locking the buffer here,
             * since caller should hold ShareLock on the relation, but let's
             * be conservative about it.  (This remark is still correct even
             * with HOT-pruning: our pin on the buffer prevents pruning.)
             */
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

            if (u_sess->attr.attr_storage.enable_debug_vacuum)
                t_thrd.utils_cxt.pRelatedRel = heapRelation;

            if (!u_sess->attr.attr_sql.enable_cluster_resize) {
                result  = HeapTupleSatisfiesVacuum(heapTuple, OldestXmin, scan->rs_cbuf);
            } else {
                Oid redis_ns = get_namespace_oid("data_redis", true);
                if (!OidIsValid(redis_ns) || heapRelation->rd_rel->relnamespace != redis_ns) {
                    result  = HeapTupleSatisfiesVacuum(heapTuple, OldestXmin, scan->rs_cbuf);
                } else if (!inRedistribution) {
                /*
                 * Determine the status of tuples for redistribution.
                 * In redistribution, data in the temporary table is inserted into by 
                 * original table, No delete or update operation is performed. 
                 * All tuples are valid.
                 */
                    ereport(LOG, (errmsg("In redistribution, skip HeapTupleSatisfiesVacuum for table %s",
                        RelationGetRelationName(heapRelation))));
                    inRedistribution = true;
                }
            }
            switch (result) {
                case HEAPTUPLE_DEAD:
                    /* Definitely dead, we can ignore it */
                    indexIt = false;
                    tupleIsAlive = false;
                    break;
                case HEAPTUPLE_LIVE:
                    /* Normal case, index and unique-check it */
                    indexIt = true;
                    tupleIsAlive = true;
                    break;
                case HEAPTUPLE_RECENTLY_DEAD:

                    /*
                     * If tuple is recently deleted then we must index it
                     * anyway to preserve MVCC semantics.  (Pre-existing
                     * transactions could try to use the index after we finish
                     * building it, and may need to see such tuples.)
                     *
                     * However, if it was HOT-updated then we must only index
                     * the live tuple at the end of the HOT-chain.	Since this
                     * breaks semantics for pre-existing snapshots, mark the
                     * index as unusable for them.
                     */
                    if (HeapTupleIsHotUpdated(heapTuple)) {
                        indexIt = false;
                        /* mark the index as unsafe for old snapshots */
                        indexInfo->ii_BrokenHotChain = true;
                    } else
                        indexIt = true;
                    /* In any case, exclude the tuple from unique-checking */
                    tupleIsAlive = false;
                    break;
                case HEAPTUPLE_INSERT_IN_PROGRESS:

                    /*
                     * Since caller should hold ShareLock or better, normally
                     * the only way to see this is if it was inserted earlier
                     * in our own transaction.	However, it can happen in
                     * system catalogs, since we tend to release write lock
                     * before commit there.  Give a warning if neither case
                     * applies.
                     */
                    xwait = HeapTupleGetRawXmin(heapTuple);
                    if (!TransactionIdIsCurrentTransactionId(xwait)) {
                        if (!is_system_catalog)
                            ereport(WARNING,
                                (errmsg("concurrent insert in progress within table \"%s\"",
                                    RelationGetRelationName(heapRelation))));

                        /*
                         * If we are performing uniqueness checks, indexing
                         * such a tuple could lead to a bogus uniqueness
                         * failure.  In that case we wait for the inserting
                         * transaction to finish and check again.
                         */
                        if (checking_uniqueness) {
                            /*
                             * Must drop the lock on the buffer before we wait
                             */
                            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
                            XactLockTableWait(xwait);
                            CHECK_FOR_INTERRUPTS();
                            goto recheck;
                        }
                    }

                    /*
                     * We must index such tuples, since if the index build
                     * commits then they're good.
                     */
                    indexIt = true;
                    tupleIsAlive = true;
                    break;
                case HEAPTUPLE_DELETE_IN_PROGRESS:

                    /*
                     * As with INSERT_IN_PROGRESS case, this is unexpected
                     * unless it's our own deletion or a system catalog.
                     */
                    Assert(!(heapTuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
                    xwait = HeapTupleGetUpdateXid(heapTuple);
                    if (!TransactionIdIsCurrentTransactionId(xwait)) {
                        if (!is_system_catalog)
                            ereport(WARNING,
                                (errmsg("concurrent delete in progress within table \"%s\"",
                                    RelationGetRelationName(heapRelation))));

                        /*
                         * If we are performing uniqueness checks, assuming
                         * the tuple is dead could lead to missing a
                         * uniqueness violation.  In that case we wait for the
                         * deleting transaction to finish and check again.
                         *
                         * Also, if it's a HOT-updated tuple, we should not
                         * index it but rather the live tuple at the end of
                         * the HOT-chain.  However, the deleting transaction
                         * could abort, possibly leaving this tuple as live
                         * after all, in which case it has to be indexed. The
                         * only way to know what to do is to wait for the
                         * deleting transaction to finish and check again.
                         */
                        if (checking_uniqueness || HeapTupleIsHotUpdated(heapTuple)) {
                            /*
                             * Must drop the lock on the buffer before we wait
                             */
                            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
                            XactLockTableWait(xwait);
                            CHECK_FOR_INTERRUPTS();
                            goto recheck;
                        }

                        /*
                         * Otherwise index it but don't check for uniqueness,
                         * the same as a RECENTLY_DEAD tuple.
                         */
                        indexIt = true;
                    } else if (HeapTupleIsHotUpdated(heapTuple)) {
                        /*
                         * It's a HOT-updated tuple deleted by our own xact.
                         * We can assume the deletion will commit (else the
                         * index contents don't matter), so treat the same as
                         * RECENTLY_DEAD HOT-updated tuples.
                         */
                        indexIt = false;
                        /* mark the index as unsafe for old snapshots */
                        indexInfo->ii_BrokenHotChain = true;
                    } else {
                        /*
                         * It's a regular tuple deleted by our own xact. Index
                         * it but don't check for uniqueness, the same as a
                         * RECENTLY_DEAD tuple.
                         */
                        indexIt = true;
                    }
                    /* In any case, exclude the tuple from unique-checking */
                    tupleIsAlive = false;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                    indexIt = tupleIsAlive = false; /* keep compiler quiet */
                    break;
            }

            if (u_sess->attr.attr_storage.enable_debug_vacuum)
                t_thrd.utils_cxt.pRelatedRel = NULL;

            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

            if (!indexIt) {
                continue;
            }
        } else {
            /* heap_getnext did the time qual check */
            tupleIsAlive = true;
        }

        reltuples += 1;

        MemoryContextReset(econtext->ecxt_per_tuple_memory);

        /* Set up for predicate or expression evaluation */
        (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

        /*
         * In a partial index, discard tuples that don't satisfy the
         * predicate.
         */
        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }
        if (IsRedisExtraIndex(indexInfo, slot, inRedistribution)) {
            /*
            * For redistribution's delta index, its location is the last two column of heap tuple.
            */
            FormIndexDatumForRedis(slot, values, isnull);
        } else {
            /*
            * For the current heap tuple, extract all the attributes we use in
            * this index, and note which are null.  This also performs evaluation
            * of any expressions needed.
            */
            FormIndexDatum(indexInfo, slot, estate, values, isnull);
        }
        /*
         * You'd think we should go ahead and build the index tuple here, but
         * some index AMs want to do further processing on the data first.	So
         * pass the values[] and isnull[] arrays, instead.
         */

        if (HeapTupleIsHeapOnly(heapTuple)) {
            /*
             * For a heap-only tuple, pretend its TID is that of the root. See
             * src/backend/access/heap/README.HOT for discussion.
             */
            HeapTupleData rootTuple;
            OffsetNumber offnum;

            rootTuple = *heapTuple;
            offnum = ItemPointerGetOffsetNumber(&heapTuple->t_self);

            Assert(OffsetNumberIsValid(root_offsets[offnum - 1]));

            ItemPointerSetOffsetNumber(&rootTuple.t_self, root_offsets[offnum - 1]);

            /* Call the AM's callback routine to process the tuple */
            callback(indexRelation, &rootTuple, values, isnull, tupleIsAlive, callbackState);
        } else {
            /* Call the AM's callback routine to process the tuple */
            callback(indexRelation, heapTuple, values, isnull, tupleIsAlive, callbackState);
        }
    }

    heap_endscan(scan);

    /* we can now forget our snapshot, if set */
    if (indexInfo->ii_Concurrent)
        UnregisterSnapshot(snapshot);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;

    return reltuples;
}

/*
 * IndexBuildUHeapScan - Similar to IndexBuildHeapScan, but for uheap
 */
double IndexBuildUHeapScan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo, bool allowSync,
    IndexBuildCallback callback, void *callbackState, TableScanDesc scan)
{
    /* system catalog will not use Ustore table */
    Assert(!IsSystemNamespace(RelationGetNamespace(heapRelation)));

    bool checkingUniqueness = false;
    Assert(scan == NULL);
    UHeapScanDesc sscan;
    HeapTupleData heapTuple;
    UHeapTuple uheapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    double reltuples;
    List *predicate = NIL;
    TupleTableSlot *slot = NULL;
    EState *estate = NULL;
    ExprContext *econtext = NULL;
    Snapshot snapshot;
    /*
     * sanity checks
     */
    Assert(OidIsValid(indexRelation->rd_rel->relam));

    /* See whether we're verifying uniqueness/exclusion properties */
    checkingUniqueness = (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL);

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.  Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), false, TAM_USTORE);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = (List *)ExecPrepareExpr((Expr *)indexInfo->ii_Predicate, estate);

    if (indexInfo->ii_Concurrent) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("concurrent index create is not supported in ustore")));
    }
    /*
     * Prepare for scan of the base relation. We always use SNAPSHOT_NOW to
     * build a ustore index, and set the ii_BrokenHotChain flag to mark the
     * index as unusable for old snapshots because we don't index RECENTLY_DEAD
     * tuples. Every index tuple will created with xmin as FrozenTransactionId
     * and xmax as InvalidTransactionId.
     */
    snapshot = SnapshotNow;
    indexInfo->ii_BrokenHotChain = true;

    scan = UHeapBeginScan(heapRelation, snapshot, 0); /* number of keys is 0 */
    sscan = (UHeapScanDesc)scan;
    reltuples = 0;

    /*
     * Scan all tuples in the base relation.
     */
    while ((UHeapIndexBuildGetNextTuple(sscan, slot)) != NULL) {
        uheapTuple = ExecGetUHeapTupleFromSlot(slot);

        CHECK_FOR_INTERRUPTS();

        /* IO collector and IO scheduler for creating index. -- for read */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);

        reltuples += 1;

        MemoryContextReset(econtext->ecxt_per_tuple_memory);

        /*
         * In a partial index, discard tuples that don't satisfy the
         * predicate.
         */
        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }

        /*
         * For the current heap tuple, extract all the attributes we use in
         * this index, and note which are null.  This also performs evaluation
         * of any expressions needed.
         *
         * NOTE: We can't free the uheap tuple fetched by the scan method
         * before next iteration since this tuple is also referenced by
         * scan->rs_cutup. which is used by uheap scan API's to fetch the next
         * tuple. But, for forming and creating the index, we've to store the
         * correct version of the tuple in the slot. Hence, after forming the
         * index and calling the callback function, we restore the uheap tuple
         * fetched by the scan method in the slot.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        /*
         * But, it needs only the tid. So, we set t_self for the uheap tuple
         * and call the AM's callback.
         */
        heapTuple.t_self = uheapTuple->ctid;

        /* Call the AM's callback routine to process the tuple */
        callback(indexRelation, &heapTuple, values, isnull, true, callbackState);
    }

    UHeapEndScan(scan);

    /* we can now forget our snapshot, if set */
    if (indexInfo->ii_Concurrent)
        UnregisterSnapshot(snapshot);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);


    /* These may have been pointing to the now-gone estate */
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;

    return reltuples;
}

double* GetGlobalIndexTuplesForPartition(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo,
    IndexBuildCallback callback, void* callbackState)
{
    ListCell *partitionCell = NULL;
    Oid partitionId;
    Partition partition = NULL;
    List *partitionIdList = NIL;
    Relation heapPartRel = NULL;

    partitionIdList = relationGetPartitionOidList(heapRelation);
    double relTuples;
    int partitionIdx = 0;
    int partNum = partitionIdList->length;
    double *globalIndexTuples = (double *)palloc0(partNum * sizeof(double));
    foreach (partitionCell, partitionIdList) {
        partitionId = lfirst_oid(partitionCell);
        partition = partitionOpen(heapRelation, partitionId, ShareLock);
        heapPartRel = partitionGetRelation(heapRelation, partition);
        if (RelationIsCrossBucketIndex(indexRelation)) {
            relTuples = IndexBuildHeapScanCrossBucket(heapPartRel, indexRelation, indexInfo, callback, callbackState);
        } else {
            relTuples =
                tableam_index_build_scan(heapPartRel, indexRelation, indexInfo, true, callback, callbackState, NULL);
        }
        globalIndexTuples[partitionIdx] = relTuples;
        releaseDummyRelation(&heapPartRel);
        partitionClose(heapRelation, partition, NoLock);
        partitionIdx++;
    }
    releasePartitionOidList(&partitionIdList);
    return globalIndexTuples;
}

List* LockAllGlobalIndexes(Relation relation, LOCKMODE lockmode)
{
    ListCell* cell = NULL;
    List* indexRelList = NIL;
    List* indexIds = NIL;

    indexIds = RelationGetSpecificKindIndexList(relation, true);
    foreach (cell, indexIds) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = index_open(indexOid, lockmode);
        indexRelList = lappend(indexRelList, indexRel);
    }
    list_free_ext(indexIds);
    return indexRelList;
}

void ReleaseLockAllGlobalIndexes(List** indexRelList, LOCKMODE lockmode)
{
    ListCell* cell = NULL;
    foreach (cell, *indexRelList) {
        Relation indexRel = (Relation)lfirst(cell);
        relation_close(indexRel, lockmode);
    }
    list_free_ext(*indexRelList);
}

double* GetGlobalIndexTuplesForSubPartition(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo,
    IndexBuildCallback callback, void* callbackState)
{
    ListCell* partitionCell = NULL;
    ListCell* subPartitionCell = NULL;
    Oid partitionId = InvalidOid;
    Oid subPartitionId = InvalidOid;
    Partition partition = NULL;
    Partition subPartition = NULL;
    List* partitionIdList = NIL;
    List* subPartitionIdList = NIL;
    Relation heapPartRel = NULL;
    Relation heapSubPartRel = NULL;

    partitionIdList = relationGetPartitionOidList(heapRelation);
    double relTuples;
    int subPartitionIdx = 0;
    double* globalIndexTuples = NULL;
    foreach(partitionCell, partitionIdList) {
        partitionId = lfirst_oid(partitionCell);
        partition = partitionOpen(heapRelation, partitionId, ShareLock);
        heapPartRel = partitionGetRelation(heapRelation, partition);
        subPartitionIdList = relationGetPartitionOidList(heapPartRel);
        int subPartNum = subPartitionIdList->length;
        if (globalIndexTuples != NULL) {
            globalIndexTuples = (double*)repalloc(globalIndexTuples, (subPartitionIdx + subPartNum) * sizeof(double));
        } else {
            globalIndexTuples = (double*)palloc0(subPartNum * sizeof(double));
        }
        foreach(subPartitionCell, subPartitionIdList) {
            subPartitionId = lfirst_oid(subPartitionCell);
            subPartition = partitionOpen(heapPartRel, subPartitionId, ShareLock);
            heapSubPartRel = partitionGetRelation(heapPartRel, subPartition);
            relTuples =
                tableam_index_build_scan(heapSubPartRel, indexRelation, indexInfo, true, callback, callbackState, NULL);
            globalIndexTuples[subPartitionIdx] = relTuples;
            subPartitionIdx++;
            releaseDummyRelation(&heapSubPartRel);
            partitionClose(heapPartRel, subPartition, NoLock);
        }
        releaseDummyRelation(&heapPartRel);
        partitionClose(heapRelation, partition, ShareLock);
        releasePartitionOidList(&subPartitionIdList);
    }
    releasePartitionOidList(&partitionIdList);
    return globalIndexTuples;
}

double* GlobalIndexBuildHeapScan(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo,
    IndexBuildCallback callback, void* callbackState)
{
    double* globalIndexTuples = NULL;
    if (RelationIsSubPartitioned(heapRelation)) {
        globalIndexTuples =
            GetGlobalIndexTuplesForSubPartition(heapRelation, indexRelation, indexInfo, callback, callbackState);
    } else {
        globalIndexTuples =
            GetGlobalIndexTuplesForPartition(heapRelation, indexRelation, indexInfo, callback, callbackState);
    }
    return globalIndexTuples;
}

double IndexBuildHeapScanCrossBucket(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    IndexBuildCallback callback, void *callbackState)
{
    double relTuples = 0;
    Relation targetHeapRel = NULL;

    oidvector *bucketlist = searchHashBucketByOid(indexRelation->rd_bucketoid);
    for (int i = 0; i < bucketlist->dim1; i++) {
        targetHeapRel = bucketGetRelation(heapRelation, NULL, bucketlist->values[i]);
        relTuples += tableam_index_build_scan(targetHeapRel, indexRelation, indexInfo, true, callback, callbackState,
            NULL);
        bucketCloseRelation(targetHeapRel);
    }

    return relTuples;
}

double IndexBuildVectorBatchScan(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo,
    VectorBatch* vecScanBatch, Snapshot snapshot, IndexBuildVecBatchScanCallback callback, void* callback_state,
    void* transferFuncs)
{
    int rowIdx;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    Datum h_values[heapRelation->rd_att->natts];
    bool h_isnull[heapRelation->rd_att->natts];
    int rows = vecScanBatch->m_rows;
    ScalarVector* vec = vecScanBatch->m_arr;
    ScalarVector* sysVec = vecScanBatch->GetSysVector(SelfItemPointerAttributeNumber);
    ScalarToDatum* transFuncs = (ScalarToDatum*)transferFuncs;

    List* predicate = NIL;
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    HeapTuple heapTuple;

    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));
    econtext->ecxt_scantuple = slot;
    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);

    List* vars = pull_var_clause((Node*)indexInfo->ii_Expressions, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

    for (rowIdx = 0; rowIdx < rows; rowIdx++) {
        int i;
        ItemPointer tid = (ItemPointer)(&sysVec->m_vals[rowIdx]);

        MemoryContextReset(econtext->ecxt_per_tuple_memory);

        ListCell* lc = list_head(vars);
        for (i = 0; i < heapRelation->rd_att->natts; i++) {
            h_isnull[i] = true;
        }

        for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
            int keycol = indexInfo->ii_KeyAttrNumbers[i];
            if (keycol == 0) {
                Var* var = (Var*)lfirst(lc);
                keycol = (int)var->varattno;
                lc = lnext(lc);
            }

            keycol = keycol - 1;
            if (!vec[keycol].IsNull(rowIdx)) {
                h_isnull[keycol] = false;
                h_values[keycol] = transFuncs[i](vec[keycol].m_vals[rowIdx]);
            }
        }

        heapTuple = heap_form_tuple(RelationGetDescr(heapRelation), h_values, h_isnull);

        (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }

        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        heap_freetuple(heapTuple);
        heapTuple = NULL;

        callback(indexRelation, tid, values, isnull, callback_state);
    }

    list_free(vars);
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;

    return (double)rows;
}

static void IndexCheckExclusionForBucket(Relation heapRelation, Partition heapPartition, Relation indexRelation,
    Partition indexPartition, IndexInfo* indexInfo)
{
    Relation heapBucketRel = NULL;
    Relation indexBucketRel = NULL;

    oidvector* bucketlist = searchHashBucketByOid(indexRelation->rd_bucketoid);

    for (int i = 0; i < bucketlist->dim1; i++) {
        heapBucketRel = bucketGetRelation(heapRelation, heapPartition, bucketlist->values[i]);
        indexBucketRel = bucketGetRelation(indexRelation, indexPartition, bucketlist->values[i]);

        IndexCheckExclusion(heapBucketRel, indexBucketRel, indexInfo);

        bucketCloseRelation(heapBucketRel);
        bucketCloseRelation(indexBucketRel);
    }
}

/*
 * IndexCheckExclusion - verify that a new exclusion constraint is satisfied
 *
 * When creating an exclusion constraint, we first build the index normally
 * and then rescan the heap to check for conflicts.  We assume that we only
 * need to validate tuples that are live according to SnapshotNow, and that
 * these were correctly indexed even in the presence of broken HOT chains.
 * This should be OK since we are holding at least ShareLock on the table,
 * meaning there can be no uncommitted updates from other transactions.
 * (Note: that wouldn't necessarily work for system catalogs, since many
 * operations release write lock early on the system catalogs.)
 */
static void IndexCheckExclusion(Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo)
{
    TableScanDesc scan;
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    List* predicate = NIL;
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;

    /*
     * If we are reindexing the target index, mark it as no longer being
     * reindexed, to forestall an Assert in index_beginscan when we try to use
     * the index for probes.  This is OK because the index is now fully valid.
     */
    if (ReindexIsCurrentlyProcessingIndex(RelationGetRelid(indexRelation)))
        ResetReindexProcessing();

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.	Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);

    /*
     * Scan all live tuples in the base relation.
     */
    scan = heap_beginscan_strat(heapRelation, /* relation */
        SnapshotNow,                          /* snapshot */
        0,                                    /* number of keys */
        NULL,                                 /* scan key */
        true,                                 /* buffer access strategy OK */
        true);                                /* syncscan OK */

    while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        MemoryContextReset(econtext->ecxt_per_tuple_memory);

        /* Set up for predicate or expression evaluation */
        (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

        /*
         * In a partial index, ignore tuples that don't satisfy the predicate.
         */
        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }

        /*
         * Extract index column values, including computing expressions.
         */
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        /*
         * Check that this tuple has no conflicts.
         */
        (void)check_exclusion_constraint(
            heapRelation, indexRelation, indexInfo, &(heapTuple->t_self), values, isnull, estate, true, false);
    }

    heap_endscan(scan);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;
}

/*
 * validate_index - support code for concurrent index builds
 *
 * We do a concurrent index build by first inserting the catalog entry for the
 * index via index_create(), marking it not indisready and not indisvalid.
 * Then we commit our transaction and start a new one, then we wait for all
 * transactions that could have been modifying the table to terminate.	Now
 * we know that any subsequently-started transactions will see the index and
 * honor its constraints on HOT updates; so while existing HOT-chains might
 * be broken with respect to the index, no currently live tuple will have an
 * incompatible HOT update done to it.	We now build the index normally via
 * index_build(), while holding a weak lock that allows concurrent
 * insert/update/delete.  Also, we index only tuples that are valid
 * as of the start of the scan (see IndexBuildHeapScan), whereas a normal
 * build takes care to include recently-dead tuples.  This is OK because
 * we won't mark the index valid until all transactions that might be able
 * to see those tuples are gone.  The reason for doing that is to avoid
 * bogus unique-index failures due to concurrent UPDATEs (we might see
 * different versions of the same row as being valid when we pass over them,
 * if we used HeapTupleSatisfiesVacuum).  This leaves us with an index that
 * does not contain any tuples added to the table while we built the index.
 *
 * Next, we mark the index "indisready" (but still not "indisvalid") and
 * commit the second transaction and start a third.  Again we wait for all
 * transactions that could have been modifying the table to terminate.	Now
 * we know that any subsequently-started transactions will see the index and
 * insert their new tuples into it.  We then take a new reference snapshot
 * which is passed to validate_index().  Any tuples that are valid according
 * to this snap, but are not in the index, must be added to the index.
 * (Any tuples committed live after the snap will be inserted into the
 * index by their originating transaction.	Any tuples committed dead before
 * the snap need not be indexed, because we will wait out all transactions
 * that might care about them before we mark the index valid.)
 *
 * validate_index() works by first gathering all the TIDs currently in the
 * index, using a bulkdelete callback that just stores the TIDs and doesn't
 * ever say "delete it".  (This should be faster than a plain indexscan;
 * also, not all index AMs support full-index indexscan.)  Then we sort the
 * TIDs, and finally scan the table doing a "merge join" against the TID list
 * to see which tuples are missing from the index.	Thus we will ensure that
 * all tuples valid according to the reference snapshot are in the index.
 *
 * Building a unique index this way is tricky: we might try to insert a
 * tuple that is already dead or is in process of being deleted, and we
 * mustn't have a uniqueness failure against an updated version of the same
 * row.  We could try to check the tuple to see if it's already dead and tell
 * index_insert() not to do the uniqueness check, but that still leaves us
 * with a race condition against an in-progress update.  To handle that,
 * we expect the index AM to recheck liveness of the to-be-inserted tuple
 * before it declares a uniqueness error.
 *
 * After completing validate_index(), we wait until all transactions that
 * were alive at the time of the reference snapshot are gone; this is
 * necessary to be sure there are none left with a transaction snapshot
 * older than the reference (and hence possibly able to see tuples we did
 * not index).	Then we mark the index "indisvalid" and commit.  Subsequent
 * transactions will be able to use it for queries.
 *
 * Doing two full table scans is a brute-force strategy.  We could try to be
 * cleverer, eg storing new tuples in a special area of the table (perhaps
 * making the table append-only by setting use_fsm).  However that would
 * add yet more locking issues.
 */
void validate_index(Oid heapId, Oid indexId, Snapshot snapshot)
{
    Relation heapRelation, indexRelation;
    IndexInfo* indexInfo = NULL;
    IndexVacuumInfo ivinfo;
    v_i_state state;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;

    /* Open and lock the parent heap relation */
    heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
    /* And the target index relation */
    indexRelation = index_open(indexId, RowExclusiveLock);

    /*
     * Fetch info needed for index_insert.	(You might think this should be
     * passed in from DefineIndex, but its copy is long gone due to having
     * been built in a previous transaction.)
     */
    indexInfo = BuildIndexInfo(indexRelation);

    /* mark build is concurrent just for consistency */
    indexInfo->ii_Concurrent = true;

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(heapRelation->rd_rel->relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();

    /*
     * Scan the index and gather up all the TIDs into a tuplesort object.
     */
    ivinfo.index = indexRelation;
    ivinfo.analyze_only = false;
    ivinfo.estimated_count = true;
    ivinfo.message_level = DEBUG2;
    ivinfo.num_heap_tuples = heapRelation->rd_rel->reltuples;
    ivinfo.strategy = NULL;
    ivinfo.invisibleParts = NULL;

    state.tuplesort = tuplesort_begin_datum(
        TIDOID, TIDLessOperator, InvalidOid, false, u_sess->attr.attr_memory.maintenance_work_mem, false);
    state.htups = state.itups = state.tups_inserted = 0;

    (void)index_bulk_delete(&ivinfo, NULL, validate_index_callback, (void*)&state);

    /* Execute the sort */
    tuplesort_performsort(state.tuplesort);

    /*
     * Now scan the heap and "merge" it with the index
     */
    tableam_index_validate_scan(heapRelation, indexRelation, indexInfo, snapshot, &state);

    /* Done with tuplesort object */
    tuplesort_end(state.tuplesort);

    ereport(DEBUG2,
        (errmsg("validate_index found %.0f heap tuples, %.0f index tuples; inserted %.0f missing tuples",
            state.htups,
            state.itups,
            state.tups_inserted)));

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Close rels, but keep locks */
    index_close(indexRelation, NoLock);
    heap_close(heapRelation, NoLock);
}

/*
 * validate_index_callback - bulkdelete callback to collect the index TIDs
 */
static bool validate_index_callback(ItemPointer itemptr, void* opaque, Oid partOid, int2 bktId)
{
    v_i_state* state = (v_i_state*)opaque;

    tuplesort_putdatum(state->tuplesort, PointerGetDatum(itemptr), false);
    state->itups += 1;
    return false; /* never actually delete anything */
}

/*
 * validate_index_heapscan - second table scan for concurrent index build
 *
 * This has much code in common with IndexBuildHeapScan, but it's enough
 * different that it seems cleaner to have two routines not one.
 */
void validate_index_heapscan(
    Relation heapRelation, Relation indexRelation, IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state)
{
    TableScanDesc scan;
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    List* predicate = NIL;
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    BlockNumber root_blkno = InvalidBlockNumber;
    OffsetNumber root_offsets[MaxHeapTuplesPerPage];
    bool in_index[MaxHeapTuplesPerPage];

    /* state variables for the merge */
    ItemPointer indexcursor = NULL;
    bool tuplesort_empty = false;

    /*
     * sanity checks
     */
    Assert(OidIsValid(indexRelation->rd_rel->relam));

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.	Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);

    /*
     * Prepare for scan of the base relation.  We need just those tuples
     * satisfying the passed-in reference snapshot.  We must disable syncscan
     * here, because it's critical that we read from block zero forward to
     * match the sorted TIDs.
     */
    scan = heap_beginscan_strat(heapRelation, /* relation */
        snapshot,                             /* snapshot */
        0,                                    /* number of keys */
        NULL,                                 /* scan key */
        true,                                 /* buffer access strategy OK */
        false);                               /* syncscan not OK */

    /*
     * Scan all tuples matching the snapshot.
     */
    while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        ItemPointer heapcursor = &heapTuple->t_self;
        ItemPointerData rootTuple;
        OffsetNumber root_offnum;

        CHECK_FOR_INTERRUPTS();

        state->htups += 1;

        /*
         * As commented in IndexBuildHeapScan, we should index heap-only
         * tuples under the TIDs of their root tuples; so when we advance onto
         * a new heap page, build a map of root item offsets on the page.
         *
         * This complicates merging against the tuplesort output: we will
         * visit the live tuples in order by their offsets, but the root
         * offsets that we need to compare against the index contents might be
         * ordered differently.  So we might have to "look back" within the
         * tuplesort output, but only within the current page.	We handle that
         * by keeping a bool array in_index[] showing all the
         * already-passed-over tuplesort output TIDs of the current page. We
         * clear that array here, when advancing onto a new heap page.
         */
        if (scan->rs_cblock != root_blkno) {
            Page page = BufferGetPage(scan->rs_cbuf);

            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets);
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

            errno_t rc = memset_s(in_index, sizeof(in_index), 0, sizeof(in_index));
            securec_check(rc, "\0", "\0");

            root_blkno = scan->rs_cblock;
        }

        /* Convert actual tuple TID to root TID */
        rootTuple = *heapcursor;
        root_offnum = ItemPointerGetOffsetNumber(heapcursor);

        if (HeapTupleIsHeapOnly(heapTuple)) {
            root_offnum = root_offsets[root_offnum - 1];
            Assert(OffsetNumberIsValid(root_offnum));
            ItemPointerSetOffsetNumber(&rootTuple, root_offnum);
        }

        /*
         * "merge" by skipping through the index tuples until we find or pass
         * the current root tuple.
         */
        while (!tuplesort_empty && (!indexcursor || ItemPointerCompare(indexcursor, &rootTuple) < 0)) {
            Datum ts_val;
            bool ts_isnull = false;

            if (indexcursor) {
                /*
                 * Remember index items seen earlier on the current heap page
                 */
                if (ItemPointerGetBlockNumber(indexcursor) == root_blkno)
                    in_index[ItemPointerGetOffsetNumber(indexcursor) - 1] = true;
                pfree(indexcursor);
                indexcursor = NULL;
            }

            tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true, &ts_val, &ts_isnull);
            Assert(tuplesort_empty || !ts_isnull);
            indexcursor = (ItemPointer)DatumGetPointer(ts_val);
        }

        /*
         * If the tuplesort has overshot *and* we didn't see a match earlier,
         * then this tuple is missing from the index, so insert it.
         */
        if ((tuplesort_empty || ItemPointerCompare(indexcursor, &rootTuple) > 0) && !in_index[root_offnum - 1]) {
            MemoryContextReset(econtext->ecxt_per_tuple_memory);

            /* Set up for predicate or expression evaluation */
            (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

            /*
             * In a partial index, discard tuples that don't satisfy the
             * predicate.
             */
            if (predicate != NIL) {
                if (!ExecQual(predicate, econtext, false)) {
                    continue;
                }
            }

            /*
             * For the current heap tuple, extract all the attributes we use
             * in this index, and note which are null.	This also performs
             * evaluation of any expressions needed.
             */
            FormIndexDatum(indexInfo, slot, estate, values, isnull);

            /*
             * You'd think we should go ahead and build the index tuple here,
             * but some index AMs want to do further processing on the data
             * first. So pass the values[] and isnull[] arrays, instead.
             */

            /*
             * If the tuple is already committed dead, you might think we
             * could suppress uniqueness checking, but this is no longer true
             * in the presence of HOT, because the insert is actually a proxy
             * for a uniqueness check on the whole HOT-chain.  That is, the
             * tuple we have here could be dead because it was already
             * HOT-updated, and if so the updating transaction will not have
             * thought it should insert index entries.	The index AM will
             * check the whole HOT-chain and correctly detect a conflict if
             * there is one.
             */

            (void)index_insert(indexRelation,
                values,
                isnull,
                &rootTuple,
                heapRelation,
                indexInfo->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);

            state->tups_inserted += 1;
        }
    }

    heap_endscan(scan);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;
}

/*
 * index_set_state_flags - adjust pg_index state flags
 *
 * This is used during CREATE/DROP INDEX CONCURRENTLY to adjust the pg_index
 * flags that denote the index's state.  We must use an in-place update of
 * the pg_index tuple, because we do not have exclusive lock on the parent
 * table and so other sessions might concurrently be doing SnapshotNow scans
 * of pg_index to identify the table's indexes.  A transactional update would
 * risk somebody not seeing the index at all.  Because the update is not
 * transactional and will not roll back on error, this must only be used as
 * the last step in a transaction that has not made any transactional catalog
 * updates!
 *
 * Note that heap_u_update does send a cache inval message for the
 * tuple, so other sessions will hear about the update as soon as we commit.
 */
void index_set_state_flags(Oid indexId, IndexStateFlagsAction action)
{
    Relation pg_index;
    HeapTuple indexTuple;
    Form_pg_index indexForm;

    /* Assert that current xact hasn't done any transactional updates */
    Assert(GetTopTransactionIdIfAny() == InvalidTransactionId);

    /* Open pg_index and fetch a writable copy of the index's tuple */
    pg_index = heap_open(IndexRelationId, RowExclusiveLock);

    indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexId));
    if (!HeapTupleIsValid(indexTuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));
    indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

    /* Perform the requested state change on the copy */
    switch (action) {
        case INDEX_CREATE_SET_READY:
            /* Set indisready during a CREATE INDEX CONCURRENTLY sequence */
            Assert(!indexForm->indisready);
            Assert(!indexForm->indisvalid);
            indexForm->indisready = true;
            break;
        case INDEX_CREATE_SET_VALID:
            /* Set indisvalid during a CREATE INDEX CONCURRENTLY sequence */
            Assert(indexForm->indisready);
            indexForm->indisvalid = true;
            break;
        case INDEX_DROP_CLEAR_VALID:

            /*
             * Clear indisvalid during a DROP INDEX CONCURRENTLY sequence
             *
             * If indisready == true we leave it set so the index still gets
             * maintained by active transactions.  We only need to ensure that
             * indisvalid is false.  (We don't assert that either is initially
             * true, though, since we want to be able to retry a DROP INDEX
             * CONCURRENTLY that failed partway through.)
             *
             * Note: the CLUSTER logic assumes that indisclustered cannot be
             * set on any invalid index, so clear that flag too.
             */
            indexForm->indisvalid = false;
            indexForm->indisclustered = false;
            break;
        case INDEX_DROP_SET_DEAD:

            /*
             * Clear indisready during DROP INDEX CONCURRENTLY
             *
             * We clear indisready and set indisvalid, because we not only
             * want to stop updates, we want to prevent sessions from touching
             * the index at all.  See README.HOT.
             */
            Assert(!indexForm->indisvalid);
            indexForm->indisready = false;
            indexForm->indisvalid = true;
            break;
        default:
            break;
    }

    /* ... and write it back in-place */
    heap_inplace_update(pg_index, indexTuple);

    heap_close(pg_index, RowExclusiveLock);
}

/*
 * IndexGetRelation: given an index's relation OID, get the OID of the
 * relation it is an index on.	Uses the system cache.
 */
Oid IndexGetRelation(Oid indexId, bool missing_ok)
{
    HeapTuple tuple;
    Form_pg_index index;
    Oid result;

    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok)
            return InvalidOid;
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));
    }
    index = (Form_pg_index)GETSTRUCT(tuple);
    Assert(index->indexrelid == indexId);

    result = index->indrelid;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void reindex_indexpart_internal(Relation heapRelation, Relation iRel, IndexInfo* indexInfo, Oid indexPartId,
    void* baseDesc)
{
    Oid heapPartId = InvalidOid;
    Partition heapPart = NULL;
    Partition indexpart = NULL;
    HeapTuple indexPartTup = NULL;
    Form_pg_partition indexPartForm = NULL;
    Relation sys_table = relation_open(PartitionRelationId, RowExclusiveLock);

    indexPartTup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(indexPartId));
    if (!HeapTupleIsValid(indexPartTup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", indexPartId)));
    }

    indexPartForm = (Form_pg_partition)GETSTRUCT(indexPartTup);
    heapPartId = indexPartForm->indextblid;
    // remember to release catcache tuple
    ReleaseSysCache(indexPartTup);

    // step 1: rebuild index partition
    heapPart = partitionOpen(heapRelation, heapPartId, ShareLock);
    indexpart = partitionOpen(iRel, indexPartId, AccessExclusiveLock);

    if (baseDesc) {
        /* We'll put the old relfilenode to recycle bin. */
        TrPartitionSetNewRelfilenode(iRel, indexpart, InvalidTransactionId, baseDesc);
    } else {
        /* We'll build a new physical relation for the index */
        PartitionSetNewRelfilenode(iRel, indexpart, InvalidTransactionId, InvalidMultiXactId);
    }

    index_build(heapRelation, heapPart, iRel, indexpart, indexInfo, false, true, INDEX_CREATE_LOCAL_PARTITION, true);

    /*the whole partitioned index has brokenUndoChain if any one partition has brokenUndoChain */
    partitionClose(iRel, indexpart, NoLock);
    partitionClose(heapRelation, heapPart, NoLock);
    relation_close(sys_table, RowExclusiveLock);

    // step 2: reset indisusable state of index partition
    ATExecSetIndexUsableState(PartitionRelationId, indexPartId, true);
}

/*
 * ReindexGlobalIndexInternal - This routine is used to recreate a single global index
 */
void ReindexGlobalIndexInternal(Relation heapRelation, Relation iRel, IndexInfo* indexInfo, void* baseDesc)
{
    List* partitionList = NULL;
    /* We'll open any partition of relation by partition OID and lock it */
    if (RelationIsSubPartitioned(heapRelation)) {
        partitionList = RelationGetSubPartitionList(heapRelation, ShareLock);
    } else {
        partitionList = relationGetPartitionList(heapRelation, ShareLock);
    }

    if (baseDesc) {
        /* We'll put the old relfilenode to recycle bin. */
        TrRelationSetNewRelfilenode(iRel, InvalidTransactionId, baseDesc);
    } else {
        /* We'll build a new physical relation for the index */
        RelationSetNewRelfilenode(iRel, InvalidTransactionId, InvalidMultiXactId);
    }

    /* Initialize the index and rebuild */
    /* Note: we do not need to re-establish pkey setting */
    index_build(heapRelation, NULL, iRel, NULL, indexInfo, false, true, INDEX_CREATE_GLOBAL_PARTITION, true);

    releasePartitionList(heapRelation, &partitionList, NoLock);

    // call the internal function, update pg_index system table
    ATExecSetIndexUsableState(IndexRelationId, iRel->rd_id, true);
}

#ifdef ENABLE_MOT
static void MotFdwReindex(Relation heapRelation, Relation iRel)
{
    /* Forward reindex stmt to MOT FDW. */
    if (RelationIsForeignTable(heapRelation) && isMOTFromTblOid(RelationGetRelid(heapRelation))) {
        FdwRoutine* fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(heapRelation));
        if (fdwroutine->ValidateTableDef != NULL) {
            ReindexForeignStmt stmt;
            stmt.type = T_ReindexStmt;
            stmt.relkind = RELKIND_INDEX;
            stmt.reloid = RelationGetRelid(heapRelation);
            stmt.indexoid = RelationGetRelid(iRel);
            stmt.name = iRel->rd_rel->relname.data;

            fdwroutine->ValidateTableDef((Node*)&stmt);
        }
    }
}
#endif

void PartRelSetHasIndexState(Oid relid)
{
    Relation pg_class = heap_open(RelationRelationId, RowExclusiveLock);
    HeapTuple tup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", relid)));
    }
    Form_pg_class pgcform = (Form_pg_class)GETSTRUCT(tup);

    /* modify pg_class hasindex is true when reindex and it is false. */
    if (!pgcform->relhasindex) {
        pgcform->relhasindex = true;
        heap_inplace_update(pg_class, tup);
    }
    heap_freetuple(tup);
    heap_close(pg_class, RowExclusiveLock);
}

/*
 * reindex_index - This routine is used to recreate a single index
 */
void reindex_index(Oid indexId, Oid indexPartId, bool skip_constraint_checks,
                   AdaptMem *memInfo, bool dbWide, void *baseDesc)
{
    Relation iRel, heapRelation;
    Oid heapId;
    LOCKMODE indexLockMode, heapLockMode;
    IndexInfo* indexInfo = NULL;
    volatile bool skipped_constraint = false;

    // determine the lock mode
    if (OidIsValid(indexPartId)) {
        indexLockMode = AccessShareLock;
        heapLockMode = AccessShareLock;
    } else {
        indexLockMode = AccessExclusiveLock;
        heapLockMode = ShareLock;
    }

    /*
     * Open and lock the parent heap relation.	ShareLock is sufficient since
     * we only need to be sure no schema or data changes are going on.
     */
    heapId = IndexGetRelation(indexId, false);
    heapRelation = heap_open(heapId, heapLockMode);

    /*
     * Open the target index relation and get an exclusive lock on it, to
     * ensure that no one else is touching this particular index.
     */
    iRel = index_open(indexId, indexLockMode);

    /*
     * Don't allow reindex on temp tables of other backends ... their local
     * buffer manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(iRel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot reindex temporary tables of other sessions")));

#ifdef ENABLE_MOT
    /* Forward reindex stmt to MOT FDW. */
    MotFdwReindex(heapRelation, iRel);
#endif

    /*
     * Also check for active uses of the index in the current transaction; we
     * don't want to reindex underneath an open indexscan.
     */
    CheckTableNotInUse(iRel, "REINDEX INDEX");

    /*
     * All predicate locks on the index are about to be made invalid. Promote
     * them to relation locks on the heap.
     */
    TransferPredicateLocksToHeapRelation(iRel);

    PG_TRY();
    {
        /* Suppress use of the target index while rebuilding it */
        SetReindexProcessing(heapId, indexId);

        /* Fetch info needed for index_build */
        indexInfo = BuildIndexInfo(iRel);

        /* If requested, skip checking uniqueness/exclusion constraints */
        if (skip_constraint_checks) {
            if (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL)
                skipped_constraint = true;
            indexInfo->ii_Unique = false;
            indexInfo->ii_ExclusionOps = NULL;
            indexInfo->ii_ExclusionProcs = NULL;
            indexInfo->ii_ExclusionStrats = NULL;
        }

        /* workload client manager */
        if (IS_PGXC_COORDINATOR && ENABLE_WORKLOAD_CONTROL) {
            /* if operatorMem is already set, the mem check is already done */
            if (memInfo != NULL && memInfo->work_mem == 0) {
                EstIdxMemInfo(heapRelation, NULL, &indexInfo->ii_desc, indexInfo, iRel->rd_am->amname.data);
                if (dbWide) {
                    indexInfo->ii_desc.cost = g_instance.cost_cxt.disable_cost;
                    indexInfo->ii_desc.query_mem[0] = Max(STATEMENT_MIN_MEM * 1024, indexInfo->ii_desc.query_mem[0]);
                }
                WLMInitQueryPlan((QueryDesc*)&indexInfo->ii_desc, false);
                dywlm_client_manager((QueryDesc*)&indexInfo->ii_desc, false);
                AdjustIdxMemInfo(memInfo, &indexInfo->ii_desc);
            }
        } else if (IS_PGXC_DATANODE && memInfo != NULL && memInfo->work_mem > 0) {
            indexInfo->ii_desc.query_mem[0] = memInfo->work_mem;
            indexInfo->ii_desc.query_mem[1] = memInfo->max_mem;
        }

        if (!RELATION_IS_PARTITIONED(heapRelation)) /* for non partitioned table */
        {
            if (RELATION_IS_GLOBAL_TEMP(heapRelation) && !gtt_storage_attached(RelationGetRelid(heapRelation))) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot reindex when the global temporary table is not in use")));
            }

            /*
             * I move it here to avoid useless invoking.
             */
            if (baseDesc) {
                /* We'll put the old relfilenode to recycle bin. */
                TrRelationSetNewRelfilenode(iRel, InvalidTransactionId, baseDesc);
            } else {
                /* We'll build a new physical relation for the index */
                RelationSetNewRelfilenode(iRel, InvalidTransactionId, InvalidMultiXactId);
            }

            /* Initialize the index and rebuild */
            /* Note: we do not need to re-establish pkey setting */
            index_build(heapRelation, NULL, iRel, NULL, indexInfo, false, true, INDEX_CREATE_NONE_PARTITION, true);

            // call the internal function, update pg_index system table
            ATExecSetIndexUsableState(IndexRelationId, iRel->rd_id, true);
        } else /* for partitioned table */
        {
            if (OidIsValid(indexPartId)) {
                reindex_indexpart_internal(heapRelation, iRel, indexInfo, indexPartId, baseDesc);
            } else if (RelationIsGlobalIndex(iRel)) {
                ReindexGlobalIndexInternal(heapRelation, iRel, indexInfo, baseDesc);
            } else {
                List* indexPartOidList = NULL;
                ListCell* partCell = NULL;

                indexPartOidList = indexGetPartitionOidList(iRel);
                foreach (partCell, indexPartOidList) {
                    Oid indexPartOid = lfirst_oid(partCell);
                    reindex_indexpart_internal(heapRelation, iRel, indexInfo, indexPartOid, baseDesc);
                }
                releasePartitionOidList(&indexPartOidList);

                PartRelSetHasIndexState(heapRelation->rd_id);
                // call the internal function, update pg_index system table
                ATExecSetIndexUsableState(IndexRelationId, iRel->rd_id, true);
                CacheInvalidateRelcache(heapRelation);
            }
        }
    }
    PG_CATCH();
    {
        /* Make sure flag gets cleared on error exit */
        ResetReindexProcessing();
        PG_RE_THROW();
    }
    PG_END_TRY();
    ResetReindexProcessing();

    /*
     * If the index is marked invalid/not-ready/dead (ie, it's from a failed
     * CREATE INDEX CONCURRENTLY, or a DROP INDEX CONCURRENTLY failed midway),
     * and we didn't skip a uniqueness check, we can now mark it valid.  This
     * allows REINDEX to be used to clean up in such cases.
     *
     * We can also reset indcheckxmin, because we have now done a
     * non-concurrent index build, *except* in the case where index_build
     * found some still-broken HOT chains. If it did, and we don't have to
     * change any of the other flags, we just leave indcheckxmin alone (note
     * that index_build won't have changed it, because this is a reindex).
     * This is okay and desirable because not updating the tuple leaves the
     * index's usability horizon (recorded as the tuple's xmin value) the same
     * as it was.
     *
     * But, if the index was invalid/not-ready/dead and there were broken HOT
     * chains, we had better force indcheckxmin true, because the normal
     * argument that the HOT chains couldn't conflict with the index is
     * suspect for an invalid index.  (A conflict is definitely possible if
     * the index was dead.	It probably shouldn't happen otherwise, but let's
     * be conservative.)  In this case advancing the usability horizon is
     * appropriate.
     *
     * Note that if we have to update the tuple, there is a risk of concurrent
     * transactions not seeing it during their SnapshotNow scans of pg_index.
     * While not especially desirable, this is safe because no such
     * transaction could be trying to update the table (since we have
     * ShareLock on it).  The worst case is that someone might transiently
     * fail to use the index for a query --- but it was probably unusable
     * before anyway, if we are updating the tuple.
     *
     * Another reason for avoiding unnecessary updates here is that while
     * reindexing pg_index itself, we must not try to update tuples in it.
     * pg_index's indexes should always have these flags in their clean state,
     * so that won't happen.
     */
    if (!skipped_constraint) {
        Relation pg_index;
        HeapTuple indexTuple;
        Form_pg_index indexForm;
        bool index_bad = false;

        pg_index = heap_open(IndexRelationId, RowExclusiveLock);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexId));
        if (!HeapTupleIsValid(indexTuple))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));
        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        index_bad = (!indexForm->indisvalid || !indexForm->indisready);
        if (index_bad || (indexForm->indcheckxmin && !indexInfo->ii_BrokenHotChain)) {
            if (!indexInfo->ii_BrokenHotChain)
                indexForm->indcheckxmin = false;
            else if (index_bad)
                indexForm->indcheckxmin = true;
            indexForm->indisvalid = true;
            indexForm->indisready = true;
            simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
            CatalogUpdateIndexes(pg_index, indexTuple);

            /*
             * Invalidate the relcache for the table, so that after we commit
             * all sessions will refresh the table's index list.  This ensures
             * that if anyone misses seeing the pg_index row during this
             * update, they'll refresh their list before attempting any update
             * on the table.
             */
            CacheInvalidateRelcache(heapRelation);
        }

        heap_close(pg_index, RowExclusiveLock);
    }

    /* Recode time of reindex index. */
    if (indexLockMode == AccessExclusiveLock) {
        UpdatePgObjectMtime(indexId, OBJECT_TYPE_INDEX);
        CacheInvalidateRelcache(heapRelation);
    }

    if (RelationIsCrossBucketIndex(iRel) && IndexEnableWaitCleanCbi(iRel)) {
        cbi_set_enable_clean(iRel);
    }

    /* Close rels, but keep locks */
    index_close(iRel, NoLock);
    heap_close(heapRelation, NoLock);
}

/*
 * ReindexRelation - This routine is used to recreate all indexes
 * of a relation (and optionally its toast relation too, if any).
 *
 * "flags" is a bitmask that can include any combination of these bits:
 *
 * REINDEX_REL_PROCESS_TOAST: if true, process the toast table too (if any).
 *
 * REINDEX_REL_SUPPRESS_INDEX_USE: if true, the relation was just completely
 * rebuilt by an operation such as VACUUM FULL or CLUSTER, and therefore its
 * indexes are inconsistent with it.  This makes things tricky if the relation
 * is a system catalog that we might consult during the reindexing.  To deal
 * with that case, we mark all of the indexes as pending rebuild so that they
 * won't be trusted until rebuilt.  The caller is required to call us *without*
 * having made the rebuilt table visible by doing CommandCounterIncrement;
 * we'll do CCI after having collected the index list.  (This way we can still
 * use catalog indexes while collecting the list.)
 *
 * REINDEX_REL_CHECK_CONSTRAINTS: if true, recheck unique and exclusion
 * constraint conditions, else don't.  To avoid deadlocks, VACUUM FULL or
 * CLUSTER on a system catalog must omit this flag.  REINDEX should be used to
 * rebuild an index if constraint inconsistency is suspected.  For optimal
 * performance, other callers should include the flag only after transforming
 * the data in a manner that risks a change in constraint validity.
 *
 * Returns true if any indexes were rebuilt (including toast table's index
 * when relevant).	Note that a CommandCounterIncrement will occur after each
 * index rebuild.
 */
bool ReindexRelation(Oid relid, int flags, int reindexType, void *baseDesc, AdaptMem *memInfo,
    bool dbWide, IndexKind indexKind)
{
    Relation rel;
    Oid toast_relid;
    List* indexIds = NIL;
    bool is_pg_class = false;
    bool result = false;
    bool isPartitioned = false;

    /*
     * Open and lock the relation.	ShareLock is sufficient since we only need
     * to prevent schema and data changes in it.  The lock level used here
     * should match ReindexTable().
     */
    rel = heap_open(relid, ShareLock);

    toast_relid = rel->rd_rel->reltoastrelid;

    isPartitioned = RELATION_IS_PARTITIONED(rel);

    /*
     * Get the list of index OIDs for this relation.  (We trust to the
     * relcache to get this with a sequential scan if ignoring system
     * indexes.)
     */
    if (indexKind == ALL_KIND) {
        indexIds = RelationGetIndexList(rel);
    } else if (indexKind == GLOBAL_INDEX) {
        indexIds = RelationGetSpecificKindIndexList(rel, true);
    } else {
        indexIds = RelationGetSpecificKindIndexList(rel, false);
    }


    /*
     * reindex_index will attempt to update the pg_class rows for the relation
     * and index.  If we are processing pg_class itself, we want to make sure
     * that the updates do not try to insert index entries into indexes we
     * have not processed yet.	(When we are trying to recover from corrupted
     * indexes, that could easily cause a crash.) We can accomplish this
     * because CatalogUpdateIndexes will use the relcache's index list to know
     * which indexes to update. We just force the index list to be only the
     * stuff we've processed.
     *
     * It is okay to not insert entries into the indexes we have not processed
     * yet because all of this is transaction-safe.  If we fail partway
     * through, the updated rows are dead and it doesn't matter whether they
     * have index entries.	Also, a new pg_class index will be created with a
     * correct entry for its own pg_class row because we do
     * RelationSetNewRelfilenode() before we do index_build().
     *
     * Note that we also clear pg_class's rd_oidindex until the loop is done,
     * so that that index can't be accessed either.  This means we cannot
     * safely generate new relation OIDs while in the loop; shouldn't be a
     * problem.
     */
    is_pg_class = (RelationGetRelid(rel) == RelationRelationId);

    /* Ensure rd_indexattr is valid; see comments for RelationSetIndexList */
    if (is_pg_class)
        (void)RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_ALL);

    PG_TRY();
    {
        List* doneIndexes = NIL;
        ListCell* indexId = NULL;

        if (((uint32)flags) & REINDEX_REL_SUPPRESS_INDEX_USE) {
            /* Suppress use of all the indexes until they are rebuilt */
            SetReindexPending(indexIds);

            /*
             * Make the new heap contents visible --- now things might be
             * inconsistent!
             */
            CommandCounterIncrement();
        }

        /* Reindex all the indexes. */
        doneIndexes = NIL;
        foreach (indexId, indexIds) {
            Oid indexOid = lfirst_oid(indexId);
            Relation indexRel = index_open(indexOid, AccessShareLock);

            if (is_pg_class) {
                RelationSetIndexList(rel, doneIndexes, InvalidOid);
            }

            if ((((uint32)reindexType) & REINDEX_ALL_INDEX) ||
                ((((uint32)reindexType) & REINDEX_BTREE_INDEX) && (indexRel->rd_rel->relam == BTREE_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_HASH_INDEX) && (indexRel->rd_rel->relam == HASH_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_GIN_INDEX) && (indexRel->rd_rel->relam == GIN_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_GIN_INDEX) && (indexRel->rd_rel->relam == CGIN_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_GIST_INDEX) && (indexRel->rd_rel->relam == GIST_AM_OID))) {
                index_close(indexRel, AccessShareLock);
                reindex_index(indexOid, InvalidOid, !((static_cast<uint32>(flags)) & REINDEX_REL_CHECK_CONSTRAINTS),
                    memInfo, dbWide, baseDesc);

#ifndef ENABLE_MULTIPLE_NODES
                if (RelationIsCUFormat(rel) && indexRel->rd_index != NULL && indexRel->rd_index->indisunique) {
                    ReindexDeltaIndex(indexOid, InvalidOid);
                }
#endif

                CommandCounterIncrement();
            } else {
                index_close(indexRel, AccessShareLock);
            }

            /* Index should no longer be in the pending list */
            Assert(!ReindexIsProcessingIndex(indexOid));

            if (is_pg_class) {
                doneIndexes = lappend_oid(doneIndexes, indexOid);
            }
        }
    }
    PG_CATCH();
    {
        /* Make sure list gets cleared on error exit */
        ResetReindexPending();
        PG_RE_THROW();
    }
    PG_END_TRY();
    ResetReindexPending();

    if (is_pg_class) {
        RelationSetIndexList(rel, indexIds, ClassOidIndexId);
    }

    // reset all local indexes on partition usable if needed
    if (RELATION_IS_PARTITIONED(rel)) { /* for partitioned table */
        Oid partOid;
        ListCell* cell = NULL;
        List *partOidList =
            RelationIsSubPartitioned(rel) ? RelationGetSubPartitionOidList(rel) : relationGetPartitionOidList(rel);

        foreach (cell, partOidList) {
            partOid = lfirst_oid(cell);
            ATExecSetIndexUsableState(PartitionRelationId, partOid, true);
        }
    }

    result = (indexIds != NIL);

    bool isRelSubPartitioned = RelationIsSubPartitioned(rel);

    /*
     * Close rel, but continue to hold the lock.
     */
    heap_close(rel, NoLock);

    if (!isPartitioned) { /* for non partitioned table */
        /*
         * If the relation has a secondary toast rel, reindex that too while we
         * still hold the lock on the master table.
         */
        if ((((uint32)flags) & REINDEX_REL_PROCESS_TOAST) && OidIsValid(toast_relid))
            result = ReindexRelation(toast_relid, flags, REINDEX_BTREE_INDEX, baseDesc) || result;
    } else { /* for partitioned table */
        List* partTupleList = NULL;
        ListCell* partCell = NULL;

        if (((uint32)flags) & REINDEX_REL_PROCESS_TOAST) {
            partTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, relid);
            foreach (partCell, partTupleList) {
                if (isRelSubPartitioned) {
                    /* for subpartitioned table, we reindex the toastoid of subpartition */
                    HeapTuple partTuple = (HeapTuple)lfirst(partCell);
                    List *subpartTupleList =
                        searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION, HeapTupleGetOid(partTuple));
                    ListCell* subpartCell = NULL;
                    foreach (subpartCell, subpartTupleList) {
                        Oid toastOid = ((Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(subpartCell)))->reltoastrelid;

                        if (OidIsValid(toastOid)) {
                            result = ReindexRelation(toastOid, flags, REINDEX_BTREE_INDEX, baseDesc) || result;
                        }
                    }
                    freePartList(subpartTupleList);
                } else {
                    /* for partitioned table, we reindex the toastoid of partition */
                    Oid toastOid = ((Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(partCell)))->reltoastrelid;

                    if (OidIsValid(toastOid)) {
                        result = ReindexRelation(toastOid, flags, REINDEX_BTREE_INDEX, baseDesc) || result;
                    }
                }
            }
            freePartList(partTupleList);
        }
    }

    return result;
}

/* ----------------------------------------------------------------
 *		System index reindexing support
 *
 * When we are busy reindexing a system index, this code provides support
 * for preventing catalog lookups from using that index.  We also make use
 * of this to catch attempted uses of user indexes during reindexing of
 * those indexes.
 * ----------------------------------------------------------------
 */

/*
 * ReindexIsProcessingHeap
 *		True if heap specified by OID is currently being reindexed.
 */
bool ReindexIsProcessingHeap(Oid heapOid)
{
    return heapOid == u_sess->catalog_cxt.currentlyReindexedHeap;
}

/*
 * ReindexIsCurrentlyProcessingIndex
 *		True if index specified by OID is currently being reindexed.
 */
static bool ReindexIsCurrentlyProcessingIndex(Oid indexOid)
{
    return indexOid == u_sess->catalog_cxt.currentlyReindexedIndex;
}

/*
 * ReindexIsProcessingIndex
 *		True if index specified by OID is currently being reindexed,
 *		or should be treated as invalid because it is awaiting reindex.
 */
bool ReindexIsProcessingIndex(Oid indexOid)
{
    return indexOid == u_sess->catalog_cxt.currentlyReindexedIndex ||
           list_member_oid(u_sess->catalog_cxt.pendingReindexedIndexes, indexOid);
}

/*
 * SetReindexProcessing
 *		Set flag that specified heap/index are being reindexed.
 *
 * NB: caller must use a PG_TRY block to ensure ResetReindexProcessing is done.
 */
static void SetReindexProcessing(Oid heapOid, Oid indexOid)
{
    Assert(OidIsValid(heapOid) && OidIsValid(indexOid));
    /* Reindexing is not re-entrant. */
    if (OidIsValid(u_sess->catalog_cxt.currentlyReindexedHeap))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("cannot reindex while reindexing")));
    u_sess->catalog_cxt.currentlyReindexedHeap = heapOid;
    u_sess->catalog_cxt.currentlyReindexedIndex = indexOid;
    /* Index is no longer "pending" reindex. */
    RemoveReindexPending(indexOid);
}

/*
 * ResetReindexProcessing
 *		Unset reindexing status.
 */
static void ResetReindexProcessing(void)
{
    u_sess->catalog_cxt.currentlyReindexedHeap = InvalidOid;
    u_sess->catalog_cxt.currentlyReindexedIndex = InvalidOid;
}

/*
 * SetReindexPending
 *		Mark the given indexes as pending reindex.
 *
 * NB: caller must use a PG_TRY block to ensure ResetReindexPending is done.
 * Also, we assume that the current memory context stays valid throughout.
 */
static void SetReindexPending(List* indexes)
{
    /* Reindexing is not re-entrant. */
    if (u_sess->catalog_cxt.pendingReindexedIndexes)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("cannot reindex while reindexing")));
    u_sess->catalog_cxt.pendingReindexedIndexes = list_copy(indexes);
}

/*
 * RemoveReindexPending
 *		Remove the given index from the pending list.
 */
static void RemoveReindexPending(Oid indexOid)
{
    u_sess->catalog_cxt.pendingReindexedIndexes =
        list_delete_oid(u_sess->catalog_cxt.pendingReindexedIndexes, indexOid);
}

/*
 * ResetReindexPending
 *		Unset reindex-pending status.
 */
static void ResetReindexPending(void)
{
    u_sess->catalog_cxt.pendingReindexedIndexes = NIL;
}

void reindex_partIndex(Relation heapRel, Partition heapPart, Relation indexRel, Partition indexPart)
{

    IndexInfo* indexInfo = NULL;
    /*
     * Also check for active uses of the index in the current transaction; we
     * don't want to reindex underneath an open indexscan.
     */
    CheckTableNotInUse(indexRel, "REINDEX INDEX");

    /*
     * All predicate locks on the index are about to be made invalid. Promote
     * them to relation locks on the heap.
     */

    // change the storage of part index
    PartitionSetNewRelfilenode(indexRel, indexPart, InvalidTransactionId, InvalidMultiXactId);

    // build the part index
    indexInfo = BuildIndexInfo(indexRel);
    index_build(heapRel, heapPart, indexRel, indexPart, indexInfo, false, true, INDEX_CREATE_LOCAL_PARTITION);
}

/*
 * reindexPartition - This routine is used to recreate all indexes
 * of a partitioned table (and optionally its toast relation too, if any).
 *
 * "flags" is a bitmask that can include any combination of these bits:
 *
 * REINDEX_REL_PROCESS_TOAST: if true, process the toast table too (if any).
 *
 * REINDEX_REL_SUPPRESS_INDEX_USE: if true, the relation was just completely
 * rebuilt by an operation such as VACUUM FULL or CLUSTER, and therefore its
 * indexes are inconsistent with it.  This makes things tricky if the relation
 * is a system catalog that we might consult during the reindexing.  To deal
 * with that case, we mark all of the indexes as pending rebuild so that they
 * won't be trusted until rebuilt.  The caller is required to call us *without*
 * having made the rebuilt table visible by doing CommandCounterIncrement;
 * we'll do CCI after having collected the index list.  (This way we can still
 * use catalog indexes while collecting the list.)
 *
 * REINDEX_REL_CHECK_CONSTRAINTS: if true, recheck unique and exclusion
 * constraint conditions, else don't.  To avoid deadlocks, VACUUM FULL or
 * CLUSTER on a system catalog must omit this flag.  REINDEX should be used to
 * rebuild an index if constraint inconsistency is suspected.  For optimal
 * performance, other callers should include the flag only after transforming
 * the data in a manner that risks a change in constraint validity.
 *
 * Returns true if any indexes were rebuilt (including toast table's index
 * when relevant).	Note that a CommandCounterIncrement will occur after each
 * index rebuild.
 */
// add parameter reindexType: only reindex index of type specified by reindexType.
// this parameter was added when i implemented index merge, which is a key step
// of partition merge.For b-tree index, we generate a new indes using 2 ordered b-tree indexes.
// For other indexes, we call reindexPartition() to reindex them.
bool reindexPartition(Oid relid, Oid partOid, int flags, int reindexType)
{
    Relation rel;
    List* indexIds = NIL;
    bool result = false;
    Oid toastOid = InvalidOid;
    HeapTuple partitionTup;

    /*
     * Open and lock the relation.	ShareLock is sufficient since we only need
     * to prevent schema and data changes in it.  The lock level used here
     * should match ReindexTable().
     */
    // to promote the concurrency of vacuum full on partitions in mppdb version,
    // degrade lockmode to AccessShareLock
    rel = heap_open(relid, AccessShareLock);

    /*
     * Get the list of index OIDs for this relation.  (We trust to the
     * relcache to get this with a sequential scan if ignoring system
     * indexes.)
     */
    indexIds = RelationGetSpecificKindIndexList(rel, false);

    /*
     * reindex_index will attempt to update the pg_class rows for the relation
     * and index.  If we are processing pg_class itself, we want to make sure
     * that the updates do not try to insert index entries into indexes we
     * have not processed yet.	(When we are trying to recover from corrupted
     * indexes, that could easily cause a crash.) We can accomplish this
     * because CatalogUpdateIndexes will use the relcache's index list to know
     * which indexes to update. We just force the index list to be only the
     * stuff we've processed.
     *
     * It is okay to not insert entries into the indexes we have not processed
     * yet because all of this is transaction-safe.  If we fail partway
     * through, the updated rows are dead and it doesn't matter whether they
     * have index entries.	Also, a new pg_class index will be created with a
     * correct entry for its own pg_class row because we do
     * RelationSetNewRelfilenode() before we do index_build().
     *
     * Note that we also clear pg_class's rd_oidindex until the loop is done,
     * so that that index can't be accessed either.  This means we cannot
     * safely generate new relation OIDs while in the loop; shouldn't be a
     * problem.
     */
    PG_TRY();
    {
        ListCell* indexId = NULL;

        if (((uint32)flags) & REINDEX_REL_SUPPRESS_INDEX_USE) {
            /* Suppress use of all the indexes until they are rebuilt */
            SetReindexPending(indexIds);

            /*
             * Make the new heap contents visible --- now things might be
             * inconsistent!
             */
            CommandCounterIncrement();
        }

        /* Reindex all the indexes. */
        foreach (indexId, indexIds) {
            Oid indexOid = lfirst_oid(indexId);
            Relation indexRel = index_open(indexOid, AccessShareLock);

            if ((((uint32)reindexType) & REINDEX_ALL_INDEX) ||
                ((((uint32)reindexType) & REINDEX_BTREE_INDEX) && (indexRel->rd_rel->relam == BTREE_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_HASH_INDEX) && (indexRel->rd_rel->relam == HASH_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_GIN_INDEX) && (indexRel->rd_rel->relam == GIN_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_CGIN_INDEX) && (indexRel->rd_rel->relam == CGIN_AM_OID)) ||
                ((((uint32)reindexType) & REINDEX_GIST_INDEX) && (indexRel->rd_rel->relam == GIST_AM_OID))) {

                reindexPartIndex(indexOid, partOid, !(((uint32)flags) & REINDEX_REL_CHECK_CONSTRAINTS));
                index_close(indexRel, AccessShareLock);

#ifndef ENABLE_MULTIPLE_NODES
                if (RelationIsCUFormat(rel) && indexRel->rd_index != NULL && indexRel->rd_index->indisunique) {
                    ReindexPartDeltaIndex(indexOid, partOid);
                }
#endif

                CommandCounterIncrement();
            } else
                index_close(indexRel, AccessShareLock);

            /* Index should no longer be in the pending list */
            Assert(!ReindexIsProcessingIndex(indexOid));
        }
    }
    PG_CATCH();
    {
        /* Make sure list gets cleared on error exit */
        ResetReindexPending();
        PG_RE_THROW();
    }
    PG_END_TRY();
    ResetReindexPending();

    /*
     * Close rel, but continue to hold the lock.
     */
    heap_close(rel, NoLock);

    // reset all local indexes on partition usable if needed
    ATExecSetIndexUsableState(PartitionRelationId, partOid, true);

    result = (indexIds != NIL);

    if (!(((uint32)flags) & REINDEX_REL_PROCESS_TOAST)) {
        return result;
    }

    partitionTup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));

    if (!HeapTupleIsValid(partitionTup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", partOid)));
    }

    toastOid = ((Form_pg_partition)GETSTRUCT(partitionTup))->reltoastrelid;
    ReleaseSysCache(partitionTup);

    if (OidIsValid(toastOid)) {
        result = ReindexRelation(toastOid, flags, REINDEX_BTREE_INDEX, NULL) || result;
    }

    return result;
}

/*
 * reindexPartIndex - This routine is used to recreate a single index partition
 */
static void reindexPartIndex(Oid indexId, Oid partOid, bool skip_constraint_checks)
{
    Relation iRel, heapRelation, pg_index;
    Oid heapId;
    IndexInfo* indexInfo = NULL;
    HeapTuple indexTuple;
    Form_pg_index indexForm;
    volatile bool skipped_constraint = false;
    Partition heapPart = NULL;
    Partition indexpart = NULL;

    /*
     * Open and lock the parent heap relation.	ShareLock is sufficient since
     * we only need to be sure no schema or data changes are going on.
     */
    heapId = IndexGetRelation(indexId, false);
    // to promote the concurrency of vacuum full on partitions in mppdb version,
    // degrade lockmode to AccessShareLock
    heapRelation = heap_open(heapId, AccessShareLock);

    /*
     * Open the target index relation and get an exclusive lock on it, to
     * ensure that no one else is touching this particular index.
     */
    // to promote the concurrency of vacuum full on partitions in mppdb version,
    // degrade lockmode to AccessShareLock
    iRel = index_open(indexId, AccessShareLock);
    /*
     * Don't allow reindex on temp tables of other backends ... their local
     * buffer manager is not going to cope.
     */
    if (RELATION_IS_OTHER_TEMP(iRel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot reindex temporary tables of other sessions")));

    /*
     * Also check for active uses of the index in the current transaction; we
     * don't want to reindex underneath an open indexscan.
     */

    /*
     * All predicate locks on the index are about to be made invalid. Promote
     * them to relation locks on the heap.
     */
    TransferPredicateLocksToHeapRelation(iRel);

    PG_TRY();
    {
        Relation partRel = NULL;
        SysScanDesc partScan;
        HeapTuple partTuple;
        Form_pg_partition partForm;
        ScanKeyData partKey;
        Oid indexPartOid = InvalidOid;

        /* Suppress use of the target index while rebuilding it */
        SetReindexProcessing(heapId, indexId);

        /* Fetch info needed for index_build */
        indexInfo = BuildIndexInfo(iRel);

        /* If requested, skip checking uniqueness/exclusion constraints */
        if (skip_constraint_checks) {
            if (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL)
                skipped_constraint = true;
            indexInfo->ii_Unique = false;
            indexInfo->ii_ExclusionOps = NULL;
            indexInfo->ii_ExclusionProcs = NULL;
            indexInfo->ii_ExclusionStrats = NULL;
        }

        // step 1: rebuild index partition
        /*
         * Find the tuple in pg_partition whose 'indextblid' is partOid
         * and 'parentid' is indexId with systable scan.
         */
        partRel = heap_open(PartitionRelationId, AccessShareLock);

        ScanKeyInit(&partKey, Anum_pg_partition_indextblid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partOid));

        partScan = systable_beginscan(partRel, PartitionIndexTableIdIndexId, true, NULL, 1, &partKey);

        while ((partTuple = systable_getnext(partScan)) != NULL) {
            partForm = (Form_pg_partition)GETSTRUCT(partTuple);

            if (partForm->parentid == indexId) {
                indexPartOid = HeapTupleGetOid(partTuple);
                break;
            }
        }

        /* End scan and close pg_partition */
        systable_endscan(partScan);
        heap_close(partRel, AccessShareLock);

        if (!OidIsValid(indexPartOid)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for partitioned index %u", indexId)));
        }

        /* Now, we have get the index partition oid and open it. */
        heapPart = partitionOpen(heapRelation, partOid, ShareLock);
        indexpart = partitionOpen(iRel, indexPartOid, AccessExclusiveLock);

        // REINDEX INDEX
        CheckPartitionNotInUse(indexpart, "REINDEX INDEX index_partition");

        PartitionSetNewRelfilenode(iRel, indexpart, InvalidTransactionId, InvalidMultiXactId);
        index_build(heapRelation, heapPart, iRel, indexpart, indexInfo, false, true, INDEX_CREATE_LOCAL_PARTITION);

        /*
         * The whole partitioned index has brokenUndoChain if any one
         * partition has brokenUndoChain.
         */
        partitionClose(iRel, indexpart, NoLock);
        partitionClose(heapRelation, heapPart, NoLock);

        // step 2: reset indisusable state of index partition
        ATExecSetIndexUsableState(PartitionRelationId, indexPartOid, true);
    }
    PG_CATCH();
    {
        /* Make sure flag gets cleared on error exit */
        if (PointerIsValid(indexpart)) {
            partitionClose(iRel, indexpart, NoLock);
        }

        if (PointerIsValid(heapPart)) {
            partitionClose(heapRelation, heapPart, NoLock);
        }

        ResetReindexProcessing();
        PG_RE_THROW();
    }
    PG_END_TRY();
    ResetReindexProcessing();
    if (indexInfo == NULL)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Memory alloc failed for indexInfo")));

    /*
     * If the index is marked invalid or not ready (ie, it's from a failed
     * CREATE INDEX CONCURRENTLY), and we didn't skip a uniqueness check, we
     * can now mark it valid.  This allows REINDEX to be used to clean up in
     * such cases.
     *
     * We can also reset indcheckxmin, because we have now done a
     * non-concurrent index build, *except* in the case where index_build
     * found some still-broken HOT chains.	If it did, we normally leave
     * indcheckxmin alone (note that index_build won't have changed it,
     * because this is a reindex).	But if the index was invalid or not ready
     * and there were broken HOT chains, it seems best to force indcheckxmin
     * true, because the normal argument that the HOT chains couldn't conflict
     * with the index is suspect for an invalid index.
     *
     * Note that it is important to not update the pg_index entry if we don't
     * have to, because updating it will move the index's usability horizon
     * (recorded as the tuple's xmin value) if indcheckxmin is true.  We don't
     * really want REINDEX to move the usability horizon forward ever, but we
     * have no choice if we are to fix indisvalid or indisready.  Of course,
     * clearing indcheckxmin eliminates the issue, so we're happy to do that
     * if we can.  Another reason for caution here is that while reindexing
     * pg_index itself, we must not try to update it.  We assume that
     * pg_index's indexes will always have these flags in their clean state.
     */
    if (!skipped_constraint) {
        pg_index = heap_open(IndexRelationId, RowExclusiveLock);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexId));
        if (!HeapTupleIsValid(indexTuple))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));

        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);

        if (!indexForm->indisvalid || !indexForm->indisready ||
            (indexForm->indcheckxmin && !indexInfo->ii_BrokenHotChain)) {
            if (!indexInfo->ii_BrokenHotChain)
                indexForm->indcheckxmin = false;
            else if (!indexForm->indisvalid || !indexForm->indisready)
                indexForm->indcheckxmin = true;
            simple_heap_update(pg_index, &indexTuple->t_self, indexTuple);
            CatalogUpdateIndexes(pg_index, indexTuple);
        }

        heap_close(pg_index, RowExclusiveLock);
    }

    /* Update reltuples and relpages in pg_class for partitioned index. */
    vac_update_pgclass_partitioned_table(iRel, false, InvalidTransactionId, InvalidMultiXactId);

    /* Close rels, but keep locks */
    index_close(iRel, NoLock);
    heap_close(heapRelation, NoLock);
}

void ScanHeapInsertCBI(Relation parentRel, Relation heapRel, Relation idxRel, Oid tmpPartOid)
{
    TransactionId oldestXmin;
    ItemPointerData startCtid, endCtid;
    bool isSystemCatalog   = false;
    bool checking_uniqueness = false;
    BlockNumber root_blkno = InvalidBlockNumber;
    OffsetNumber root_offsets[MaxHeapTuplesPerPage];
    List* predicate = NIL;
    Tuple tuple            = NULL;
    IndexInfo *idxInfo     = NULL;
    EState* estate         = NULL;
    ExprContext* econtext  = NULL;
    TupleDesc tupleDesc    = NULL;
    TupleTableSlot* slot   = NULL;
    TableScanDesc scan     = NULL;
    TableScanDesc currScan = NULL;

    oldestXmin = GetOldestXmin(parentRel);
    idxInfo = BuildIndexInfo(idxRel);
    /* Remember if it's a system catalog */
    isSystemCatalog = IsSystemRelation(parentRel);

    /* See whether we're verifying uniqueness/exclusion properties */
    checking_uniqueness = (idxInfo->ii_Unique || idxInfo->ii_ExclusionOps != NULL);

    tupleDesc = heapRel->rd_att;
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(parentRel), false, parentRel->rd_tam_type);
    econtext->ecxt_scantuple = slot;
    /* Set up execution state for predicate, if any. */
    predicate = (List*)ExecPrepareExpr((Expr*)idxInfo->ii_Predicate, estate);

    scan = scan_handler_tbl_beginscan(heapRel, SnapshotAny, 0, NULL, NULL, true);
    if (scan == NULL) {
        ereport(PANIC, (errmsg("failed to create table scan.")));
    }
    while ((tuple = scan_handler_tbl_getnext(scan, ForwardScanDirection, NULL)) != NULL) {
        currScan = GetTableScanDesc(scan, heapRel);
        ItemPointer t_ctid = tableam_tops_get_t_self(parentRel, tuple);
        RelationGetCtids(currScan->rs_rd, &startCtid, &endCtid);
        if (ItemPointerCompare(t_ctid, &startCtid) < 0 || ItemPointerCompare(t_ctid, &endCtid) > 0) {
            continue;
        }
        HeapTuple heapTuple = (HeapTuple)tuple;
        bool tupleIsAlive = false;
        HTSV_Result result = HEAPTUPLE_LIVE;

        if (currScan->rs_cblock != root_blkno) {
            Page page = BufferGetPage(currScan->rs_cbuf);
            LockBuffer(currScan->rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets);
            LockBuffer(currScan->rs_cbuf, BUFFER_LOCK_UNLOCK);
            root_blkno = currScan->rs_cblock;
        }

        /* do our own time qual check */
        bool indexIt = false;
        TransactionId xwait;

    recheck:

        /*
         * We could possibly get away with not locking the buffer here,
         * since caller should hold ShareLock on the relation, but let's
         * be conservative about it.  (This remark is still correct even
         * with HOT-pruning: our pin on the buffer prevents pruning.)
         */
        LockBuffer(currScan->rs_cbuf, BUFFER_LOCK_SHARE);
        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = currScan->rs_rd;

        result = HeapTupleSatisfiesVacuum(heapTuple, oldestXmin, currScan->rs_cbuf);
        switch (result) {
            case HEAPTUPLE_DEAD:
                /* Definitely dead, we can ignore it */
                indexIt = false;
                tupleIsAlive = false;
                break;
            case HEAPTUPLE_LIVE:
                /* Normal case, index and unique-check it */
                indexIt = true;
                tupleIsAlive = true;
                break;
            case HEAPTUPLE_RECENTLY_DEAD:
                /*
                 * If tuple is recently deleted then we must index it
                 * anyway to preserve MVCC semantics.  (Pre-existing
                 * transactions could try to use the index after we finish
                 * building it, and may need to see such tuples.)
                 *
                 * However, if it was HOT-updated then we must only index
                 * the live tuple at the end of the HOT-chain.	Since this
                 * breaks semantics for pre-existing snapshots, mark the
                 * index as unusable for them.
                 */
                if (HeapTupleIsHotUpdated(heapTuple)) {
                    indexIt = false;
                    /* mark the index as unsafe for old snapshots */
                    idxInfo->ii_BrokenHotChain = true;
                } else
                    indexIt = true;
                /* In any case, exclude the tuple from unique-checking */
                tupleIsAlive = false;
                break;
            case HEAPTUPLE_INSERT_IN_PROGRESS:
                ereport(ERROR,
                    (errmsg("Current table is in append mode, heap tuple can not be insert in progress")));
                break;
            case HEAPTUPLE_DELETE_IN_PROGRESS:
                /*
                 * As with INSERT_IN_PROGRESS case, this is unexpected
                 * unless it's our own deletion or a system catalog.
                 */
                Assert(!(heapTuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
                xwait = HeapTupleGetRawXmax(heapTuple);
                if (!TransactionIdIsCurrentTransactionId(xwait)) {
                    if (!isSystemCatalog)
                    ereport(WARNING,
                            (errmsg("concurrent delete in progress within table \"%s\"",
                                    RelationGetRelationName(parentRel))));

                    /*
                     * If we are performing uniqueness checks, assuming
                     * the tuple is dead could lead to missing a
                     * uniqueness violation.  In that case we wait for the
                     * deleting transaction to finish and check again.
                     *
                     * Also, if it's a HOT-updated tuple, we should not
                     * index it but rather the live tuple at the end of
                     * the HOT-chain.  However, the deleting transaction
                     * could abort, possibly leaving this tuple as live
                     * after all, in which case it has to be indexed. The
                     * only way to know what to do is to wait for the
                     * deleting transaction to finish and check again.
                     */
                    if (checking_uniqueness || HeapTupleIsHotUpdated(heapTuple)) {
                        /*
                         * Must drop the lock on the buffer before we wait
                         */
                        LockBuffer(currScan->rs_cbuf, BUFFER_LOCK_UNLOCK);
                        XactLockTableWait(xwait);
                        CHECK_FOR_INTERRUPTS();
                        goto recheck;
                    }

                    /*
                     * Otherwise index it but don't check for uniqueness,
                     * the same as a RECENTLY_DEAD tuple.
                     */
                    indexIt = true;
                } else if (HeapTupleIsHotUpdated(heapTuple)) {
                    /*
                     * It's a HOT-updated tuple deleted by our own xact.
                     * We can assume the deletion will commit (else the
                     * index contents don't matter), so treat the same as
                     * RECENTLY_DEAD HOT-updated tuples.
                     */
                    indexIt = false;
                    /* mark the index as unsafe for old snapshots */
                    idxInfo->ii_BrokenHotChain = true;
                } else {
                    /*
                     * It's a regular tuple deleted by our own xact. Index
                     * it but don't check for uniqueness, the same as a
                     * RECENTLY_DEAD tuple.
                     */
                    indexIt = true;
                }
                /* In any case, exclude the tuple from unique-checking */
                tupleIsAlive = false;
                break;
            default:
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                indexIt = tupleIsAlive = false; /* keep compiler quiet */
                break;
        }

        if (u_sess->attr.attr_storage.enable_debug_vacuum)
            t_thrd.utils_cxt.pRelatedRel = NULL;
        LockBuffer(currScan->rs_cbuf, BUFFER_LOCK_UNLOCK);
        if (!indexIt) {
            continue;
        }
        MemoryContextReset(econtext->ecxt_per_tuple_memory);
        /*
         * In a partial index, discard tuples that don't satisfy the
         * predicate.
         */
        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext, false)) {
                continue;
            }
        }
        // insert the index tuple
        (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        if (RelationIsGlobalIndex(idxRel)) {
            ((HeapTuple)(slot->tts_tuple))->t_tableOid = tmpPartOid;
        }
        Datum values[tupleDesc->natts];
        bool isNull[tupleDesc->natts];
        bool estateIsNotNull = false;
        if (idxInfo->ii_Expressions != NIL || idxInfo->ii_ExclusionOps != NULL) {
            ExprContext* econtext = GetPerTupleExprContext(estate);
            econtext->ecxt_scantuple = slot;
            estateIsNotNull = true;
        }
        FormIndexDatum(idxInfo, slot, estateIsNotNull ? estate : NULL, values, isNull);
        if (HeapTupleIsHeapOnly(heapTuple)) {
            /*
             * For a heap-only tuple, pretend its TID is that of the root. See
             * src/backend/access/heap/README.HOT for discussion.
             */
            HeapTupleData rootTuple;
            OffsetNumber offnum;
            rootTuple = *heapTuple;
            offnum = ItemPointerGetOffsetNumber(&heapTuple->t_self);
            Assert(OffsetNumberIsValid(root_offsets[offnum - 1]));
            ItemPointerSetOffsetNumber(&rootTuple.t_self, root_offsets[offnum - 1]);
            t_ctid = tableam_tops_get_t_self(currScan->rs_rd, (Tuple)(&rootTuple));
        }
        (void)index_insert(idxRel,
                           values,
                           isNull,
                           t_ctid,
                           currScan->rs_rd,
                           idxRel->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);
    }
    scan_handler_tbl_endscan(scan);
    if (PointerIsValid(estate)) {
        FreeExecutorState(estate);
    }
    if (PointerIsValid(slot)) {
        ExecDropSingleTupleTableSlot(slot);
    }
}

/*
 * @@GaussDB@@
 * Target		: This routine is used to scan partition tuples into all global partition indexes.
 * Brief		:
 * Description	:
 * Notes		:
 */
void ScanPartitionInsertIndex(Relation partTableRel, Relation partRel, const List* indexRelList, 
    const List* indexInfoList)
{
    TableScanDesc scan = NULL;
    Tuple tuple = NULL;
    ListCell* cell = NULL;
    ListCell* cell1 = NULL;
    EState* estate = NULL;
    TupleDesc tupleDesc = NULL;
    TupleTableSlot* slot = NULL;

    tupleDesc = partRel->rd_att;

    if (PointerIsValid(indexRelList)) {
        estate = CreateExecutorState();
        slot = MakeSingleTupleTableSlot(RelationGetDescr(partTableRel), false, partTableRel->rd_tam_type);
    }

    scan = scan_handler_tbl_beginscan(partRel, SnapshotNow, 0, NULL);
    if (scan == NULL) {
        ereport(PANIC, (errmsg("failed to create table scan.")));
    }
    while ((tuple = scan_handler_tbl_getnext(scan, ForwardScanDirection, NULL)) != NULL) {
        // insert the index tuple
        if (PointerIsValid(indexRelList)) {
            (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        }

        forboth(cell, indexRelList, cell1, indexInfoList) {
            Relation indexRel = (Relation)lfirst(cell);
            IndexInfo* indexInfo = static_cast<IndexInfo*>(lfirst(cell1));

            Datum values[tupleDesc->natts];
            bool isNull[tupleDesc->natts];
            bool estateIsNotNull = false;
            ItemPointer t_ctid = tableam_tops_get_t_self(partTableRel, tuple);

            if (indexInfo->ii_Expressions != NIL || indexInfo->ii_ExclusionOps != NULL) {
                ExprContext* econtext = GetPerTupleExprContext(estate);
                econtext->ecxt_scantuple = slot;
                estateIsNotNull = true;
            }

            FormIndexDatum(indexInfo, slot, estateIsNotNull ? estate : NULL, values, isNull);

            (void)index_insert(indexRel,
                values,
                isNull,
                t_ctid,
                partRel,
                indexRel->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);
        }
    }
    scan_handler_tbl_endscan(scan);

    if (PointerIsValid(estate)) {
        FreeExecutorState(estate);
    }

    if (PointerIsValid(slot)) {
        ExecDropSingleTupleTableSlot(slot);
    }
}

/*
 * @@GaussDB@@
 * Target		: This routine is used to inserts the partition tuples into all global partition indexes.
 * Brief		:
 * Description	:
 * Notes		:
 */
void AddCBIForPartition(Relation partTableRel, Relation tempTableRel, const List* indexRelList, 
    const List* indexDestOidList)
{
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;

    List* CBIRelList = NIL;
    List* indexInfoList = NIL;
    forboth(cell1, indexRelList, cell2, indexDestOidList) {
        Relation currentIndex = (Relation)lfirst(cell1);
        Oid clonedIndexRelationId = lfirst_oid(cell2);

        Relation clonedIdx = index_open(clonedIndexRelationId, AccessExclusiveLock);
        if (!RelationIsCrossBucketIndex(currentIndex) || clonedIdx->newcbi) {
            /* here the crossbucket index was already merged */
            index_close(clonedIdx, AccessExclusiveLock);
            continue;
        }
        IndexInfo* indexInfo = BuildIndexInfo(clonedIdx);

        CBIRelList = lappend(CBIRelList, clonedIdx);
        indexInfoList = lappend(indexInfoList, indexInfo);
    }
    ScanPartitionInsertIndex(partTableRel, tempTableRel, CBIRelList, indexInfoList);
    foreach (cell1, CBIRelList) {
        Relation indexRel = (Relation)lfirst(cell1);
        relation_close(indexRel, AccessExclusiveLock);
    }

    list_free_ext(CBIRelList);
    list_free_ext(indexInfoList);

}

/*
 * @@GaussDB@@
 * Target		: This routine is used to inserts the partition tuples into all global partition indexes.
 * Brief		:
 * Description	:
 * Notes		:
 */
void AddGPIForPartition(Oid partTableOid, Oid partOid)
{
    Relation partTableRel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    List* indexList = NIL;
    List* indexRelList = NIL;
    List* indexInfoList = NIL;
    ListCell* cell = NULL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    partTableRel = heap_open(partTableOid, AccessShareLock);
    part = partitionOpen(partTableRel, partOid, AccessExclusiveLock);
    partRel = partitionGetRelation(partTableRel, part);
    indexList = RelationGetSpecificKindIndexList(partTableRel, true);

    foreach (cell, indexList) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = relation_open(indexOid, RowExclusiveLock);
        IndexInfo* indexInfo = BuildIndexInfo(indexRel);

        indexRelList = lappend(indexRelList, indexRel);
        indexInfoList = lappend(indexInfoList, indexInfo);
    }

    ScanPartitionInsertIndex(partTableRel, partRel, indexRelList, indexInfoList);

    forboth (lc1, indexRelList, lc2, indexInfoList) {
        Relation indexRel = (Relation)lfirst(lc1);
        IndexInfo* indexInfo  = (IndexInfo*)lfirst(lc2);

        relation_close(indexRel, RowExclusiveLock);
        pfree_ext(indexInfo);
    }

    list_free_ext(indexList);
    list_free_ext(indexRelList);
    list_free_ext(indexInfoList);

    releaseDummyRelation(&partRel);
    partitionClose(partTableRel, part, NoLock);
    heap_close(partTableRel, NoLock);
}

/*
 * @@GaussDB@@
 * Target		: This routine is used to inserts the partition tuples into all global partition indexes.
 * Brief		:
 * Description	:
 * Notes		:
 */
void AddGPIForSubPartition(Oid partTableOid, Oid partOid, Oid subPartOid)
{
    Relation partTableRel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    Relation subPartRel = NULL;
    Partition subPart = NULL;
    List* indexList = NIL;
    List* indexRelList = NIL;
    List* indexInfoList = NIL;
    ListCell* cell = NULL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    partTableRel = heap_open(partTableOid, AccessShareLock);
    part = partitionOpen(partTableRel, partOid, AccessShareLock);
    partRel = partitionGetRelation(partTableRel, part);
    subPart = partitionOpen(partRel, subPartOid, AccessExclusiveLock);
    subPartRel = partitionGetRelation(partRel, subPart);
    indexList = RelationGetSpecificKindIndexList(partTableRel, true);

    foreach (cell, indexList) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = relation_open(indexOid, RowExclusiveLock);
        IndexInfo* indexInfo = BuildIndexInfo(indexRel);

        indexRelList = lappend(indexRelList, indexRel);
        indexInfoList = lappend(indexInfoList, indexInfo);
    }

    ScanPartitionInsertIndex(partTableRel, subPartRel, indexRelList, indexInfoList);

    forboth (lc1, indexRelList, lc2, indexInfoList) {
        Relation indexRel = (Relation)lfirst(lc1);
        IndexInfo* indexInfo  = (IndexInfo*)lfirst(lc2);

        relation_close(indexRel, RowExclusiveLock);
        pfree_ext(indexInfo);
    }


    list_free_ext(indexList);
    list_free_ext(indexRelList);
    list_free_ext(indexInfoList);

    releaseDummyRelation(&subPartRel);
    partitionClose(partRel, subPart, NoLock);
    releaseDummyRelation(&partRel);
    partitionClose(partTableRel, part, NoLock);
    heap_close(partTableRel, NoLock);
}

/*
 * @@GaussDB@@
 * Target : This routine is used to scan partition tuples and delete them from all global partition indexes.
 */
void ScanPartitionDeleteGPITuples(Relation partTableRel, Relation partRel, const List* indexRelList,
    const List* indexInfoList)
{
    TableScanDesc scan = NULL;
    Tuple tuple = NULL;
    ListCell* cell = NULL;
    ListCell* cell1 = NULL;
    EState* estate = NULL;
    TupleDesc tupleDesc = NULL;
    TupleTableSlot* slot = NULL;

    tupleDesc = partRel->rd_att;

    if (PointerIsValid(indexRelList)) {
        estate = CreateExecutorState();
        slot = MakeSingleTupleTableSlot(RelationGetDescr(partTableRel), false, partTableRel->rd_tam_type);
    }

    scan = scan_handler_tbl_beginscan(partRel, SnapshotNow, 0, NULL);
    if (scan == NULL) {
        ereport(PANIC, (errmsg("failed to create table scan.")));
    }
    while ((tuple = scan_handler_tbl_getnext(scan, ForwardScanDirection, NULL)) != NULL) {
        if (PointerIsValid(indexRelList)) {
            (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        }

        forboth(cell, indexRelList, cell1, indexInfoList) {
            Relation indexRel = (Relation)lfirst(cell);
            IndexInfo* indexInfo = static_cast<IndexInfo*>(lfirst(cell1));

            /* only ubtree have index_delete routine */
            Assert(RelationIsUstoreIndex(indexRel));

            Datum values[tupleDesc->natts];
            bool isNull[tupleDesc->natts];
            bool estateIsNotNull = false;
            ItemPointer t_ctid = tableam_tops_get_t_self(partTableRel, tuple);

            if (indexInfo->ii_Expressions != NIL || indexInfo->ii_ExclusionOps != NULL) {
                ExprContext* econtext = GetPerTupleExprContext(estate);
                econtext->ecxt_scantuple = slot;
                estateIsNotNull = true;
            }

            FormIndexDatum(indexInfo, slot, estateIsNotNull ? estate : NULL, values, isNull);

            (void)index_delete(indexRel,
                values,
                isNull,
                t_ctid);
        }
    }
    scan_handler_tbl_endscan(scan);

    if (PointerIsValid(estate)) {
        FreeExecutorState(estate);
    }

    if (PointerIsValid(slot)) {
        ExecDropSingleTupleTableSlot(slot);
    }
}

/*
 * @@GaussDB@@
 * Target : This routine is used to delete the partition tuples from all global partition indexes.
 */
bool DeleteGPITuplesForPartition(Oid partTableOid, Oid partOid)
{
    Relation partTableRel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    List* indexList = NIL;
    List* indexRelList = NIL;
    List* indexInfoList = NIL;
    ListCell* cell = NULL;
    bool all_ubtree = true;

    partTableRel = heap_open(partTableOid, AccessShareLock);
    part = partitionOpen(partTableRel, partOid, AccessExclusiveLock);
    partRel = partitionGetRelation(partTableRel, part);
    indexList = RelationGetSpecificKindIndexList(partTableRel, true);

    foreach (cell, indexList) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = relation_open(indexOid, RowExclusiveLock);
        IndexInfo* indexInfo = BuildIndexInfo(indexRel);

        if (!RelationIsUstoreIndex(indexRel)) {
            all_ubtree = false;
            relation_close(indexRel, RowExclusiveLock);
            continue;
        }

        indexRelList = lappend(indexRelList, indexRel);
        indexInfoList = lappend(indexInfoList, indexInfo);
    }

    ScanPartitionDeleteGPITuples(partTableRel, partRel, indexRelList, indexInfoList);

    foreach (cell, indexRelList) {
        Relation indexRel = (Relation)lfirst(cell);

        relation_close(indexRel, RowExclusiveLock);
    }

    list_free_ext(indexList);
    list_free_ext(indexRelList);
    list_free_ext(indexInfoList);

    releaseDummyRelation(&partRel);
    partitionClose(partTableRel, part, NoLock);
    heap_close(partTableRel, NoLock);

    return all_ubtree;
}

/*
 * @@GaussDB@@
 * Target		: This routine is used to delete the partition tuples from all global partition indexes.
 * Brief		:
 * Description	:
 * Notes		:
 */
bool DeleteGPITuplesForSubPartition(Oid partTableOid, Oid partOid, Oid subPartOid)
{
    Relation partTableRel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    Relation subPartRel = NULL;
    Partition subPart = NULL;
    List* indexList = NIL;
    List* indexRelList = NIL;
    List* indexInfoList = NIL;
    ListCell* cell = NULL;
    bool all_ubtree = true;

    partTableRel = heap_open(partTableOid, AccessShareLock);
    part = partitionOpen(partTableRel, partOid, AccessShareLock);
    partRel = partitionGetRelation(partTableRel, part);
    subPart = partitionOpen(partRel, subPartOid, AccessExclusiveLock);
    subPartRel = partitionGetRelation(partRel, subPart);
    indexList = RelationGetSpecificKindIndexList(partTableRel, true);

    foreach (cell, indexList) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = relation_open(indexOid, RowExclusiveLock);
        IndexInfo* indexInfo = BuildIndexInfo(indexRel);

        if (!RelationIsUstoreIndex(indexRel)) {
            all_ubtree = false;
            relation_close(indexRel, RowExclusiveLock);
            continue;
        }

        indexRelList = lappend(indexRelList, indexRel);
        indexInfoList = lappend(indexInfoList, indexInfo);
    }

    ScanPartitionDeleteGPITuples(partTableRel, subPartRel, indexRelList, indexInfoList);

    foreach (cell, indexRelList) {
        Relation indexRel = (Relation)lfirst(cell);

        relation_close(indexRel, RowExclusiveLock);
    }

    list_free_ext(indexList);
    list_free_ext(indexRelList);
    list_free_ext(indexInfoList);

    releaseDummyRelation(&subPartRel);
    partitionClose(partRel, subPart, NoLock);
    releaseDummyRelation(&partRel);
    partitionClose(partTableRel, part, NoLock);
    heap_close(partTableRel, NoLock);

    return all_ubtree;
}

void mergeBTreeIndexes(List* mergingBtreeIndexes, List* srcPartMergeOffset, int2 bktId)
{
    Relation targetIndexRelation = NULL;
    Relation srcIndexRelation = NULL;
    IndexScanDesc srcIndexScan = NULL;
    RegProcedure procedure;
    List* mergeBTScanList = NIL;
    ListCell* cell = NULL;

    if (mergingBtreeIndexes == NULL || mergingBtreeIndexes->length < 3 ||
        mergingBtreeIndexes->length > (MAX_MERGE_PARTITIONS + 1) ||
        mergingBtreeIndexes->length - srcPartMergeOffset->length != 1)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("mergingBtreeIndexes, zero or less than 2 or greater than 4 source index relations")));

    // step 1: pick out target index relation and generate mergeBTScanList.
    foreach (cell, mergingBtreeIndexes) {
        srcIndexRelation = (Relation)lfirst(cell);
        if (!OID_IS_BTREE(srcIndexRelation->rd_rel->relam)) {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("mergingBtreeIndexes, only btree indexes can be merged")));
        }

        if (cell == list_head(mergingBtreeIndexes)) {
            targetIndexRelation = srcIndexRelation;
            // target index relation must be empty
            if (RelationGetNumberOfBlocks(targetIndexRelation) != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("target merging index \"%s\" already contains data",
                            RelationGetRelationName(targetIndexRelation))));
            }
        } else {
            // begin scan on src index relation
            srcIndexScan = (IndexScanDesc)index_beginscan(NULL, srcIndexRelation, SnapshotAny, 0, 0);
            srcIndexScan->xs_want_itup = true;
            srcIndexScan->xs_want_xid = RelationIsUstoreIndex(targetIndexRelation);
            srcIndexScan->xs_want_bucketid = RelationIsCrossBucketIndex(targetIndexRelation);
            index_rescan(srcIndexScan, NULL, 0, NULL, 0);
            if (srcIndexScan->xs_want_bucketid) {
                srcIndexScan->xs_cbi_scan->mergingBktId = bktId;
            }
            mergeBTScanList = lappend(mergeBTScanList, srcIndexScan);
        }
    }

    /*
     * step2: Call the access method's build procedure, merge is a special build method
     */
    procedure = targetIndexRelation->rd_am->ammerge;
    Assert(RegProcedureIsValid(procedure));

    DatumGetPointer(OidFunctionCall3(procedure,
        PointerGetDatum(targetIndexRelation),
        PointerGetDatum(mergeBTScanList),
        PointerGetDatum(srcPartMergeOffset)));

    // step 3: end the src index relation scan
    foreach (cell, mergeBTScanList) {
        srcIndexScan = (IndexScanDesc)lfirst(cell);
        index_endscan(srcIndexScan);
    }
    list_free(mergeBTScanList);
}

bool RecheckIndexTuple(const IndexScanDesc scan, TupleTableSlot *slot)
{
    /* only recheck for ubtree */
    if (!RelationIsUstoreIndex(scan->indexRelation)) {
        return true;
    }

    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    IndexTuple itup = scan->xs_itup;

    /* need to construct index tuple through uheap tuple, then compare the index columns */
    MemoryContext oldContext = NULL;
    ExprContext *econtext = NULL;
    /* we need IndexInfo and EState to execute FormIndexDatum() */
    if (so->indexInfo == NULL || so->fakeEstate == NULL) {
        so->fakeEstate = CreateExecutorState();
        /* switch memory context to a temporary context */
        oldContext = MemoryContextSwitchTo(so->fakeEstate->es_query_cxt);
        so->indexInfo = BuildIndexInfo(scan->indexRelation);
    } else {
        oldContext = MemoryContextSwitchTo(so->fakeEstate->es_query_cxt);
    }
    /* set scan tuple slot */
    econtext = GetPerTupleExprContext(so->fakeEstate);
    econtext->ecxt_scantuple = slot;

    int nattrs = IndexRelationGetNumberOfAttributes(scan->indexRelation);
    Datum *values = (Datum*)palloc(sizeof(Datum) * nattrs);
    bool *isnull = (bool*)palloc(sizeof(bool) * nattrs);

    /* form index datum with correct tuple descriptor */
    FormIndexDatum(so->indexInfo, slot, so->fakeEstate, values, isnull);

    IndexTuple trueItup = index_form_tuple(RelationGetDescr(scan->indexRelation), values, isnull);
    trueItup->t_tid = itup->t_tid;
    /*
     * compare the binary directly. If these index tuples are formed from the
     * same uheap tuple, they should be exactly the same.
     */
    bool result = IndexTupleSize(itup) == IndexTupleSize(trueItup) &&
                  memcmp(itup, trueItup, IndexTupleSize(itup)) == 0;

    /* be tidy */
    MemoryContextSwitchTo(oldContext);
    ResetExprContext(econtext); /* reset per tuple context */
    pfree(trueItup);
    pfree(values);
    pfree(isnull);

    return result;
}

TupleDesc GetPsortTupleDesc(TupleDesc indexTupDesc)
{
    int numatts = indexTupDesc->natts + 1;
    TupleDesc psortTupDesc = CreateTemplateTupleDesc(numatts, false);

    /* Add key columns */
    for (int i = 0; i < numatts - 1; i++) {
        Form_pg_attribute from = indexTupDesc->attrs[i];

        AttrNumber attId = i + 1;
        TupleDescInitEntry(psortTupDesc, attId, from->attname.data, from->atttypid, from->atttypmod, from->attndims);
    }

    /* Add tid as last column */
    TupleDescInitEntry(psortTupDesc, (AttrNumber)numatts, "tid", INT8OID, -1, 0);

    return psortTupDesc;
}

/* 
 * indexRelOptions is passed in by a third parameter but not fetched from indexRelation,
 * because when called indexRelation may not hold the options at all.
 * tablespaceId: tablespace used for psort index, which may be different from indexRelation
 *               if it's a partition. 
 */
Oid psort_create(const char* indexRelationName, Relation indexRelation, Oid tablespaceId, Datum indexRelOptions)
{
    Oid namespaceId = CSTORE_NAMESPACE;
    Oid ownerId = indexRelation->rd_rel->relowner;
    bool shared_relation = indexRelation->rd_rel->relisshared;
    bool mapped_relation = RelationIsMapped(indexRelation);
    char relpersistence = indexRelation->rd_rel->relpersistence;

    // Step 1. get psort column table tuple descriptor
    TupleDesc psortTupDesc = GetPsortTupleDesc(indexRelation->rd_att);

    // Step 2. get reloptions
    indexRelOptions = AddOrientationOption(indexRelOptions, true);

    indexRelOptions = AddInternalOption(indexRelOptions,
        INTERNAL_MASK_DALTER | INTERNAL_MASK_DDELETE | INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE |
            INTERNAL_MASK_ENABLE);

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = bupgrade_get_next_psort_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = bupgrade_get_next_psort_array_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = bupgrade_get_next_psort_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = bupgrade_get_next_psort_pg_class_rfoid();
    }

    // Step 3. cretate psort column table
    Oid psortRelationId = heap_create_with_catalog(indexRelationName,
        namespaceId,
        tablespaceId,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        ownerId,
        psortTupDesc,
        NIL,
        RELKIND_RELATION,
        relpersistence,
        shared_relation,
        mapped_relation,
        true,
        0,
        ONCOMMIT_NOOP,
        indexRelOptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL,
        false);

    CommandCounterIncrement();

    Relation psortRelation = relation_open(psortRelationId, AccessExclusiveLock);

    // Step 4. add cluster constraint
    int natts = psortRelation->rd_att->natts - 1;
    int16* attrNums = (int16*)palloc(sizeof(int16) * natts);
    for (int i = 0; i < natts; i++) {
        attrNums[i] = i + 1;
    }

    char* conname = ChooseConstraintName(RelationGetRelationName(psortRelation), NULL, "cluster", namespaceId, NIL);

    CreateConstraintEntry(conname, /* Constraint Name */
        namespaceId,               /* namespace */
        CONSTRAINT_CLUSTER,        /* Constraint Type */
        false,                     /* Is Deferrable */
        false,                     /* Is Deferred */
        true,                      /* Is Validated */
        psortRelationId,           /* relation */
        attrNums,                  /* attrs in the constraint */
        natts,                     /* # key attrs in the constraint */
        natts,                     /* total # attrs (include attrs and key attrs) in the constraint */
        InvalidOid,                /* not a domain constraint */
        InvalidOid,                /* no associated index */
        InvalidOid,                /* Foreign key fields */
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        ' ',
        ' ',
        ' ',
        NULL,  /* not an exclusion constraint */
        NULL,  /* Tree form of check constraint */
        NULL,  /* Binary form of check constraint */
        NULL,  /* Source form of check constraint */
        true,  /* conislocal */
        0,     /* coninhcount */
        true,  /* connoinherit */
        NULL); /* @hdfs informational constraint */

    SetRelHasClusterKey(psortRelation, true);

    // Step 5. create cudesc table and delta table
    CreateCUDescTable(psortRelation, (Datum)0, false);
    (void)CreateDeltaTable(psortRelation, (Datum)0, false, NULL);
    CStore::CreateStorage(psortRelation, InvalidOid);

    relation_close(psortRelation, NoLock);

    return psortRelationId;
}

static bool binary_upgrade_is_next_part_index_pg_class_oid_valid()
{
    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_oid) {
        return false;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid >=
        u_sess->upg_cxt.binary_upgrade_max_part_index_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_oid
                        [u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid])) {
        return false;
    }

    return true;
}

static Oid binary_upgrade_get_next_part_index_pg_class_oid()
{
    Oid old_part_index_pg_class_oid = InvalidOid;
    if (false == binary_upgrade_is_next_part_index_pg_class_oid_valid()) {
        return InvalidOid;
    }

    old_part_index_pg_class_oid =
        u_sess->upg_cxt
            .binary_upgrade_next_part_index_pg_class_oid[u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_index_pg_class_oid[u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid++;

    return old_part_index_pg_class_oid;
}

static Oid binary_upgrade_get_next_part_index_pg_class_rfoid()
{
    Oid old_part_index_pg_class_rfoid = InvalidOid;
    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_rfoid) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_rfoid >=
        u_sess->upg_cxt.binary_upgrade_max_part_index_pg_class_rfoid) {
        return InvalidOid;
    }

    old_part_index_pg_class_rfoid = u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_rfoid
                                        [u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_rfoid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_index_pg_class_rfoid[u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_rfoid++;

    return old_part_index_pg_class_rfoid;
}

static bool binary_upgrade_is_next_psort_pg_class_oid_valid()
{
    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid) {
        return false;
    }

    if (u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid >= u_sess->upg_cxt.bupgrade_max_psort_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_psort_pg_class_oid()
{
    Oid old_psort_pg_class_oid = InvalidOid;
    if (false == binary_upgrade_is_next_psort_pg_class_oid_valid()) {
        return InvalidOid;
    }

    old_psort_pg_class_oid =
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid];

    u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid++;

    return old_psort_pg_class_oid;
}

static Oid bupgrade_get_next_psort_pg_class_rfoid()
{
    Oid old_psort_pg_class_rfoid = InvalidOid;

    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_psort_pg_class_rfoid >= u_sess->upg_cxt.bupgrade_max_psort_pg_class_rfoid) {
        return InvalidOid;
    }

    old_psort_pg_class_rfoid =
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_psort_pg_class_rfoid];

    u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_psort_pg_class_rfoid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_psort_pg_class_rfoid++;

    return old_psort_pg_class_rfoid;
}

static bool binary_upgrade_is_next_psort_pg_type_oid_valid()
{
    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid) {
        return false;
    }

    if (u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_psort_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_psort_pg_type_oid()
{
    Oid old_psort_pg_type_oid = InvalidOid;
    if (false == binary_upgrade_is_next_psort_pg_type_oid_valid()) {
        return InvalidOid;
    }

    old_psort_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid++;

    return old_psort_pg_type_oid;
}

static bool binary_upgrade_is_next_psort_array_pg_type_oid_valid()
{
    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid) {
        return false;
    }

    if (u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_psort_array_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt
                        .bupgrade_next_psort_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_psort_array_pg_type_oid()
{
    Oid old_psort_array_pg_type_oid = InvalidOid;
    if (false == binary_upgrade_is_next_psort_array_pg_type_oid_valid()) {
        return InvalidOid;
    }

    old_psort_array_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid++;

    return old_psort_array_pg_type_oid;
}

/*
 * set IndexCreateExtraArgs, more info refer to defination of IndexCreateExtraArgs
 */
void SetIndexCreateExtraArgs(IndexCreateExtraArgs* extra, Oid psortOid, bool isPartition, bool isGlobal,
    bool crossBucket)
{
    extra->existingPSortOid = psortOid;
    extra->isPartitionedIndex = isPartition;
    extra->isGlobalPartitionedIndex = isGlobal;
    extra->crossBucket = crossBucket;
}

Datum SetWaitCleanCbiRelOptions(Datum oldOptions, bool enable)
{
    Datum newOptions;
    List* defList = NIL;
    DefElem* def = NULL;
    Value* defArg = enable ? makeString(OptEnabledWaitCleanCbi) : makeString(OptDisabledWaitCleanCbi);
    def = makeDefElem(pstrdup("wait_clean_cbi"), (Node*)defArg);
    defList = lappend(defList, def);
    newOptions = transformRelOptions(oldOptions, defList, NULL, NULL, false, false);
    pfree_ext(def->defname);
    list_free_ext(defList);
    return newOptions;
}
void cbi_set_enable_clean(Relation rel)
{
    HeapTuple newTuple;
    Oid relid = RelationGetRelid(rel);
    Relation pg_class;
    HeapTuple tuple;
    Datum relOptions;
    Datum newOptions;
    Datum replVal[Natts_pg_class];
    bool replNull[Natts_pg_class];
    bool replRepl[Natts_pg_class];
    bool isNull = false;
    errno_t rc;
    pg_class = heap_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("could not find tuple for relation %u", relid)));
    }
    relOptions = fastgetattr(tuple, Anum_pg_class_reloptions, RelationGetDescr(pg_class), &isNull);
    newOptions = SetWaitCleanCbiRelOptions(isNull ? (Datum)0 : relOptions, false);
    rc = memset_s(replVal, sizeof(replVal), 0, sizeof(replVal));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replNull, sizeof(replNull), false, sizeof(replNull));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replRepl, sizeof(replRepl), false, sizeof(replRepl));
    securec_check(rc, "\0", "\0");
    if (PointerIsValid(newOptions)) {
        replVal[Anum_pg_class_reloptions - 1] = newOptions;
        replNull[Anum_pg_class_reloptions - 1] = false;
    } else {
        replNull[Anum_pg_class_reloptions - 1] = true;
    }
    replRepl[Anum_pg_class_reloptions - 1] = true;
    newTuple = heap_modify_tuple(tuple, RelationGetDescr(pg_class), replVal, replNull, replRepl);
    simple_heap_update(pg_class, &newTuple->t_self, newTuple);
    CatalogUpdateIndexes(pg_class, newTuple);
    ereport(LOG, (errmsg("index %u set reloptions wait_clean_cbi success", HeapTupleGetOid(tuple))));
    heap_freetuple_ext(tuple);
    heap_freetuple_ext(newTuple);
    heap_close(pg_class, RowExclusiveLock);
    
}
