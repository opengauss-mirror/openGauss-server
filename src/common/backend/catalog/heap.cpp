/* -------------------------------------------------------------------------
 *
 * heap.cpp
 *	  code to create and destroy openGauss heap relations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/heap.cpp
 *
 *
 * INTERFACE ROUTINES
 *		heap_create()			- Create an uncataloged heap relation
 *		heap_create_with_catalog() - Create a cataloged relation
 *		heap_drop_with_catalog() - Removes named relation from catalogs
 *
 * NOTES
 *	  this code taken from access/heap/create.c, which contains
 *	  the old heap_create_with_catalog, amcreate, and amdestroy.
 *	  those routines will soon call these routines using the function
 *	  manager,
 *	  just like the poorly named "NewXXX" routines do.	The
 *	  "New" routines are all going to die soon, once and for all!
 *		-cim 1/13/91
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "knl/knl_variable.h"

#include "access/cstore_delta.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/multixact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gs_obsscaninfo.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_obsscaninfo.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_hashbucket_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/pg_uid_fn.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/storage_gtt.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/typecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_utilcmd.h"
#include "parser/parsetree.h"
#include "pgxc/groupmgr.h"
#include "storage/buf/buf.h"
#include "storage/predicate.h"
#include "storage/page_compression.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/hotkey.h"
#include "utils/int8.h"
#include "utils/int16.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partitionkey.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "foreign/fdwapi.h"
#include "instruments/generate_report.h"
#include "catalog/gs_encrypted_columns.h"

#ifdef PGXC
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_slice.h"
#include "pgxc/locator.h"
#include "pgxc/groupmgr.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "replication/bcm.h"
#endif

#include "client_logic/client_logic.h"

#ifndef ENABLE_MULTIPLE_NODES
#include "utils/pl_package.h"
#endif


static void AddNewRelationTuple(Relation pg_class_desc, Relation new_rel_desc, Oid new_rel_oid, Oid new_type_oid,
    Oid reloftype, Oid relowner, char relkind, char relpersistence, Datum relacl, Datum reloptions,
    int2vector* bucketcol, bool ispartrel);
static oidvector* BuildIntervalTablespace(const IntervalPartitionDefState* intervalPartDef);
static void deletePartitionTuple(Oid part_id);
static void addNewPartitionTuplesForPartition(Relation pg_partition_rel,
    Oid relid, Oid reltablespace,
    Oid bucketOid, PartitionState* partTableState, Oid ownerid, 
    Datum reloptions, const TupleDesc tupledesc, char strategy, StorageType storage_type, LOCKMODE partLockMode);
static void addNewPartitionTupleForTable(Relation pg_partition_rel, const char* relname, const Oid reloid,
    const Oid reltablespaceid, const TupleDesc reltupledesc, const PartitionState* partTableState, Oid ownerid,
    Datum reloptions);

static void addNewPartitionTupleForValuePartitionedTable(Relation pg_partition_rel, const char* relname,
    const Oid reloid, const Oid reltablespaceid, const TupleDesc reltupledesc, const PartitionState* partTableState,
    Datum reloptions);

static void heapDropPartitionTable(Relation relation);

static Oid AddNewRelationType(const char* typeName, Oid typeNamespace, Oid new_rel_oid, char new_rel_kind, Oid ownerid,
    Oid new_row_type, Oid new_array_type);
static void RelationRemoveInheritance(Oid relid);
static void StoreRelCheck(
    Relation rel, const char* ccname, Node* expr, bool is_validated, bool is_local, int inhcount, bool is_no_inherit);
static void StoreConstraints(Relation rel, List* cooked_constraints);
static bool MergeWithExistingConstraint(
    Relation rel, char* ccname, Node* expr, bool allow_merge, bool is_local, bool is_no_inherit);
static void SetRelationNumChecks(Relation rel, int numchecks);
static Node* cookConstraint(ParseState* pstate, Node* raw_constraint, char* relname);
static List* insert_ordered_unique_oid(List* list, Oid datum);
static void InitPartitionDef(Partition newPartition, Oid partOid, char strategy);
static void InitSubPartitionDef(Partition newPartition, Oid partOid, char strategy);
static bool binary_upgrade_is_next_part_pg_partition_oid_valid();
static Oid binary_upgrade_get_next_part_pg_partition_oid();
bool binary_upgrade_is_next_part_toast_pg_class_oid_valid();
static Oid binary_upgrade_get_next_part_toast_pg_class_oid();

static Oid binary_upgrade_get_next_part_toast_pg_class_rfoid();
static Oid binary_upgrade_get_next_part_pg_partition_rfoid();
static void LockSeqConstraints(Relation rel, List* Constraints);
static bool ReferenceGenerated(const List *rawdefaultlist, AttrNumber attnum);
static bool CheckNestedGeneratedWalker(Node *node, ParseState *context);
static bool CheckNestedGenerated(ParseState *pstate, Node *node);
extern void getErrorTableFilePath(char* buf, int len, Oid databaseid, Oid reid);
extern void make_tmptable_cache_key(Oid relNode);

#define RELKIND_IN_RTM (relkind == RELKIND_RELATION || relkind == RELKIND_TOASTVALUE || relkind == RELKIND_MATVIEW)

static RangePartitionDefState *MakeRangeDefaultSubpartition(PartitionState *partitionState, char *partitionName,
    char *tablespacename);
static ListPartitionDefState *MakeListDefaultSubpartition(PartitionState *partitionState, char *partitionName,
    char *tablespacename);
static HashPartitionDefState *MakeHashDefaultSubpartition(PartitionState *partitionState, char *partitionName,
    char *tablespacename);
static void MakeDefaultSubpartitionName(PartitionState *partitionState, char **subPartitionName,
    const char *partitionName);
static void getSubPartitionInfo(char partitionStrategy, Node *partitionDefState, List **subPartitionDefState,
    char **partitionName, char **tablespacename);
/* ----------------------------------------------------------------
 *				XXX UGLY HARD CODED BADNESS FOLLOWS XXX
 *
 *		these should all be moved to someplace in the lib/catalog
 *		module, if not obliterated first.
 * ----------------------------------------------------------------
 */

/*
 * Note:
 *		Should the system special case these attributes in the future?
 *		Advantage:	consume much less space in the ATTRIBUTE relation.
 *		Disadvantage:  special cases will be all over the place.
 */

/*
 * The initializers below do not include trailing variable length fields,
 * but that's OK - we're never going to reference anything beyond the
 * fixed-size portion of the structure anyway.
 */

static FormData_pg_attribute a1 = {0,
    {"ctid"},
    TIDOID,
    0,
    sizeof(ItemPointerData),
    SelfItemPointerAttributeNumber,
    0,
    -1,
    -1,
    false,
    'p',
    's',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a2 = {0,
    {"oid"},
    OIDOID,
    0,
    sizeof(Oid),
    ObjectIdAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a3 = {0,
    {"xmin"},
    XIDOID,
    0,
    sizeof(TransactionId),
    MinTransactionIdAttributeNumber,
    0,
    -1,
    -1,
    FLOAT8PASSBYVAL,
    'p',
    'd',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a4 = {0,
    {"cmin"},
    CIDOID,
    0,
    sizeof(CommandId),
    MinCommandIdAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a5 = {0,
    {"xmax"},
    XIDOID,
    0,
    sizeof(TransactionId),
    MaxTransactionIdAttributeNumber,
    0,
    -1,
    -1,
    FLOAT8PASSBYVAL,
    'p',
    'd',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a6 = {0,
    {"cmax"},
    CIDOID,
    0,
    sizeof(CommandId),
    MaxCommandIdAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

/*
 * We decided to call this attribute "tableoid" rather than say
 * "classoid" on the basis that in the future there may be more than one
 * table of a particular class/type. In any case table is still the word
 * used in SQL.
 */
static FormData_pg_attribute a7 = {0,
    {"tableoid"},
    OIDOID,
    0,
    sizeof(Oid),
    TableOidAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

#ifdef PGXC
/*
 * In XC we need some sort of node identification for each tuple
 * We are adding another system column that would serve as node identifier.
 * This is not only required by WHERE CURRENT OF but it can be used any
 * where we want to know the originating Datanode of a tuple received
 * at the Coordinator
 */
static FormData_pg_attribute a8 = {0,
    {"xc_node_id"},
    INT4OID,
    0,
    sizeof(int4),
    XC_NodeIdAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a9 = {0,
    {"tablebucketid"},
    INT2OID,
    0,
    sizeof(int2),
    BucketIdAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'i',
    true,
    false,
    false,
    true,
    0};

static FormData_pg_attribute a10 = {0,
    {"gs_tuple_uid"},
    INT8OID,
    0,
    sizeof(int64),
    UidAttributeNumber,
    0,
    -1,
    -1,
    true,
    'p',
    'd',
    true,
    false,
    false,
    true,
    0};

static const Form_pg_attribute SysAtt[] = {&a1, &a2, &a3, &a4, &a5, &a6, &a7, &a8, &a9, &a10};
#else
static const Form_pg_attribute SysAtt[] = {&a1, &a2, &a3, &a4, &a5, &a6, &a7};
#endif

/*
 * This function returns a Form_pg_attribute pointer for a system attribute.
 * Note that we elog if the presented attno is invalid, which would only
 * happen if there's a problem upstream.
 */
Form_pg_attribute SystemAttributeDefinition(AttrNumber attno, bool relhasoids, bool relhasbucket, bool relhasuids)
{
    if (attno >= 0 || attno < -(int)lengthof(SysAtt))
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errmsg("invalid system attribute number %d", attno)));
    if (attno == ObjectIdAttributeNumber && !relhasoids)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errmsg("invalid system attribute number %d", attno)));
    if (attno == BucketIdAttributeNumber && !relhasbucket)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errmsg("invalid system attribute number %d", attno)));
    if (attno == UidAttributeNumber && !relhasuids) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errmsg("invalid system attribute number %d", attno)));
    }
    return SysAtt[-attno - 1];
}

/*
 * If the given name is a system attribute name, return a Form_pg_attribute
 * pointer for a prototype definition.	If not, return NULL.
 */
Form_pg_attribute SystemAttributeByName(const char* attname, bool relhasoids)
{
    int j;

    for (j = 0; j < (int)lengthof(SysAtt); j++) {
        Form_pg_attribute att = SysAtt[j];

        if (relhasoids || att->attnum != ObjectIdAttributeNumber) {
            if (strcmp(NameStr(att->attname), attname) == 0)
                return att;
        }
    }

    return NULL;
}

int GetSysAttLength(bool hasBucketAttr)
{
    if (hasBucketAttr) {
        return lengthof(SysAtt);
    } else {
        return lengthof(SysAtt) - 1;
    }

}

static void InitPartitionDef(Partition newPartition, Oid partOid, char strategy)
{
    newPartition->pd_part->parttype = PART_OBJ_TYPE_TABLE_PARTITION;
    newPartition->pd_part->parentid = partOid;
    newPartition->pd_part->rangenum = 0;
    newPartition->pd_part->intervalnum = 0;
    newPartition->pd_part->partstrategy = strategy;
    newPartition->pd_part->reltoastrelid = InvalidOid;
    newPartition->pd_part->reltoastidxid = InvalidOid;
    newPartition->pd_part->indextblid = InvalidOid;
    newPartition->pd_part->reldeltarelid = InvalidOid;
    newPartition->pd_part->reldeltaidx = InvalidOid;
    newPartition->pd_part->relcudescrelid = InvalidOid;
    newPartition->pd_part->relcudescidx = InvalidOid;
    newPartition->pd_part->indisusable = true;
}

static void InitSubPartitionDef(Partition newPartition, Oid partOid, char strategy)
{
    newPartition->pd_part->parttype = PART_OBJ_TYPE_TABLE_SUB_PARTITION;
    newPartition->pd_part->parentid = partOid;
    newPartition->pd_part->rangenum = 0;
    newPartition->pd_part->intervalnum = 0;
    newPartition->pd_part->partstrategy = strategy;
    newPartition->pd_part->reltoastrelid = InvalidOid;
    newPartition->pd_part->reltoastidxid = InvalidOid;
    newPartition->pd_part->indextblid = InvalidOid;
    newPartition->pd_part->reldeltarelid = InvalidOid;
    newPartition->pd_part->reldeltaidx = InvalidOid;
    newPartition->pd_part->relcudescrelid = InvalidOid;
    newPartition->pd_part->relcudescidx = InvalidOid;
    newPartition->pd_part->indisusable = true;
}

/* ----------------------------------------------------------------
 *				XXX END OF UGLY HARD CODED BADNESS XXX
 * ---------------------------------------------------------------- */

/* ----------------------------------------------------------------
 *		heap_create		- Create an uncataloged heap relation
 *
 *		Note API change: the caller must now always provide the OID
 *		to use for the relation.  The relfilenode may (and, normally,
 *		should) be left unspecified.
 *
 *		rel->rd_rel is initialized by RelationBuildLocalRelation,
 *		and is mostly zeroes at return.
 * ----------------------------------------------------------------
 */
Relation heap_create(const char* relname, Oid relnamespace, Oid reltablespace, Oid relid, Oid relfilenode,
    Oid bucketOid, TupleDesc tupDesc, char relkind, char relpersistence, bool partitioned_relation, bool rowMovement,
    bool shared_relation, bool mapped_relation, bool allow_system_table_mods, int8 row_compress, Datum reloptions,
    Oid ownerid, bool skip_create_storage, TableAmType tam_type, int8 relindexsplit, StorageType storage_type,
    bool newcbi, Oid accessMethodObjectId)
{
    bool create_storage = false;
    Relation rel;
    bool isbucket = false;

    /* The caller must have provided an OID for the relation. */
    Assert(OidIsValid(relid));

    /*
     * sanity checks
     */
    if (!allow_system_table_mods && 
        (IsSystemNamespace(relnamespace) || IsToastNamespace(relnamespace) || IsCStoreNamespace(relnamespace) || 
        IsPackageSchemaOid(relnamespace)) && IsNormalProcessingMode()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create \"%s.%s\"", get_namespace_name(relnamespace), relname),
                errdetail("System catalog modifications are currently disallowed.")));
    }
    /*
     * Decide if we need storage or not, and handle a couple other special
     * cases for particular relkinds.
     */
    switch (relkind) {
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
        case RELKIND_COMPOSITE_TYPE:
        case RELKIND_STREAM:
        case RELKIND_FOREIGN_TABLE:
            create_storage = false;

            /*
             * Force reltablespace to zero if the relation has no physical
             * storage.  This is mainly just for cleanliness' sake.
             */
            reltablespace = InvalidOid;
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            create_storage = true;

            /*
             * Force reltablespace to zero for sequences, since we don't
             * support moving them around into different tablespaces.
             */
            reltablespace = InvalidOid;
            break;
        case RELKIND_GLOBAL_INDEX:
            create_storage = true;
            break;
        default:
            if (!partitioned_relation) {
                create_storage = true;
            } else {
                create_storage = false;
            }
            break;
    }
    
    /*
     * Never allow a pg_class entry to explicitly specify the database's
     * default tablespace in reltablespace; force it to zero instead. This
     * ensures that if the database is cloned with a different default
     * tablespace, the pg_class entry will still match where CREATE DATABASE
     * will put the physically copied relation.
     *
     * Yes, this is a bit of a hack.
     */
    reltablespace = ConvertToPgclassRelTablespaceOid(reltablespace);

    /*
     * Unless otherwise requested, the physical ID (relfilenode) is initially
     * the same as the logical ID (OID).  When the caller did specify a
     * relfilenode, it already exists; do not attempt to create it.
     */
    if (OidIsValid(relfilenode)) {
        if (u_sess->proc_cxt.IsBinaryUpgrade) {
            if (!partitioned_relation && storage_type == SEGMENT_PAGE) {
                isbucket = BUCKET_OID_IS_VALID(bucketOid) && !newcbi;
                relfilenode = seg_alloc_segment(ConvertToRelfilenodeTblspcOid(reltablespace), 
                                                u_sess->proc_cxt.MyDatabaseId, isbucket, relfilenode);
            }
        } else {
            create_storage = false;
        }
    } else if (storage_type == SEGMENT_PAGE && !partitioned_relation) {
        Assert(reltablespace != GLOBALTABLESPACE_OID);
        isbucket = BUCKET_OID_IS_VALID(bucketOid) && !newcbi;
        relfilenode = (Oid)seg_alloc_segment(ConvertToRelfilenodeTblspcOid(reltablespace), 
                                             u_sess->proc_cxt.MyDatabaseId, isbucket, InvalidBlockNumber);
        ereport(LOG, (errmsg("Segment Relation %s(%u) set relfilenode %u xid %lu", relname, relid, relfilenode,
            GetCurrentTransactionIdIfAny())));
    } else {
        relfilenode = relid;
    }

    /*
     * build the relcache entry.
     */
    rel = RelationBuildLocalRelation(relname,
        relnamespace,
        tupDesc,
        relid,
        relfilenode,
        reltablespace,
        shared_relation,
        mapped_relation,
        relpersistence,
        relkind,
        row_compress,
        reloptions,
        tam_type,
        relindexsplit,
        storage_type,
        accessMethodObjectId
    );

    if (partitioned_relation) {
        rel->rd_rel->parttype = PARTTYPE_PARTITIONED_RELATION;
    }
    rel->rd_rel->relrowmovement = rowMovement;

    /*
     * Save newcbi as a context indicator to 
     * avoid missing information in later index building process.
     */
    rel->newcbi = newcbi;

    if (u_sess->attr.attr_common.IsInplaceUpgrade && !u_sess->upg_cxt.new_catalog_need_storage)
        create_storage = false;

    if (skip_create_storage) {
        create_storage = false;
    }
    /*
     * Have the storage manager create the relation's disk file, if needed.
     *
     * We only create the main fork here, other forks will be created on
     * demand.
     */
    if (create_storage) {
        rel->rd_bucketoid = bucketOid;
        RelationOpenSmgr(rel);
        RelationCreateStorage(rel->rd_node, relpersistence, ownerid, bucketOid, rel);
    }

    if (RelationUsesSpaceType(rel->rd_rel->relpersistence) == SP_TEMP) {
        make_tmptable_cache_key(rel->rd_rel->relfilenode);
    }

    return rel;
}

/* ----------------------------------------------------------------
 *		heap_create_with_catalog		- Create a cataloged relation
 *
 *		this is done in multiple steps:
 *
 *		1) CheckAttributeNamesTypes() is used to make certain the tuple
 *		   descriptor contains a valid set of attribute names and types
 *
 *		2) pg_class is opened and get_relname_relid()
 *		   performs a scan to ensure that no relation with the
 *		   same name already exists.
 *
 *		3) heap_create() is called to create the new relation on disk.
 *
 *		4) TypeCreate() is called to define a new type corresponding
 *		   to the new relation.
 *
 *		5) AddNewRelationTuple() is called to register the
 *		   relation in pg_class.
 *
 *		6) AddNewAttributeTuples() is called to register the
 *		   new relation's schema in pg_attribute.
 *
 *		7) StoreConstraints is called ()		- vadim 08/22/97
 *
 *		8) the relations are closed and the new relation's oid
 *		   is returned.
 *
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *		CheckAttributeNamesTypes
 *
 *		this is used to make certain the tuple descriptor contains a
 *		valid set of attribute names and datatypes.  a problem simply
 *		generates ereport(ERROR) which aborts the current transaction.
 * --------------------------------
 */
void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind, bool allow_system_table_mods)
{
    int i;
    int j;
    int natts = tupdesc->natts;

    /* Sanity check on column count */
    if (natts < 0 || natts > MaxHeapAttributeNumber)
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("tables can have at most %d columns", MaxHeapAttributeNumber)));

    /*
     * first check for collision with system attribute names
     *
     * Skip this for a view or type relation, since those don't have system
     * attributes.
     */
    if (relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE && relkind != RELKIND_CONTQUERY) {
        for (i = 0; i < natts; i++) {
            if (SystemAttributeByName(NameStr(tupdesc->attrs[i]->attname), tupdesc->tdhasoid) != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_COLUMN),
                        errmsg("column name \"%s\" conflicts with a system column name",
                            NameStr(tupdesc->attrs[i]->attname))));
        }
    }

    /*
     * next check for repeated attribute names
     */
    for (i = 1; i < natts; i++) {
        for (j = 0; j < i; j++) {
            if (strcmp(NameStr(tupdesc->attrs[j]->attname), NameStr(tupdesc->attrs[i]->attname)) == 0)
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_COLUMN),
                        errmsg("column name \"%s\" specified more than once", NameStr(tupdesc->attrs[j]->attname))));
        }
    }

    /*
     * next check the attribute types
     */
    for (i = 0; i < natts; i++) {
        CheckAttributeType(NameStr(tupdesc->attrs[i]->attname),
            tupdesc->attrs[i]->atttypid,
            tupdesc->attrs[i]->attcollation,
            NIL, /* assume we're creating a new rowtype */
            allow_system_table_mods);
    }
}

/* --------------------------------
 *		CheckAttributeType
 *
 *		Verify that the proposed datatype of an attribute is legal.
 *		This is needed mainly because there are types (and pseudo-types)
 *		in the catalogs that we do not support as elements of real tuples.
 *		We also check some other properties required of a table column.
 *
 * If the attribute is being proposed for addition to an existing table or
 * composite type, pass a one-element list of the rowtype OID as
 * containing_rowtypes.  When checking a to-be-created rowtype, it's
 * sufficient to pass NIL, because there could not be any recursive reference
 * to a not-yet-existing rowtype.
 * --------------------------------
 */
void CheckAttributeType(
    const char* attname, Oid atttypid, Oid attcollation, List* containing_rowtypes, bool allow_system_table_mods)
{
    char att_typtype = get_typtype(atttypid);
    Oid att_typelem;

    if (atttypid == UNKNOWNOID) {
        /*
         * Warn user, but don't fail, if column to be created has UNKNOWN type
         * (usually as a result of a 'retrieve into' - jolly)
         */
        ereport(WARNING,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("column \"%s\" has type \"unknown\"", attname),
                errdetail("Proceeding with relation creation anyway.")));
    } else if (att_typtype == TYPTYPE_PSEUDO) {
        /*
         * Refuse any attempt to create a pseudo-type column, except for a
         * special hack for pg_statistic: allow ANYARRAY when modifying system
         * catalogs (this allows creating pg_statistic and cloning it during
         * VACUUM FULL)
         */
        if (atttypid != ANYARRAYOID || !allow_system_table_mods) {
#ifndef ENABLE_MULTIPLE_NODES
            if (u_sess->attr.attr_common.plsql_show_all_error) {
                StringInfoData message;
                initStringInfo(&message);
                appendStringInfo(&message, "column \"%s\" has pseudo-type %s", attname, format_type_be(atttypid));
                InsertErrorMessage(message.data, 0, true);
            }
#endif
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("column \"%s\" has pseudo-type %s", attname, format_type_be(atttypid))));
        }
    } else if (att_typtype == TYPTYPE_DOMAIN) {
        /*
         * If it's a domain, recurse to check its base type.
         */
        CheckAttributeType(attname, getBaseType(atttypid), attcollation, containing_rowtypes, allow_system_table_mods);
    } else if (att_typtype == TYPTYPE_COMPOSITE) {
        /*
         * For a composite type, recurse into its attributes.
         */
        Relation relation;
        TupleDesc tupdesc;
        int i;

        /*
         * Check for self-containment.	Eventually we might be able to allow
         * this (just return without complaint, if so) but it's not clear how
         * many other places would require anti-recursion defenses before it
         * would be safe to allow tables to contain their own rowtype.
         */
        if (list_member_oid(containing_rowtypes, atttypid)) {
            if (strcmp(attname, "pljson_list_data") == 0) {
                return;
            }
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("composite type %s cannot be made a member of itself", format_type_be(atttypid))));
        }

        containing_rowtypes = lcons_oid(atttypid, containing_rowtypes);

        relation = relation_open(get_typ_typrelid(atttypid), AccessShareLock);

        tupdesc = RelationGetDescr(relation);

        for (i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = tupdesc->attrs[i];

            if (attr->attisdropped)
                continue;
            CheckAttributeType(NameStr(attr->attname),
                attr->atttypid,
                attr->attcollation,
                containing_rowtypes,
                allow_system_table_mods);
        }

        relation_close(relation, AccessShareLock);

        containing_rowtypes = list_delete_first(containing_rowtypes);
    } else if (att_typtype == TYPTYPE_TABLEOF) {
        /*
         * For a table of type, find its base elemid and collation
         */
        att_typelem = get_element_type(atttypid);
        Oid att_collation = get_typcollation(att_typelem);
        CheckAttributeType(attname, att_typelem, att_collation, containing_rowtypes, allow_system_table_mods);
    } else if (OidIsValid((att_typelem = get_element_type(atttypid)))) {
        /*
         * Must recurse into array types, too, in case they are composite.
         */
        CheckAttributeType(attname, att_typelem, attcollation, containing_rowtypes, allow_system_table_mods);
    }

    /*
     * This might not be strictly invalid per SQL standard, but it is pretty
     * useless, and it cannot be dumped, so we must disallow it.
     */
    if (!OidIsValid(attcollation) && type_is_collatable(atttypid))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("no collation was derived for column \"%s\" with collatable type %s",
                    attname,
                    format_type_be(atttypid)),
                errhint("Use the COLLATE clause to set the collation explicitly.")));
}

void InsertTablebucketidAttribute(Oid newRelOid)
{
    CatalogIndexState indstate;
    Relation rel;
    int tablebucketidIndex = -BucketIdAttributeNumber - 1;
    /*
     * open pg_attribute and its indexes.
     */
    rel = heap_open(AttributeRelationId, RowExclusiveLock);
    indstate = CatalogOpenIndexes(rel);

    FormData_pg_attribute attStruct;
    errno_t rc = memcpy_s(&attStruct, sizeof(FormData_pg_attribute), (char*)SysAtt[tablebucketidIndex],
        sizeof(FormData_pg_attribute));
    securec_check(rc, "\0", "\0");
    attStruct.attrelid = newRelOid;

    InsertPgAttributeTuple(rel, &attStruct, indstate);
    CatalogCloseIndexes(indstate);
    heap_close(rel, RowExclusiveLock);
}

/*
 * InsertPgAttributeTuple
 *		Construct and insert a new tuple in pg_attribute.
 *
 * Caller has already opened and locked pg_attribute.  new_attribute is the
 * attribute to insert (but we ignore attacl and attoptions, which are always
 * initialized to NULL).
 *
 * indstate is the index state for CatalogIndexInsert.	It can be passed as
 * NULL, in which case we'll fetch the necessary info.  (Don't do this when
 * inserting multiple attributes, because it's a tad more expensive.)
 */
void InsertPgAttributeTuple(Relation pg_attribute_rel, Form_pg_attribute new_attribute, CatalogIndexState indstate)
{
    Datum values[Natts_pg_attribute];
    bool nulls[Natts_pg_attribute];
    HeapTuple tup;
    errno_t rc;
    /* This is a tad tedious, but way cleaner than what we used to do... */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_attribute_attrelid - 1] = ObjectIdGetDatum(new_attribute->attrelid);
    values[Anum_pg_attribute_attname - 1] = NameGetDatum(&new_attribute->attname);
    values[Anum_pg_attribute_atttypid - 1] = ObjectIdGetDatum(new_attribute->atttypid);
    values[Anum_pg_attribute_attstattarget - 1] = Int32GetDatum(new_attribute->attstattarget);
    values[Anum_pg_attribute_attlen - 1] = Int16GetDatum(new_attribute->attlen);
    values[Anum_pg_attribute_attnum - 1] = Int16GetDatum(new_attribute->attnum);
    values[Anum_pg_attribute_attndims - 1] = Int32GetDatum(new_attribute->attndims);
    values[Anum_pg_attribute_attcacheoff - 1] = Int32GetDatum(new_attribute->attcacheoff);
    values[Anum_pg_attribute_atttypmod - 1] = Int32GetDatum(new_attribute->atttypmod);
    values[Anum_pg_attribute_attbyval - 1] = BoolGetDatum(new_attribute->attbyval);
    values[Anum_pg_attribute_attstorage - 1] = CharGetDatum(new_attribute->attstorage);
    values[Anum_pg_attribute_attalign - 1] = CharGetDatum(new_attribute->attalign);
    values[Anum_pg_attribute_attnotnull - 1] = BoolGetDatum(new_attribute->attnotnull);
    values[Anum_pg_attribute_atthasdef - 1] = BoolGetDatum(new_attribute->atthasdef);
    values[Anum_pg_attribute_attisdropped - 1] = BoolGetDatum(new_attribute->attisdropped);
    values[Anum_pg_attribute_attislocal - 1] = BoolGetDatum(new_attribute->attislocal);
    values[Anum_pg_attribute_attcmprmode - 1] = Int8GetDatum(new_attribute->attcmprmode);
    values[Anum_pg_attribute_attinhcount - 1] = Int32GetDatum(new_attribute->attinhcount);
    values[Anum_pg_attribute_attcollation - 1] = ObjectIdGetDatum(new_attribute->attcollation);
    values[Anum_pg_attribute_attkvtype - 1] = Int8GetDatum(new_attribute->attkvtype);

    /* start out with empty permissions and empty options */
    nulls[Anum_pg_attribute_attacl - 1] = true;
    nulls[Anum_pg_attribute_attoptions - 1] = true;
    nulls[Anum_pg_attribute_attfdwoptions - 1] = true;

    /* at default, new fileld attinitdefval of pg_attribute is null. */
    nulls[Anum_pg_attribute_attinitdefval - 1] = true;

    tup = heap_form_tuple(RelationGetDescr(pg_attribute_rel), values, nulls);

    /* finally insert the new tuple, update the indexes, and clean up */
    (void)simple_heap_insert(pg_attribute_rel, tup);

    if (indstate != NULL)
        CatalogIndexInsert(indstate, tup);
    else
        CatalogUpdateIndexes(pg_attribute_rel, tup);

    heap_freetuple(tup);
}

/* --------------------------------
 *		AddNewAttributeTuples
 *
 *		this registers the new relation's schema by adding
 *		tuples to pg_attribute.
 * --------------------------------
 */
static void AddNewAttributeTuples(Oid new_rel_oid, TupleDesc tupdesc, char relkind, bool oidislocal, int oidinhcount, bool hasbucket, bool hasuids)
{
    Form_pg_attribute attr;
    int i;
    Relation rel;
    CatalogIndexState indstate;
    int natts = tupdesc->natts;
    ObjectAddress myself, referenced;

    /*
     * open pg_attribute and its indexes.
     */
    rel = heap_open(AttributeRelationId, RowExclusiveLock);

    indstate = CatalogOpenIndexes(rel);

    /*
     * First we add the user attributes.  This is also a convenient place to
     * add dependencies on their datatypes and collations.
     */
    for (i = 0; i < natts; i++) {
        attr = tupdesc->attrs[i];
        /* Fill in the correct relation OID */
        attr->attrelid = new_rel_oid;
        /* Make sure these are OK, too */
        attr->attstattarget = -1;
        attr->attcacheoff = -1;

        InsertPgAttributeTuple(rel, attr, indstate);

        /*
         * During inplace/grey upgrade, we don't record dependencies
         * for new catalog attributes.  Also if the column is dropped
         * no need to record dependency anymore
         */
        if (!u_sess->attr.attr_common.IsInplaceUpgrade && !attr->attisdropped) {
            /* Add dependency info */
            myself.classId = RelationRelationId;
            myself.objectId = new_rel_oid;
            myself.objectSubId = i + 1;
            referenced.classId = TypeRelationId;
            referenced.objectId = attr->atttypid;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

            /* The default collation is pinned, so don't bother recording it */
            if (OidIsValid(attr->attcollation) && attr->attcollation != DEFAULT_COLLATION_OID) {
                referenced.classId = CollationRelationId;
                referenced.objectId = attr->attcollation;
                referenced.objectSubId = 0;
                recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
            }
        }
    }

    /*
     * Next we add the system attributes.  Skip OID if rel has no OIDs. Skip
     * all for a view or type relation.  We don't bother with making datatype
     * dependencies here, since presumably all these types are pinned.
     */
    if (relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE && relkind != RELKIND_CONTQUERY) {
        for (i = 0; i < (int)lengthof(SysAtt); i++) {
            FormData_pg_attribute attStruct;
            errno_t rc;
            /* skip OID where appropriate */
            if (!tupdesc->tdhasoid && SysAtt[i]->attnum == ObjectIdAttributeNumber)
                continue;
            if (!hasbucket && SysAtt[i]->attnum == BucketIdAttributeNumber)
                continue;
            if (!hasuids && SysAtt[i]->attnum == UidAttributeNumber)
                continue;
            rc = memcpy_s(&attStruct, sizeof(FormData_pg_attribute), (char*)SysAtt[i], sizeof(FormData_pg_attribute));
            securec_check(rc, "\0", "\0");

            /* Fill in the correct relation OID in the copied tuple */
            attStruct.attrelid = new_rel_oid;

            /* Fill in correct inheritance info for the OID column */
            if (attStruct.attnum == ObjectIdAttributeNumber) {
                attStruct.attislocal = oidislocal;
                attStruct.attinhcount = oidinhcount;
            }

            InsertPgAttributeTuple(rel, &attStruct, indstate);
        }
    }

    /*
     * clean up
     */
    CatalogCloseIndexes(indstate);

    heap_close(rel, RowExclusiveLock);
}

static void AddNewGsSecEncryptedColumnsTuples(const Oid new_rel_oid, List *ceLst)
{
    Relation rel;
    CatalogIndexState indstate;
    /*
     * open gs_encrypted_columns and its indexes.
     */
    rel = heap_open(ClientLogicCachedColumnsId, RowExclusiveLock);

    indstate = CatalogOpenIndexes(rel);
    ListCell *li = NULL;
    foreach (li, ceLst) {
        CeHeapInfo *ceHeapInfo = (CeHeapInfo *)lfirst(li);
        insert_gs_sec_encrypted_column_tuple(ceHeapInfo, rel, new_rel_oid, indstate);
    }
    /*
     * clean up
     */
    CatalogCloseIndexes(indstate);
    heap_close(rel, RowExclusiveLock);
}

/* --------------------------------
 *		InsertPgClassTuple
 *
 *		Construct and insert a new tuple in pg_class.
 *
 * Caller has already opened and locked pg_class.
 * Tuple data is taken from new_rel_desc->rd_rel, except for the
 * variable-width fields which are not present in a cached reldesc.
 * relacl and reloptions are passed in Datum form (to avoid having
 * to reference the data types in heap.h).	Pass (Datum) 0 to set them
 * to NULL.
 * --------------------------------
 */
void InsertPgClassTuple(
    Relation pg_class_desc, Relation new_rel_desc, Oid new_rel_oid, Datum relacl, Datum reloptions, char relkind, int2vector* bucketcol)
{
    Form_pg_class rd_rel = new_rel_desc->rd_rel;
    Datum values[Natts_pg_class];
    bool nulls[Natts_pg_class];
    HeapTuple tup;
    errno_t rc;

    /* This is a tad tedious, but way cleaner than what we used to do... */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_class_relname - 1] = NameGetDatum(&rd_rel->relname);
    values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(rd_rel->relnamespace);
    values[Anum_pg_class_reltype - 1] = ObjectIdGetDatum(rd_rel->reltype);
    values[Anum_pg_class_reloftype - 1] = ObjectIdGetDatum(rd_rel->reloftype);
    values[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(rd_rel->relowner);
    values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(rd_rel->relam);
    values[Anum_pg_class_relfilenode - 1] = ObjectIdGetDatum(rd_rel->relfilenode);
    values[Anum_pg_class_reltablespace - 1] = ObjectIdGetDatum(rd_rel->reltablespace);
    values[Anum_pg_class_relpages - 1] = Float8GetDatum(rd_rel->relpages);
    values[Anum_pg_class_reltuples - 1] = Float8GetDatum(rd_rel->reltuples);
    values[Anum_pg_class_relallvisible - 1] = Int32GetDatum(rd_rel->relallvisible);
    values[Anum_pg_class_reltoastrelid - 1] = ObjectIdGetDatum(rd_rel->reltoastrelid);
    values[Anum_pg_class_reltoastidxid - 1] = ObjectIdGetDatum(rd_rel->reltoastidxid);
    values[Anum_pg_class_reldeltarelid - 1] = ObjectIdGetDatum(rd_rel->reldeltarelid);
    values[Anum_pg_class_reldeltaidx - 1] = ObjectIdGetDatum(rd_rel->reldeltaidx);
    values[Anum_pg_class_relcudescrelid - 1] = ObjectIdGetDatum(rd_rel->relcudescrelid);
    values[Anum_pg_class_relcudescidx - 1] = ObjectIdGetDatum(rd_rel->relcudescidx);
    values[Anum_pg_class_relhasindex - 1] = BoolGetDatum(rd_rel->relhasindex);
    values[Anum_pg_class_relisshared - 1] = BoolGetDatum(rd_rel->relisshared);
    values[Anum_pg_class_relpersistence - 1] = CharGetDatum(rd_rel->relpersistence);
    values[Anum_pg_class_relkind - 1] = CharGetDatum(rd_rel->relkind);
    values[Anum_pg_class_relnatts - 1] = Int16GetDatum(rd_rel->relnatts);
    values[Anum_pg_class_relchecks - 1] = Int16GetDatum(rd_rel->relchecks);
    values[Anum_pg_class_relhasoids - 1] = BoolGetDatum(rd_rel->relhasoids);
    values[Anum_pg_class_relhaspkey - 1] = BoolGetDatum(rd_rel->relhaspkey);
    values[Anum_pg_class_relhasrules - 1] = BoolGetDatum(rd_rel->relhasrules);
    values[Anum_pg_class_relhastriggers - 1] = BoolGetDatum(rd_rel->relhastriggers);
    values[Anum_pg_class_relhassubclass - 1] = BoolGetDatum(rd_rel->relhassubclass);
    values[ANUM_PG_CLASS_RELCMPRS - 1] = CharGetDatum(rd_rel->relcmprs);
    values[Anum_pg_class_relhasclusterkey - 1] = BoolGetDatum(rd_rel->relhasclusterkey);
    values[Anum_pg_clsss_relrowmovement - 1] = BoolGetDatum(rd_rel->relrowmovement);
    values[Anum_pg_class_parttype - 1] = CharGetDatum(rd_rel->parttype);
    values[Anum_pg_class_relfrozenxid - 1] = ShortTransactionIdGetDatum(rd_rel->relfrozenxid);

    if (relacl != (Datum)0)
        values[Anum_pg_class_relacl - 1] = relacl;
    else
        nulls[Anum_pg_class_relacl - 1] = true;
    if (reloptions != (Datum)0)
        values[Anum_pg_class_reloptions - 1] = reloptions;
    else
        nulls[Anum_pg_class_reloptions - 1] = true;

    if (RELKIND_IN_RTM)
        values[Anum_pg_class_relfrozenxid64 - 1] = u_sess->utils_cxt.RecentXmin;
    else
        values[Anum_pg_class_relfrozenxid64 - 1] = InvalidTransactionId;

    if (!IsSystemNamespace(rd_rel->relnamespace) && rd_rel->relkind == RELKIND_RELATION)
        values[Anum_pg_class_relreplident - 1] = REPLICA_IDENTITY_DEFAULT;
    else
        values[Anum_pg_class_relreplident - 1] = REPLICA_IDENTITY_NOTHING;

    if (OidIsValid(new_rel_desc->rd_bucketoid)) {
        Assert(new_rel_desc->storage_type == SEGMENT_PAGE);
        values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(new_rel_desc->rd_bucketoid);
    } else if (new_rel_desc->storage_type == SEGMENT_PAGE) {
        values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(VirtualSegmentOid);
    } else {
        nulls[Anum_pg_class_relbucket - 1] = true;
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (RELKIND_IN_RTM && !is_cstore_option(relkind, reloptions)) {
        values[Anum_pg_class_relminmxid - 1] = GetOldestMultiXactId();
    } else {
        values[Anum_pg_class_relminmxid - 1] = InvalidMultiXactId;
    }
#endif
    
    if (bucketcol != NULL)
        values[Anum_pg_class_relbucketkey - 1] = PointerGetDatum(bucketcol);
    else
        nulls[Anum_pg_class_relbucketkey - 1] = true;

    tup = heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls);

    /*
     * The new tuple must have the oid already chosen for the rel.	Sure would
     * be embarrassing to do this sort of thing in polite company.
     */
    HeapTupleSetOid(tup, new_rel_oid);

    /* finally insert the new tuple, update the indexes, and clean up */
    (void)simple_heap_insert(pg_class_desc, tup);

    CatalogUpdateIndexes(pg_class_desc, tup);

    heap_freetuple(tup);
}

/* --------------------------------
 *		AddNewRelationTuple
 *
 *		this registers the new relation in the catalogs by
 *		adding a tuple to pg_class.
 * --------------------------------
 */
static void AddNewRelationTuple(Relation pg_class_desc, Relation new_rel_desc, Oid new_rel_oid, Oid new_type_oid,
    Oid reloftype, Oid relowner, char relkind, char relpersistence, Datum relacl, Datum reloptions,
    int2vector* bucketcol, bool ispartrel)
{
    Form_pg_class new_rel_reltup;

    /*
     * first we update some of the information in our uncataloged relation's
     * relation descriptor.
     */
    new_rel_reltup = new_rel_desc->rd_rel;

    switch (relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_TOASTVALUE:
        case RELKIND_GLOBAL_INDEX:
            /* The relation is real, but as yet empty */
            new_rel_reltup->relpages = 0;
            new_rel_reltup->reltuples = 0;
            new_rel_reltup->relallvisible = 0;
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            /* Sequences always have a known size */
            new_rel_reltup->relpages = 1;
            new_rel_reltup->reltuples = 1;
            new_rel_reltup->relallvisible = 0;
            break;
        default:
            /* Views, etc, have no disk storage */
            new_rel_reltup->relpages = 0;
            new_rel_reltup->reltuples = 0;
            new_rel_reltup->relallvisible = 0;
            break;
    }
    
    /* Initialize relfrozenxid */
    if (RELKIND_IN_RTM) {
        /*
         * Initialize to the minimum XID that could put tuples in the table.
         * We know that no xacts older than RecentXmin are still running, so
         * that will do.
         */
        new_rel_reltup->relfrozenxid = (ShortTransactionId)u_sess->utils_cxt.RecentXmin;
    } else {
        /*
         * Other relation types will not contain XIDs, so set relfrozenxid to
         * InvalidTransactionId.  (Note: a sequence does contain a tuple, but
         * we force its xmin to be FrozenTransactionId always; see
         * commands/sequence.c.)
         */
        new_rel_reltup->relfrozenxid = (ShortTransactionId)InvalidTransactionId;
    }

    /* global temp table not remember transaction info in catalog */
    if (relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        new_rel_reltup->relfrozenxid = (ShortTransactionId)InvalidTransactionId;
    }

    new_rel_reltup->relowner = relowner;
    new_rel_reltup->reltype = new_type_oid;
    new_rel_reltup->reloftype = reloftype;

    new_rel_desc->rd_att->tdtypeid = new_type_oid;

    /* Now build and insert the tuple */
    InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, relacl, reloptions, relkind, bucketcol);
}

#ifdef PGXC

/* --------------------------------
 *		cmp_nodes
 *
 *		Compare the Oids of two XC nodes
 *		to sort them in ascending order by their names
 * --------------------------------
 */
static int cmp_nodes(const void* p1, const void* p2)
{
    NameData node1 = {{0}};
    NameData node2 = {{0}};

    return strcmp(get_pgxc_nodename(*(Oid*)p1, &node1), get_pgxc_nodename(*(Oid*)p2, &node2));
}

static void CheckDistColsUnique(DistributeBy *distribBy, int distribKeyNum, int2 *attnum)
{
    if (distribBy == NULL) {
        return;
    }
    
    /* Check whether include identical column in distribute key */
    for (int i = 0; i < distribKeyNum - 1; i++) {
        for (int j = i + 1; j < distribKeyNum; j++) {
            if (attnum[i] == attnum[j]) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("Include identical distribution column \"%s\"",
                            strVal(list_nth(distribBy->colname, i)))));
            }
        }
    }
}

static List* GetDistColsPos(DistributeBy* distributeBy, TupleDesc desc)
{
    int i;
    List* pos = NULL;
    ListCell* cell = NULL;
    char* colname = NULL;
    Form_pg_attribute *attrs = desc->attrs;

    foreach (cell, distributeBy->colname) {
        colname = strVal((Value*)lfirst(cell));

        for (i = 0; i < desc->natts; i++) {
            if (strcmp(colname, attrs[i]->attname.data) == 0) {
                break;
            }
        }

        Assert(i < desc->natts);
        pos = lappend_int(pos, i);
    }

    return pos;
}


static void CheckListDistEntry(List* entry, int distkeynum, bool isDefault)
{
    int boundaryLength = list_length(entry);
    if (boundaryLength != distkeynum && !isDefault) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("Distribution boundary length (%d) does not equal to the number of distribute keys (%d).", 
                boundaryLength, distkeynum)));
    }
}

/*
 * Get the List Distribution entry's items in string format,
 * the return value should be freed by caller.
 */
static char* DistEntryToString(List* entry)
{
    Oid typoutput;
    bool typIsVarlena = false;
    char* outputstr = NULL;
    ListCell* cell = NULL;
    StringInfoData string;

    initStringInfo(&string);
    appendStringInfoChar(&string, '\'');

    foreach (cell, entry) {
        Const* element = (Const *)lfirst(cell);

        if (cell != list_head(entry)) {
            appendStringInfoChar(&string, ',');
        }

        if (element->constisnull) {
            appendStringInfoString(&string, "NULL");
        } else if (element->ismaxvalue) {
            appendStringInfoString(&string, "MAXVALUE");
        } else {
            getTypeOutputInfo(element->consttype, &typoutput, &typIsVarlena);
            outputstr = OidOutputFunctionCall(typoutput, element->constvalue);
            appendStringInfo(&string, "%s", outputstr);
        }
    }

    appendStringInfoChar(&string, '\'');
    return string.data;
}

/* Check for default within one list slice */
static bool PreCheckListSlice(ListSliceDefState* slicedef)
{
    List* boundary = NULL;

    List* boundaries = NULL;
    ListCell* cell = NULL;
    ListCell* next = NULL;
    Const* firstConst = NULL;

    boundaries = slicedef->boundaries;
    foreach (cell, boundaries) {
        next = cell;
        boundary = (List*)lfirst(cell);

        /* skip checking current slice if it's DEFAULT slice */
        firstConst = linitial_node(Const, boundary);
        if (firstConst->ismaxvalue) {
            return true;
        }
    }

    return false;
}

static int ConstCmp(const void* a, const void* b)
{
    const SliceConstInfo* rea = (const SliceConstInfo*)a;
    const SliceConstInfo* reb = (const SliceConstInfo*)b;
    return partitonKeyCompare((Const**)rea->sliceBoundaryValue, (Const**)reb->sliceBoundaryValue, rea->sliceNum);
}

static void IsDuplicateBoundariesExist(SliceConstInfo* sliceConstInfo, int totalBoundariesNum, int distkeynum) 
{
    /* sort all the boundaries values */
    qsort(sliceConstInfo, totalBoundariesNum, sizeof(SliceConstInfo), ConstCmp);
    /* if duplicate boundaries exist,report and exit */
    for (int i = 0; i < totalBoundariesNum - 1; i++) {
        int cmp = partitonKeyCompare(sliceConstInfo[i].sliceBoundaryValue, 
            sliceConstInfo[i + 1].sliceBoundaryValue, distkeynum);
        if (cmp == 0) {
            if (strcmp(sliceConstInfo[i].sliceName, sliceConstInfo[i + 1].sliceName) == 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("List value %s specified multiple times in slice %s.",
                            DistEntryToString(sliceConstInfo[i].sliceBoundary),
                            sliceConstInfo[i].sliceName)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("List value %s specified multiple times in slices %s and %s.",
                            DistEntryToString(sliceConstInfo[i].sliceBoundary),
                            sliceConstInfo[i].sliceName,
                            sliceConstInfo[i + 1].sliceName)));
            }
        }
    }
}

static int GetTotalBoundariesNum(List* sliceList) 
{
    int result = 0;
    ListSliceDefState* slicedef = NULL;
    List* boundaryList = NULL;
    foreach_cell(sliceCell, sliceList) {
        slicedef = (ListSliceDefState*)lfirst(sliceCell);
        boundaryList = slicedef->boundaries;
        result += boundaryList->length;
    }
    return result;
}

static void CheckDuplicateListSlices(List* pos, Form_pg_attribute* attrs, DistributeBy *distby)
{
    List* boundary = NULL;
    List* sliceList = NULL;
    ListSliceDefState* slicedef = NULL;
    List* boundaryList = NULL;
    int sliceCount = 0;
    int totalBoundariesNum = 0;
    bool is_intreval = false;
    SliceConstInfo* sliceConstInfo = NULL;
    int distkeynum = list_length(distby->colname);
    List* constsList = NULL;

    if (pos == NULL || attrs == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("invalid list distribution table definition")));
    }

    sliceList = distby->distState->sliceList;
    totalBoundariesNum = GetTotalBoundariesNum(sliceList);
    sliceConstInfo = (SliceConstInfo*)palloc0(totalBoundariesNum * sizeof(SliceConstInfo));

    foreach_cell(sliceCell, sliceList) {
        slicedef = (ListSliceDefState*)lfirst(sliceCell);
        boundaryList = slicedef->boundaries;

        /* check that current slice have correct default boundaries */
        bool isDefault = PreCheckListSlice(slicedef);
        if (isDefault) {
            if (lnext(sliceCell) != NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("DEFAULT slice must be last slice specified")));
            }
            continue;
        }

        foreach_cell(boundaryCell, boundaryList) {
            boundary = (List*)lfirst(boundaryCell);
            /* check whether each boundary within current slice has correct number of elements */
            CheckListDistEntry(boundary, distkeynum, isDefault);
            /* check whether exist the same boundaries */
            Const* curConstValue = GetPartitionValue(pos, attrs, boundary, is_intreval, false);
            constsList = lappend(constsList, curConstValue);
            sliceConstInfo[sliceCount].sliceName = slicedef->name;
            sliceConstInfo[sliceCount].sliceNum = distkeynum;
            sliceConstInfo[sliceCount].sliceBoundary = boundary;
            for (int counter = 0; counter < pos->length; counter++) {
                sliceConstInfo[sliceCount].sliceBoundaryValue[counter] = curConstValue + counter;
            }
            sliceCount++;
        }
    }
    IsDuplicateBoundariesExist(sliceConstInfo, totalBoundariesNum, distkeynum);
    if (constsList != NULL) {
        list_free_deep(constsList);
    }
    pfree_ext(sliceConstInfo);
    return;
}

static void CheckOneBoundaryValue(List* boundary, List* posList, TupleDesc desc)
{
    int pos;
    Const* srcConst = NULL;
    Const* targetConst = NULL;
    ListCell* boundaryCell = NULL;
    ListCell* posCell = NULL;
    Form_pg_attribute* attrs = desc->attrs;

    forboth(boundaryCell, boundary, posCell, posList) {
        srcConst = (Const*)lfirst(boundaryCell);
        if (srcConst->ismaxvalue || srcConst->constisnull) {
            continue;
        }

        pos = lfirst_int(posCell);
        targetConst = (Const*)GetTargetValue(attrs[pos], srcConst, false);
        if (!PointerIsValid(targetConst)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("slice boundary value can't be converted into distribution key data type")));
        }
    }
}

/*
 * check whether every boundary value can be converted to distribute key type successfully
 */
static void CheckBoundaryValues(DistributeBy *distributeby, List* posList, TupleDesc desc)
{
    ListCell* cell = NULL;
    ListCell* boundaryCell = NULL;
    List* boundary = NULL;
    ListSliceDefState* listDef = NULL;
    RangePartitionDefState* rangeDef = NULL;

    if (distributeby->disttype == DISTTYPE_RANGE) {
        foreach(cell, distributeby->distState->sliceList) {
            rangeDef = (RangePartitionDefState*)lfirst(cell);
            CheckOneBoundaryValue(rangeDef->boundary, posList, desc);
        }
    } else {
        foreach(cell, distributeby->distState->sliceList) {
            listDef = (ListSliceDefState*)lfirst(cell);
            foreach(boundaryCell, listDef->boundaries) {
                boundary = (List*)lfirst(boundaryCell);
                CheckOneBoundaryValue(boundary, posList, desc);
            }
        }
    }

    return;
}

static void CheckDistStateBoundaryValue(DistributeBy *distributeby, List* posList, TupleDesc desc)
{
    int oldFormat = u_sess->attr.attr_sql.sql_compatibility;

    PG_TRY();
    {
        /*
         * some scenario should report error,
         * like: distribute by (int) (slice s1 values less than ('a')),
         * 'a' can't be convert to integer, references to int4in
         */
        u_sess->attr.attr_sql.sql_compatibility = A_FORMAT;
        CheckBoundaryValues(distributeby, posList, desc);
    }
    PG_CATCH();
    {
        u_sess->attr.attr_sql.sql_compatibility = oldFormat;
        PG_RE_THROW();
    }
    PG_END_TRY();

    u_sess->attr.attr_sql.sql_compatibility = oldFormat;

    return;
}

static void CheckDistBoundaries(DistributeBy *distributeby, TupleDesc desc)
{
    List* posList = NULL;

    if (distributeby == NULL || distributeby->distState == NULL) {
        return;
    }

    posList = GetDistColsPos(distributeby, desc);
    if (distributeby->disttype == DISTTYPE_RANGE) {
        /* 
         * 1. check length of every slice boundaries
         * 2. check whether range slice boundaries ascending
         */
        ComparePartitionValue(posList, desc->attrs, distributeby->distState->sliceList, false);
    } else {
        /*
         * DISTTYPE_LIST
         * 1. check length of every slice boundaries
         * 2. check whether list slice boundaries unique
         */
        CheckDuplicateListSlices(posList, desc->attrs, distributeby);
    }

    CheckDistStateBoundaryValue(distributeby, posList, desc);

    list_free_ext(posList);
    return;
}

static void CheckNodeInNodeGroup(char** dnName, Oid dnOid, char* sliceName, Oid* nodeoids, int numnodes)
{
    bool isExist = false;
    for (int i = 0; i < numnodes; i++) {
        if (PGXCNodeGetNodeId(dnOid, PGXC_NODE_DATANODE) == PGXCNodeGetNodeId(nodeoids[i], PGXC_NODE_DATANODE)) {
            isExist = true;
            break;
        }
    }

    if (!isExist) {
        if (u_sess->attr.attr_sql.enable_cluster_resize) {
            /*
             * we should clear non-existed datanode name when cluster resizing,
             * e.g. create table (like distribution)
             * because the specified dn_name maybe doesn't exist after shrinking.
             */
            *dnName = NULL;
            ereport(WARNING,
                (errmsg("the specified datanode %s in slice %s is not in node group, ignore it when cluster resizing",
                    *dnName, sliceName)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("datanode %s specified in slice %s is not contained in the specified node group",
                        *dnName, sliceName)));
        }
    }
}

static void CheckDistDatanodeValidity(DistributeBy* distributeby, Oid* nodeoids, int numnodes)
{
    ListCell* cell = NULL;
    Node* node = NULL;
    DistState* distState = NULL;
    Oid dnOid;

    if (distributeby == NULL || distributeby->distState == NULL) {
        return;
    }
    
    /*
     * Checks: 1. specified dn exists; 2. specified dn is contained in specifed node group.
     */
    distState = distributeby->distState;
    foreach (cell, distState->sliceList) {
        node = (Node*)lfirst(cell);
        switch (nodeTag(node)) {
            case T_RangePartitionDefState: {
                RangePartitionDefState *def = (RangePartitionDefState*)node;
                if (def->tablespacename != NULL) {
                    dnOid = get_pgxc_datanodeoid(def->tablespacename, false);
                    CheckNodeInNodeGroup(&def->tablespacename, dnOid, def->partitionName, nodeoids, numnodes);
                }
                break;
            }
            case T_ListSliceDefState: {
                ListSliceDefState* def = (ListSliceDefState*)node;
                if (def->datanode_name != NULL) {
                    dnOid = get_pgxc_datanodeoid(def->datanode_name, false);
                    CheckNodeInNodeGroup(&def->datanode_name, dnOid, def->name, nodeoids, numnodes);
                }
                break;
            }
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(node))));
        }
    }

    return;
}

static void CheckSliceRefNodeGroup(DistributeBy *distributeby, char* groupName) 
{
    Oid groupOid, baseGroupOid;

    /*
     * If groupname is NULL, it means we didn't specify GROUP in table creation,
     * so just use the default installation as target group
     */
    if (groupName == NULL) {
        groupName = PgxcGroupGetInstallationGroup();
    }
    groupOid = get_pgxc_groupoid(groupName, true);
    baseGroupOid = get_pgxc_class_groupoid(distributeby->referenceoid);
    if (baseGroupOid != groupOid) {
        char *baseGroupName = get_pgxc_groupname(baseGroupOid);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_ATTRIBUTE),
                    errmsg("table's node group (%s) is not the same with referenced table's node group(%s)", 
                        groupName, baseGroupName)));
    }

    return;
}

/*
 * Check whether base table can references the slice of another table
 */
static void CheckSliceReferenceValidity(Oid relid, DistributeBy *distributeby, TupleDesc descriptor, char* groupName)
{
    char baseType;
    Oid keyType;
    Oid baseKeyType;
    AttrNumber keyIdx;
    AttrNumber baseKeyIdx;
    DistributionType reftype;
    char *colname = NULL;
    RelationLocInfo* baseLocInfo = NULL;
    ListCell* colAttrCell = NULL;
    ListCell* colNameCell = NULL;

    /* check whether the two tables' distribute type are the same */
    baseLocInfo = GetRelationLocInfo(distributeby->referenceoid);
    if (baseLocInfo == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("The referenced table is not distributed")));
    }

    baseType = baseLocInfo->locatorType;
    reftype = distributeby->disttype;
    if (!((baseType == LOCATOR_TYPE_RANGE && reftype == DISTTYPE_RANGE) || 
        (baseType == LOCATOR_TYPE_LIST && reftype == DISTTYPE_LIST))) {
        FreeRelationLocInfo(baseLocInfo);
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("Assigned distribution strategy is not the same as that of base table")));
    }

    /* check whether the two tables' distribution key number are the same */
    if (list_length(distributeby->colname) != list_length(baseLocInfo->partAttrNum)) {
        FreeRelationLocInfo(baseLocInfo);
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("number of table's distribution keys is not the same with referenced table.")));
    }

    /* check whether the two tables' distribution key type are the same */
    forboth(colAttrCell, baseLocInfo->partAttrNum, colNameCell, distributeby->colname) {
        baseKeyIdx = lfirst_int(colAttrCell);
        colname = strVal(lfirst(colNameCell));

        baseKeyType = get_atttype(distributeby->referenceoid, baseKeyIdx);
        keyIdx = get_attnum(relid, colname);
        keyType = descriptor->attrs[keyIdx - 1]->atttypid;

        if (baseKeyType != keyType) {
            FreeRelationLocInfo(baseLocInfo);
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("Column %s is not of the same datatype as base table", colname)));
        }
    }

    /* check the two tables' node group are the same */
    CheckSliceRefNodeGroup(distributeby, groupName);

    FreeRelationLocInfo(baseLocInfo);
    return;
}

static char* GetRelationDistributionGroupName(PGXCSubCluster* subcluster, Oid* nodeoids, int numnodes, bool first)
{
    char *group_name = NULL;

    /* Now OK to insert data in catalog */
    if (subcluster != NULL && subcluster->clustertype == SUBCLUSTER_GROUP) {
        ListCell* lc = NULL;
        Assert(list_length(subcluster->members) == 1);
        foreach (lc, subcluster->members) {
            group_name = strVal(lfirst(lc));
            break;
        }
    } else if (subcluster != NULL && subcluster->clustertype == SUBCLUSTER_NODE) {
        /*
         * If we use the grammar "create table to node", when we support
         * multi-nodegroup, it must be converted to "create table to group",
         * but we are not sure to find the group correct through "to node".
         *
         * In non-logic cluster mode, we use "to node" for create table to installatation
         * group or redistribution group in restore mode, so we do this conversion here.
         *
         * In logic cluster mode, we use "to node" for create table to logic cluster
         * node group in restore mode.
         */
        group_name = DeduceGroupByNodeList(nodeoids, numnodes);
    } else if (first) {
        /*
         * Neither TO NODE nor TO GROUP is not specified in CREATE TABLE,
         * the foreign table and hdfs table is created in installation group.
         */
        group_name = PgxcGroupGetInstallationGroup();
    }

    /*
     * If groupname is NULL, it means we didn't specify GROUP in table creation,
     * so just use the default installation as target group
     */
    if (group_name == NULL) {
        if (isRestoreMode || strcmp(u_sess->attr.attr_sql.default_storage_nodegroup, INSTALLATION_MODE) == 0) {
            group_name = PgxcGroupGetInstallationGroup();
        } else {
            group_name = u_sess->attr.attr_sql.default_storage_nodegroup;
        }
    }

    return group_name;
}

/* --------------------------------
 *		AddRelationDistribution
 *
 *		Add to pgxc_class table
 * --------------------------------
 */
void AddRelationDistribution(const char *relname, Oid relid, DistributeBy* distributeby, PGXCSubCluster* subcluster,
    List* parentOids, TupleDesc descriptor, bool isinstallationgroup, bool isbucket, int bucketmaplen)
{
    char locatortype = '\0';
    int hashalgorithm = 0;
    int hashbuckets = 0;
    int2* attnum = NULL;
    ObjectAddress myself, referenced;
    int numnodes;
    Oid* nodeoids = NULL;
    int distributeKeyNum = 0;
    char* group_name = NULL;
    Oid   group_oid = InvalidOid;
    int   bucketCnt;

    if (distributeby != NULL && distributeby->colname) {
        distributeKeyNum = list_length(distributeby->colname);
        attnum = (int2*)palloc(distributeKeyNum * sizeof(int2));
    } else {
        distributeKeyNum = 1;
        attnum = (int2*)palloc(1 * sizeof(int2));
    }

    /* Obtain details of distribution information */
    GetRelationDistributionItems(relid, distributeby, descriptor, &locatortype, &hashalgorithm, &hashbuckets, attnum);

    /* Check whether include identical column in distribute key */
    CheckDistColsUnique(distributeby, distributeKeyNum, attnum);

    /* Check whether list/range boundaries valid */
    CheckDistBoundaries(distributeby, descriptor);

    /* Obtain details of nodes and classify them */
    nodeoids = GetRelationDistributionNodes(subcluster, &numnodes);

    /* Check Datanode validity */
    CheckDistDatanodeValidity(distributeby, nodeoids, numnodes);

    /* Determine node group name */
    group_name = GetRelationDistributionGroupName(subcluster, nodeoids, numnodes, isinstallationgroup);

    /* check if there is any child group can match the maplen */
    (void)BucketMapCacheGetBucketmap(group_name, &bucketCnt);
    if (bucketmaplen != 0 && bucketmaplen != bucketCnt) {
        bucketCnt = bucketmaplen;
        if (!is_pgxc_group_bucketcnt_exists(get_pgxc_groupoid(group_name, false), bucketmaplen, &group_name, 
            &group_oid)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("there is no group can match the bucket map length %d", bucketmaplen)));
        }
        Assert(OidIsValid(group_oid));
        t_thrd.xact_cxt.PGXCGroupOid = group_oid;
    }

    PgxcClassCreate(
        relid, locatortype, attnum, hashalgorithm, hashbuckets, numnodes, nodeoids, distributeKeyNum, group_name);

    if (IsLocatorDistributedBySlice(locatortype) && distributeby != NULL) {
        /* Report WARNING when slice count less than numnodes */
        if (!OidIsValid(distributeby->referenceoid) && list_length(distributeby->distState->sliceList) < numnodes) {
            ereport(NOTICE, (errmsg("table's slice count is less than datanode count.")));
        }

        /* Check whether base table and ref table have the same reqs */
        if (OidIsValid(distributeby->referenceoid)) {
            CheckSliceReferenceValidity(relid, distributeby, descriptor, group_name);
        }

        /*
         * generate a random startpos to reduce slice skewness.
         * we should guarantee startpos the same in all CNs when creating a list/range distribution table.
         * implement it later using ((uint32)random()) % ((uint32)numnodes),
         * and route the startpos to other CNs.
         */
        
        /* write slice information to pgxc_slice, keep startpos to 0 at present. */
        PgxcSliceCreate(relname, relid, distributeby, descriptor, nodeoids, (uint32)numnodes, 0);
    }

    pfree(attnum);
    /* Make dependency entries */
    myself.classId = PgxcClassRelationId;
    myself.objectId = relid;
    myself.objectSubId = 0;

    /* Dependency on relation */
    referenced.classId = RelationRelationId;
    referenced.objectId = relid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
}

static const char* GetDistributeTypeName(DistributionType disttype)
{
    const char *str = "";
    switch (disttype) {
        case DISTTYPE_HASH:
            str = "hash";
            break;
        case DISTTYPE_REPLICATION:
            str = "replication";
            break;
        case DISTTYPE_MODULO:
            str = "modulo";
            break;
        case DISTTYPE_ROUNDROBIN:
            str = "roundrobin";
            break;
        case DISTTYPE_HIDETAG:
            str = "hidetag";
            break;
        case DISTTYPE_LIST:
            str = "list";
            break;
        case DISTTYPE_RANGE:
            str = "range";
            break;
        default:
            break;
    }

    return str;
}

static void CheckDistributeKeyAndType(Oid relid, DistributeBy *distributeby,
    TupleDesc descriptor, AttrNumber *attnum)
{
    int i = 0;
    AttrNumber localAttrNum = 0;
    char *colname = NULL;
    ListCell *cell = NULL;

    foreach (cell, distributeby->colname) {
        colname = strVal(lfirst(cell));
        localAttrNum = get_attnum(relid, colname);

        /*
         * Validate user-specified distribute column.
         * System columns cannot be used.
         */
        if (localAttrNum <= 0 && localAttrNum >= -(int)lengthof(SysAtt)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Invalid distribution column specified")));
        }

        if (distributeby->disttype == DISTTYPE_LIST || distributeby->disttype == DISTTYPE_RANGE) {
            if (!IsTypeDistributableForSlice(descriptor->attrs[localAttrNum - 1]->atttypid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("Column %s is not a %s distributable data type", colname,
                            GetDistributeTypeName(distributeby->disttype))));
            }
        } else {
            if (!IsTypeDistributable(descriptor->attrs[localAttrNum - 1]->atttypid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("Column %s is not a %s distributable data type", colname,
                            GetDistributeTypeName(distributeby->disttype))));
            }
        }
        
        attnum[i++] = localAttrNum;
    }
}

/*
 * GetRelationDistributionItems
 * Obtain distribution type and related items based on deparsed information
 * of clause DISTRIBUTE BY.
 * Depending on the column types given a fallback to a safe distribution can be done.
 */
void GetRelationDistributionItems(Oid relid, DistributeBy* distributeby, TupleDesc descriptor, char* locatortype,
    int* hashalgorithm, int* hashbuckets, AttrNumber* attnum)
{
    int local_hashalgorithm = 0;
    int local_hashbuckets = 0;
    char local_locatortype = '\0';
    AttrNumber local_attnum = 0;

    if (distributeby == NULL) {
        /*
         * If no distribution was specified, and we have not chosen
         * one based on primary key or foreign key, use first column with
         * a supported data type.
         */
        Form_pg_attribute attr;
        int i;

        local_locatortype = LOCATOR_TYPE_HASH;

        for (i = 0; i < descriptor->natts; i++) {
            attr = descriptor->attrs[i];
            if (IsTypeDistributable(attr->atttypid)) {
                /* distribute on this column */
                local_attnum = i + 1;
                attnum[0] = local_attnum;
                Oid namespaceid = get_rel_namespace(relid);

                /*
                 * As create hdfs table will inner create a delta table. For preventing
                 * duplicate print notice, we check the relnamespace here to forbid
                 * print notice when create a delta table.
                 */
                if (!IsCStoreNamespace(namespaceid))
                    ereport(NOTICE,
                        (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                            errmsg("The 'DISTRIBUTE BY' clause is not specified. Using '%s' as the distribution column "
                                   "by default.",
                                attr->attname.data),
                            errhint(
                                "Please use 'DISTRIBUTE BY' clause to specify suitable data distribution column.")));
                break;
            }
        }
        if (local_attnum == 0) {
            FEATURE_NOT_PUBLIC_ERROR("There is no hash distributable column");
        }
        /* If we did not find a usable type, fall back to round robin */
        if (local_attnum == 0) {
            attnum[0] = local_attnum;
            local_locatortype = LOCATOR_TYPE_RROBIN;
        }
    } else {
        /*
         * User specified distribution type
         */
        switch (distributeby->disttype) {
            case DISTTYPE_HASH:
                CheckDistributeKeyAndType(relid, distributeby, descriptor, attnum);
                local_locatortype = LOCATOR_TYPE_HASH;
                break;
            case DISTTYPE_MODULO:
                CheckDistributeKeyAndType(relid, distributeby, descriptor, attnum);
                local_locatortype = LOCATOR_TYPE_MODULO;
                break;
            case DISTTYPE_REPLICATION:
                local_locatortype = LOCATOR_TYPE_REPLICATED;
                attnum[0] = local_attnum;
                break;
            case DISTTYPE_ROUNDROBIN:
                local_locatortype = LOCATOR_TYPE_RROBIN;
                attnum[0] = local_attnum;
                break;
            case DISTTYPE_LIST:
                CheckDistributeKeyAndType(relid, distributeby, descriptor, attnum);
                local_locatortype = LOCATOR_TYPE_LIST;
                break;
            case DISTTYPE_RANGE:
                CheckDistributeKeyAndType(relid, distributeby, descriptor, attnum);
                local_locatortype = LOCATOR_TYPE_RANGE;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("Invalid distribution type")));
        }
    }

    /* Use default hash values */
    if (local_locatortype == LOCATOR_TYPE_HASH) {
        local_hashalgorithm = 1;
        local_hashbuckets = HASH_SIZE;
    }

    /* Save results */
    if (hashalgorithm != NULL)
        *hashalgorithm = local_hashalgorithm;
    if (hashbuckets != NULL)
        *hashbuckets = local_hashbuckets;
    if (locatortype != NULL)
        *locatortype = local_locatortype;
}

HashBucketInfo* GetRelationBucketInfo(DistributeBy* distributeby, 
                                      TupleDesc tupledsc, 
                                      bool* createbucket,
                                      bool hashbucket)
{
    Form_pg_attribute attr = NULL;
    int2vector* bucketkey = NULL;
    HashBucketInfo* bucketinfo = NULL;
    int nattr = tupledsc->natts;
    Oid buckettmp[BUCKETDATALEN];
    int bucketcnt = 0;
    bool needbucket = *createbucket;
    int i = 0;

    /* sanity check for bucket map */
    if (!isRestoreMode && needbucket == true && t_thrd.xact_cxt.PGXCNodeId == -1) {
        if (distributeby == NULL || distributeby->disttype != DISTTYPE_HASH) {
            if (hashbucket) {
                Assert(0);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Create hash table directly on DN is not allowed.")));
            } else {
                ereport(LOG, (errmsg("Create table without bucket when directly created on dn.")));
                return NULL;
            }
        } else {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Create hash table but there is no valid PGXCBucketMap.")));
        }
    }

    bucketinfo = (HashBucketInfo*)palloc0(sizeof(HashBucketInfo));
    bucketinfo->bucketOid = InvalidOid;
    if (t_thrd.xact_cxt.PGXCNodeId == MAX_DN_NODE_NUM) {
        Assert(u_sess->attr.attr_sql.enable_cluster_resize);
        if (u_sess->attr.attr_sql.enable_cluster_resize && !isRestoreMode && needbucket == true) {
            *createbucket = false;
            ereport(LOG, (errmsg("Create table without bucket when PGXCNodeId is MAX_DN_NODE_NUM.")));

        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Invalid BucketMap or wrong NodeId(%d)", MAX_DN_NODE_NUM)));
        }
    }
    /* get the bucket key columns from distribute by statement */
    if (distributeby == NULL) {
        bool found = false;

        bucketkey = buildint2vector(NULL, 1);
        for (i = 0; i < nattr; i++) {
            attr = tupledsc->attrs[i];
            if (IsTypeDistributable(attr->atttypid)) {
                bucketkey->values[0] = attr->attnum;
                bucketinfo->bucketcol = bucketkey;
                found = true;
                break;
            }
        }

        if (!found) {
            if (hashbucket) {
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("There is no hash distributable column")));
            } else {
                pfree(bucketinfo);
                return NULL;
            }
        }
    } else if (distributeby->disttype == DISTTYPE_HASH) {
        /*
         * User specified distribution type
         */
        AttrNumber local_attnum = 0;
        ListCell* cell = NULL;
        char* colname = NULL;
        bool* is_exist = NULL;
        int j;
        /*
         * Validate user-specified hash column.
         * System columns cannot be used.
         */
        bucketkey = buildint2vector(NULL, distributeby->colname->length);
        is_exist = (bool*)palloc0(nattr * sizeof(bool));

        foreach (cell, distributeby->colname) {
            colname = strVal(lfirst(cell));
            for (j = 0; j < nattr; j++) {
                attr = tupledsc->attrs[j];
                if (strcmp(colname, attr->attname.data) == 0) {
                    local_attnum = attr->attnum;
                    break;
                }
            }

            if (j == nattr) {
                pfree(bucketkey);
                pfree(is_exist);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("Invalid distribution column specified")));
            }

            if (!IsTypeDistributable(attr->atttypid)) {
                pfree(bucketkey);
                pfree(is_exist);
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("Column %s is not a hash distributable data type", colname)));
            }

            if (is_exist[local_attnum - 1] == true) {
                pfree(bucketkey);
                pfree(is_exist);
                ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN), errmsg("duplicate partition key: %s", colname)));
            }
            bucketkey->values[i++] = local_attnum;
            is_exist[local_attnum - 1] = true;
        }
        pfree(is_exist);
        bucketinfo->bucketcol = bucketkey;
    } else {
        if (hashbucket) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("distributeby no hash is invalid with hashbucket")));
        } else {
            pfree(bucketinfo);
            return NULL;
        }
    }

    if (*createbucket == true) {
        /* loop to get the bucket list on current node */
        if (isRestoreMode) {
            if (u_sess->storage_cxt.dumpHashbucketIds != NULL && u_sess->storage_cxt.dumpHashbucketIdNum != 0) {
                for (i = 0; i < u_sess->storage_cxt.dumpHashbucketIdNum; i++) {
                    buckettmp[bucketcnt++] = u_sess->storage_cxt.dumpHashbucketIds[i];
                }
            }
        } else {
            for (int i = 0; i < t_thrd.xact_cxt.PGXCBucketCnt; i++) {
                /*
                 * if bucket seqno equals to PGXCNodeId which get from CN then
                 * then we will add a bucket for current relation later
                 */
                if (t_thrd.xact_cxt.PGXCBucketMap[i] == t_thrd.xact_cxt.PGXCNodeId) {
                    buckettmp[bucketcnt++] = i;
                }
            }
        }
        if (bucketcnt == 0) {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Invalid BucketMap or wrong NodeId(%d)", t_thrd.xact_cxt.PGXCNodeId)));
        }
        /* make sure bucket map is in ascending order */
        qsort(buckettmp, bucketcnt, sizeof(Oid), bid_cmp);
        bucketinfo->bucketlist = buildoidvector(buckettmp, bucketcnt);
    }

    return bucketinfo;
}

/*
 * BuildRelationDistributionNodes
 * Build an unsorted node Oid array based on a node name list.
 */
Oid* BuildRelationDistributionNodes(List* nodes, int* numnodes)
{
    Oid* nodeoids = NULL;
    ListCell* item = NULL;

    *numnodes = 0;

    /* Allocate once enough space for OID array */
    nodeoids = (Oid*)palloc0(u_sess->pgxc_cxt.NumDataNodes * sizeof(Oid));

    /* Do process for each node name */
    foreach (item, nodes) {
        char* node_name = strVal(lfirst(item));
        Oid noid = get_pgxc_nodeoid(node_name);

        /* Check existence of node */
        if (!OidIsValid(noid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));

        if (get_pgxc_nodetype(noid) != PGXC_NODE_DATANODE)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: not a Datanode", node_name)));

        /* Can be added if necessary */
        if (*numnodes != 0) {
            bool is_listed = false;
            int i;

            /* Id Oid already listed? */
            for (i = 0; i < *numnodes; i++) {
                if (nodeoids[i] == noid) {
                    is_listed = true;
                    break;
                }
            }

            if (!is_listed) {
                (*numnodes)++;
                nodeoids[*numnodes - 1] = noid;
            }
        } else {
            (*numnodes)++;
            nodeoids[*numnodes - 1] = noid;
        }
    }

    return nodeoids;
}

/*
 * GetRelationDistributionNodes
 * Transform subcluster information generated by query deparsing of TO NODE or
 * TO GROUP clause into a sorted array of nodes OIDs.
 */
Oid* GetRelationDistributionNodes(PGXCSubCluster* subcluster, int* numnodes)
{
    ListCell* lc = NULL;
    Oid* nodes = NULL;

    *numnodes = 0;

    if (subcluster == NULL) {
        int i;

        if (!isRestoreMode) {
            /*
             * If no subcluster is defined, all the Datanodes are associated to the
             * table. So obtain list of node Oids currenly known to the session.
             * There could be a difference between the content of pgxc_node catalog
             * table and current session, because someone may change nodes and not
             * yet update session data.
             */
            *numnodes = u_sess->pgxc_cxt.NumDataNodes;

            /* No nodes found ?? */
            if (*numnodes == 0)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No Datanode defined in cluster")));

            nodes = (Oid*)palloc(u_sess->pgxc_cxt.NumDataNodes * sizeof(Oid));
            for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++)
                nodes[i] = PGXCNodeGetNodeOid(i, PGXC_NODE_DATANODE);
        } else {
            PgxcNodeGetOids(NULL, &nodes, NULL, numnodes, false);
            /* No nodes found ?? */
            if (*numnodes == 0)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No Datanode defined in cluster")));
        }
    }

    /* Build list of nodes from given group */
    if (nodes == NULL && subcluster->clustertype == SUBCLUSTER_GROUP) {
        Assert(list_length(subcluster->members) == 1);

        foreach (lc, subcluster->members) {
            const char* group_name = strVal(lfirst(lc));
            Oid group_oid = get_pgxc_groupoid(group_name);

            if (!OidIsValid(group_oid))
                ereport(
                    ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));

            *numnodes = get_pgxc_groupmembers(group_oid, &nodes);
        }
    } else if (nodes == NULL) {
        /*
         * This is the case of a list of nodes names.
         * Here the result is a sorted array of node Oids
         */
        nodes = BuildRelationDistributionNodes(subcluster->members, numnodes);
    }

    /* Return a sorted array of node OIDs */
    return SortRelationDistributionNodes(nodes, *numnodes);
}

/*
 * SortRelationDistributionNodes
 * Sort elements in a node array.
 */
Oid* SortRelationDistributionNodes(Oid* nodeoids, int numnodes)
{
    qsort(nodeoids, numnodes, sizeof(Oid), cmp_nodes);
    return nodeoids;
}
#endif

/* --------------------------------
 *		AddNewRelationType -
 *
 *		define a composite type corresponding to the new relation
 * --------------------------------
 */
static Oid AddNewRelationType(const char* typname, Oid typeNamespace, Oid new_rel_oid, char new_rel_kind, Oid ownerid,
    Oid new_row_type, Oid new_array_type)
{
    return TypeCreate(new_row_type, /* optional predetermined OID */
        typname,                    /* type name */
        typeNamespace,              /* type namespace */
        new_rel_oid,                /* relation oid */
        new_rel_kind,               /* relation kind */
        ownerid,                    /* owner's ID */
        -1,                         /* internal size (varlena) */
        TYPTYPE_COMPOSITE,          /* type-type (composite) */
        TYPCATEGORY_COMPOSITE,      /* type-category (ditto) */
        false,                      /* composite types are never preferred */
        DEFAULT_TYPDELIM,           /* default array delimiter */
        F_RECORD_IN,                /* input procedure */
        F_RECORD_OUT,               /* output procedure */
        F_RECORD_RECV,              /* receive procedure */
        F_RECORD_SEND,              /* send procedure */
        InvalidOid,                 /* typmodin procedure - none */
        InvalidOid,                 /* typmodout procedure - none */
        InvalidOid,                 /* analyze procedure - default */
        InvalidOid,                 /* array element type - irrelevant */
        false,                      /* this is not an array type */
        new_array_type,             /* array type if any */
        InvalidOid,                 /* domain base type - irrelevant */
        NULL,                       /* default value - none */
        NULL,                       /* default binary representation */
        false,                      /* passed by reference */
        'd',                        /* alignment - must be the largest! */
        'x',                        /* fully TOASTable */
        -1,                         /* typmod */
        0,                          /* array dimensions for typBaseType */
        false,                      /* Type NOT NULL */
        InvalidOid);                /* rowtypes never have a collation */
}

/* --------------------------------
 *		heap_create_with_catalog
 *
 *		creates a new cataloged relation.  see comments above.
 *
 * Arguments:
 *	relname: name to give to new rel
 *	relnamespace: OID of namespace it goes in
 *	reltablespace: OID of tablespace it goes in
 *	relid: OID to assign to new rel, or InvalidOid to select a new OID
 *	reltypeid: OID to assign to rel's rowtype, or InvalidOid to select one
 *	reloftypeid: if a typed table, OID of underlying type; else InvalidOid
 *	ownerid: OID of new rel's owner
 *	tupdesc: tuple descriptor (source of column definitions)
 *	cooked_constraints: list of precooked check constraints and defaults
 *	relkind: relkind for new rel
 *	relpersistence: rel's persistence status (permanent, temp, or unlogged)
 *	shared_relation: TRUE if it's to be a shared relation
 *	mapped_relation: TRUE if the relation will use the relfilenode map
 *	oidislocal: TRUE if oid column (if any) should be marked attislocal
 *	oidinhcount: attinhcount to assign to oid column (if any)
 *	oncommit: ON COMMIT marking (only relevant if it's a temp table)
 *	reloptions: reloptions in Datum form, or (Datum) 0 if none
 *	use_user_acl: TRUE if should look for user-defined default permissions;
 *		if FALSE, relacl is always set NULL
 *	allow_system_table_mods: TRUE to allow creation in system namespaces
 *
 * Returns the OID of the new relation
 * --------------------------------
 */
Oid heap_create_with_catalog(const char *relname, Oid relnamespace, Oid reltablespace, Oid relid, Oid reltypeid,
                             Oid reloftypeid, Oid ownerid, TupleDesc tupdesc, List *cooked_constraints, char relkind,
                             char relpersistence, bool shared_relation, bool mapped_relation, bool oidislocal,
                             int oidinhcount, OnCommitAction oncommit, Datum reloptions, bool use_user_acl,
                             bool allow_system_table_mods, PartitionState *partTableState, int8 row_compress,
                             HashBucketInfo *bucketinfo, bool record_dependce, List *ceLst, StorageType storage_type,
                             LOCKMODE partLockMode)
{
    Relation pg_class_desc;
    Relation new_rel_desc;
    Relation pg_partition_desc = NULL;
    Acl* relacl = NULL;
    Oid existing_relid;
    Oid old_type_oid;
    Oid new_type_oid = InvalidOid;
    Oid new_array_oid = InvalidOid;
    MemoryContext old_context;
    Oid relfileid = InvalidOid;
    Oid relbucketOid = InvalidOid;
    int2vector* bucketcol = NULL;
    bool relhasbucket = false;
    bool relhasuids = false;

    pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);

    /*
     * sanity checks
     */
    Assert(IsNormalProcessingMode() || IsBootstrapProcessingMode());

    CheckAttributeNamesTypes(tupdesc, relkind, allow_system_table_mods);

    /*
     * This would fail later on anyway, if the relation already exists.  But
     * by catching it here we can emit a nicer error message.
     */
    existing_relid = get_relname_relid(relname, relnamespace);
    if (existing_relid != InvalidOid) {
        Oid namespaceid_of_existing_rel = get_rel_namespace(existing_relid);
        char* namespace_of_existing_rel = get_namespace_name(namespaceid_of_existing_rel);
        ereport(ERROR, (errmodule(MOD_COMMAND), errcode(ERRCODE_DUPLICATE_TABLE),
            errmsg("relation \"%s\" already exists in schema \"%s\"", relname, namespace_of_existing_rel),
            errdetail("creating new table with existing name in the same schema"),
            errcause("the name of the table being created already exists"),
            erraction("change a new name")));
    }

    /*
     * Since we are going to create a rowtype as well, also check for
     * collision with an existing type name.  If there is one and it's an
     * autogenerated array, we can rename it out of the way; otherwise we can
     * at least give a good error message.
     */
    old_type_oid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(relname), ObjectIdGetDatum(relnamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, relname, relnamespace))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("type \"%s\" already exists", relname),
                    errhint("A relation has an associated type of the same name, "
                            "so you must use a name that doesn't conflict "
                            "with any existing type.")));
    }

    /*
     * Shared relations must be in pg_global (last-ditch check)
     */
    if (shared_relation && reltablespace != GLOBALTABLESPACE_OID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("shared relations must be placed in pg_global tablespace")));

    /*
     * Allocate an OID for the relation, unless we were told what to use.
     *
     * The OID will be the relfilenode as well, so make sure it doesn't
     * collide with either pg_class OIDs or existing physical files.
     */
    if (!OidIsValid(relid)) {
        bool is_partition_toast_tbl = strncmp(relname, "pg_toast_part", strlen("pg_toast_part")) == 0;
        /*
         * Use binary-upgrade override for pg_class.oid/relfilenode, if
         * supplied.
         */
        if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid) &&
            (relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(relkind) || relkind == RELKIND_VIEW ||
            relkind == RELKIND_COMPOSITE_TYPE || relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_MATVIEW
            || relkind == RELKIND_STREAM || relkind == RELKIND_CONTQUERY)) {
            relid = u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid;
            u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = InvalidOid;

            relfileid = u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = InvalidOid;

        } else if (u_sess->proc_cxt.IsBinaryUpgrade && !is_partition_toast_tbl &&
                   OidIsValid(u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid) &&
                   relkind == RELKIND_TOASTVALUE) {
            relid = u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid;
            u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid = InvalidOid;

            relfileid = u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid = InvalidOid;
        } else if (u_sess->proc_cxt.IsBinaryUpgrade &&
                   (is_partition_toast_tbl && binary_upgrade_is_next_part_toast_pg_class_oid_valid()) &&
                   relkind == RELKIND_TOASTVALUE) {
            relid = binary_upgrade_get_next_part_toast_pg_class_oid();
            relfileid = binary_upgrade_get_next_part_toast_pg_class_rfoid();
        } else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
                   OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid) &&
                   (relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(relkind) || relkind == RELKIND_VIEW ||
                   relkind == RELKIND_COMPOSITE_TYPE || relkind == RELKIND_FOREIGN_TABLE
                   || relkind == RELKIND_STREAM || relkind == RELKIND_CONTQUERY)) {
            relid = u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid;
            u_sess->upg_cxt.Inplace_upgrade_next_heap_pg_class_oid = InvalidOid;
        } else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
                   OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid) &&
                   relkind == RELKIND_TOASTVALUE) {
            /* So far, system catalogs are not partitioned. */
            Assert(!is_partition_toast_tbl);

            relid = u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid;
            u_sess->upg_cxt.Inplace_upgrade_next_toast_pg_class_oid = InvalidOid;
        } else
            relid = GetNewRelFileNode(reltablespace, pg_class_desc, relpersistence);
    }

    /*
     * Determine the relation's initial permissions.
     */
    if (use_user_acl) {
        switch (relkind) {
            case RELKIND_RELATION:
            case RELKIND_VIEW:
            case RELKIND_CONTQUERY:
            case RELKIND_MATVIEW:
            case RELKIND_FOREIGN_TABLE:
            case RELKIND_STREAM:
                relacl = get_user_default_acl(ACL_OBJECT_RELATION, ownerid, relnamespace);
                break;
            case RELKIND_SEQUENCE:
            case RELKIND_LARGE_SEQUENCE: // tbf
                relacl = get_user_default_acl(ACL_OBJECT_SEQUENCE, ownerid, relnamespace);
                break;
            default:
                relacl = NULL;
                break;
        }
    } else
        relacl = NULL;

    /*
     * Determine the relation's bucket info.
     */
    if (bucketinfo != NULL && !OidIsValid(bucketinfo->bucketOid)) {
        Oid bucketid;
        bucketcol = bucketinfo->bucketcol;

        Assert(bucketcol != NULL);
        if (bucketinfo->bucketlist != NULL) {
            bucketid =
                hash_any((unsigned char*)bucketinfo->bucketlist->values, bucketinfo->bucketlist->dim1 * sizeof(Oid));

            relbucketOid = searchHashBucketByBucketid(bucketinfo->bucketlist, bucketid);

            if (!OidIsValid(relbucketOid)) {
                relbucketOid = insertHashBucketEntry(bucketinfo->bucketlist, bucketid);
            }
        } else {
            relbucketOid = VirtualBktOid;
        }

        relhasbucket = true;

    } else if (bucketinfo != NULL) {
        relbucketOid = bucketinfo->bucketOid;
        Assert(OidIsValid(relbucketOid));
        relhasbucket = true;
    }

    /* Get tableAmType from reloptions and relkind */
    bytea* hreloptions = heap_reloptions(relkind, reloptions, false);
    TableAmType tam = get_tableam_from_reloptions(hreloptions, relkind, InvalidOid);
    int8 indexsplit = get_indexsplit_from_reloptions(hreloptions, InvalidOid);

    /* Get uids info from reloptions */
    relhasuids = StdRdOptionsHasUids(hreloptions, relkind);

    /*
     * Create the relcache entry (mostly dummy at this point) and the physical
     * disk file.  (If we fail further down, it's the smgr's responsibility to
     * remove the disk file again.)
     */

    new_rel_desc = heap_create(relname,
        relnamespace,
        reltablespace,
        relid,
        relfileid,
        (partTableState == NULL) ? relbucketOid : InvalidOid,
        tupdesc,
        relkind,
        relpersistence,
        partTableState ? (partTableState->partitionStrategy == PART_STRATEGY_VALUE ? false : true) : false,
        partTableState ? (partTableState->rowMovement == ROWMOVEMENT_ENABLE ? true : false) : false,
        shared_relation,
        mapped_relation,
        allow_system_table_mods,
        row_compress,
        reloptions,
        ownerid,
        false,
        tam,
        indexsplit,
        storage_type
    );

    /* Recode the table or other object in pg_class create time. */
    PgObjectType objectType = GetPgObjectTypePgClass(relkind);
    if (objectType != OBJECT_TYPE_INVALID) {
        PgObjectOption objectOpt = {true, true, true, true};
        CreatePgObject(relid, objectType, ownerid, objectOpt);
    }

    Assert(relid == RelationGetRelid(new_rel_desc));

    new_rel_desc->rd_bucketoid = relbucketOid;
    /*
     * Decide whether to create an array type over the relation's rowtype. We
     * do not create any array types for system catalogs (ie, those made
     * during initdb).	We create array types for regular relations, views,
     * composite types and foreign tables ... but not, eg, for toast tables or
     * sequences.
     */
    if ((IsUnderPostmaster && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        (relkind == RELKIND_RELATION || relkind == RELKIND_VIEW || relkind == RELKIND_FOREIGN_TABLE ||
        relkind == RELKIND_COMPOSITE_TYPE || relkind == RELKIND_MATVIEW || relkind == RELKIND_STREAM || 
        relkind == RELKIND_CONTQUERY)) ||
        (strcmp(relname, "bulk_exception") == 0) || (strcmp(relname, "desc_rec") == 0))
        new_array_oid = AssignTypeArrayOid();

    /*
     * Since defining a relation also defines a complex type, we add a new
     * system type corresponding to the new relation.  The OID of the type can
     * be preselected by the caller, but if reltypeid is InvalidOid, we'll
     * generate a new OID for it.
     */

    /* save the CurrentMemoryContext, so we can switch to it later */
    old_context = CurrentMemoryContext;

    /*
     * try to report a nicer error message rather than 'duplicate key'
     */
    PG_TRY();
    {
        /*
         * NOTE: we could get a unique-index failure here, in case someone else is
         * creating the same type name in parallel but hadn't committed yet when
         * we checked for a duplicate name above. in such case we try to emit a
         * nicer error message using try...catch
         */
        new_type_oid = AddNewRelationType(relname, relnamespace, relid, relkind, ownerid, reltypeid, new_array_oid);
    }
    PG_CATCH();
    {
        ErrorData* edata = NULL;

        /*
         * if we are in here we are most likely in ErrorContext
         * we have to switch to the old_context to allowing for CopyErrorData()
         * because FlushErrorState will free everything in ErrorContext
         */
        (void*)MemoryContextSwitchTo(old_context);
        edata = CopyErrorData();

        if (edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION) {
            FlushErrorState();
            /* the old edata is no longer used */
            FreeErrorData(edata);

            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_TABLE),
                    errmsg("relation \"%s\" already exists", relname),
                    errdetail("because of creating the same table in parallel")));

        } else {
            /* we still reporting other error messages */
            ReThrowError(edata);
        }
    }
    PG_END_TRY();

    /*
     * Now make the array type if wanted.
     */
    if (OidIsValid(new_array_oid)) {
        char* relarrayname = NULL;

        relarrayname = makeArrayTypeName(relname, relnamespace);

        (void)TypeCreate(new_array_oid, /* force the type's OID to this */
            relarrayname,               /* Array type name */
            relnamespace,               /* Same namespace as parent */
            InvalidOid,                 /* Not composite, no relationOid */
            0,                          /* relkind, also N/A here */
            ownerid,                    /* owner's ID */
            -1,                         /* Internal size (varlena) */
            TYPTYPE_BASE,               /* Not composite - typelem is */
            TYPCATEGORY_ARRAY,          /* type-category (array) */
            false,                      /* array types are never preferred */
            DEFAULT_TYPDELIM,           /* default array delimiter */
            F_ARRAY_IN,                 /* array input proc */
            F_ARRAY_OUT,                /* array output proc */
            F_ARRAY_RECV,               /* array recv (bin) proc */
            F_ARRAY_SEND,               /* array send (bin) proc */
            InvalidOid,                 /* typmodin procedure - none */
            InvalidOid,                 /* typmodout procedure - none */
            F_ARRAY_TYPANALYZE,         /* array analyze procedure */
            new_type_oid,               /* array element type - the rowtype */
            true,                       /* yes, this is an array type */
            InvalidOid,                 /* this has no array type */
            InvalidOid,                 /* domain base type - irrelevant */
            NULL,                       /* default value - none */
            NULL,                       /* default binary representation */
            false,                      /* passed by reference */
            'd',                        /* alignment - must be the largest! */
            'x',                        /* fully TOASTable */
            -1,                         /* typmod */
            0,                          /* array dimensions for typBaseType */
            false,                      /* Type NOT NULL */
            InvalidOid);                /* rowtypes never have a collation */

        pfree(relarrayname);
    }

    /*
     * for value-partitioned relation, we need set rd_rel->parttype here before
     * insert pg_class enrty
     */
    if (partTableState != NULL && partTableState->partitionStrategy == PART_STRATEGY_VALUE) {
        new_rel_desc->rd_rel->parttype = PARTTYPE_VALUE_PARTITIONED_RELATION;
    }

    if (partTableState != NULL && partTableState->subPartitionState != NULL) {
        new_rel_desc->rd_rel->parttype = PARTTYPE_SUBPARTITIONED_RELATION;
    }

    /*
     * now create an entry in pg_class for the relation.
     *
     * NOTE: we could get a unique-index failure here, in case someone else is
     * creating the same relation name in parallel but hadn't committed yet
     * when we checked for a duplicate name above.
     */
    AddNewRelationTuple(pg_class_desc,
        new_rel_desc,
        relid,
        new_type_oid,
        reloftypeid,
        ownerid,
        relkind,
        relpersistence,
        PointerGetDatum(relacl),
        reloptions,
        bucketcol,
        (partTableState != NULL));
    /*
     * now add tuples to pg_attribute for the attributes in our new relation.
     */
    AddNewAttributeTuples(
        relid, new_rel_desc->rd_att, relkind, oidislocal, oidinhcount, relhasbucket, relhasuids);
    if (ceLst != NULL) {
        AddNewGsSecEncryptedColumnsTuples(relid, ceLst);
    }
    if (partTableState && (RELKIND_TOASTVALUE != relkind)) {
        pg_partition_desc = heap_open(PartitionRelationId, RowExclusiveLock);

        if (partTableState->partitionStrategy == PART_STRATEGY_VALUE) {
            /* add partitioned table entry to pg_partition */
            addNewPartitionTupleForValuePartitionedTable(
                pg_partition_desc, relname, relid, reltablespace, tupdesc, partTableState, reloptions);
        } else {
            /* add partitioned table entry to pg_partition */
            addNewPartitionTupleForTable(pg_partition_desc, /*RelationData pointer for pg_partition*/
                relname,                                    /*partitioned table's name*/
                relid,                                      /*partitioned table's oid in pg_class*/
                reltablespace,                              /*partitioned table's tablespace*/
                tupdesc,                                    /*partitioned table's tuple descriptor*/
                partTableState,                             /*partition schema of partitioned table*/
                ownerid,                                    /*partitioned table's owner id*/
                reloptions);

            /* add partition entry to pg_partition */
            addNewPartitionTuplesForPartition(pg_partition_desc, /*RelationData pointer for pg_partition*/
                relid,                                           /*partitioned table's oid in pg_class*/
                reltablespace,                                   /*partitioned table's tablespace*/
                relbucketOid,                                    /*partitioned table's bucket info*/
                partTableState,                                  /*partition schema of partitioned table*/
                ownerid,                                         /*partitioned table's owner id*/
                reloptions,
                tupdesc,
                partTableState->partitionStrategy,
                storage_type,
                partLockMode);
        }
    }

    /*
     * Make a dependency link to force the relation to be deleted if its
     * namespace is.  Also make a dependency link to its owner, as well as
     * dependencies for any roles mentioned in the default ACL.
     *
     * For composite types, these dependencies are tracked for the pg_type
     * entry, so we needn't record them here.  Likewise, TOAST tables don't
     * need a namespace dependency (they live in a pinned namespace) nor an
     * owner dependency (they depend indirectly through the parent table), nor
     * should they have any ACL entries.  The same applies for extension
     * dependencies.
     *
     * If it's a temp table, we do not make it an extension member; this
     * prevents the unintuitive result that deletion of the temp table at
     * session end would make the whole extension go away.
     *
     * Also, skip this in bootstrap mode, since we don't make dependencies
     * while bootstrapping.
     */
    if (record_dependce == true && relkind != RELKIND_COMPOSITE_TYPE && relkind != RELKIND_TOASTVALUE &&
        !IsBootstrapProcessingMode()) {
        ObjectAddress myself;

        myself.classId = RelationRelationId;
        myself.objectId = relid;
        myself.objectSubId = 0;

        if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId)
            recordPinnedDependency(&myself);
        else {
            ObjectAddress referenced;

            referenced.classId = NamespaceRelationId;
            referenced.objectId = relnamespace;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

            recordDependencyOnOwner(RelationRelationId, relid, ownerid);

            if (relpersistence != RELPERSISTENCE_TEMP)
                recordDependencyOnCurrentExtension(&myself, false);

            if (reloftypeid) {
                referenced.classId = TypeRelationId;
                referenced.objectId = reloftypeid;
                referenced.objectSubId = 0;
                recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
            }

            if (relacl != NULL) {
                int nnewmembers;
                Oid* newmembers = NULL;

                nnewmembers = aclmembers(relacl, &newmembers);
                updateAclDependencies(RelationRelationId, relid, 0, ownerid, 0, NULL, nnewmembers, newmembers);
            }
        }
    }

    /* Post creation hook for new relation */
    InvokeObjectAccessHook(OAT_POST_CREATE, RelationRelationId, relid, 0, NULL);

    /*
     * Store any supplied constraints and defaults.
     *
     * NB: this may do a CommandCounterIncrement and rebuild the relcache
     * entry, so the relation must be valid and self-consistent at this point.
     * In particular, there are not yet constraints and defaults anywhere.
     */
    StoreConstraints(new_rel_desc, cooked_constraints);

    /*
     * If there's a special on-commit action, remember it
     */
    if (oncommit != ONCOMMIT_NOOP)
        register_on_commit_action(relid, oncommit);

    if (relpersistence == RELPERSISTENCE_UNLOGGED) {
        Assert(RELKIND_IN_RTM);
        heap_create_init_fork(new_rel_desc);
    }

    /*
     * ok, the relation has been cataloged, so close our relations and return
     * the OID of the newly created relation.
     */
    heap_close(new_rel_desc, NoLock); /* do not unlock till end of xact */
    heap_close(pg_class_desc, RowExclusiveLock);

    if (pg_partition_desc) {
        heap_close(pg_partition_desc, RowExclusiveLock);
    }

    return relid;
}

/*
 * Set up an init fork for an unlogged table so that it can be correctly
 * reinitialized on restart.  An immediate sync is required even if the
 * page has been logged, because the write did not go through
 * shared_buffers and therefore a concurrent checkpoint may have moved
 * the redo pointer past our xlog record.  Recovery may as well remove it
 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
 * record. Therefore, logging is necessary even if wal_level=minimal.
 */
void heap_create_init_fork(Relation rel)
{
    RelationOpenSmgr(rel);
    smgrcreate(rel->rd_smgr, INIT_FORKNUM, false);
    log_smgrcreate(&rel->rd_smgr->smgr_rnode.node, INIT_FORKNUM);
    smgrimmedsync(rel->rd_smgr, INIT_FORKNUM);
}

/*
 *		RelationRemoveInheritance
 *
 * Formerly, this routine checked for child relations and aborted the
 * deletion if any were found.	Now we rely on the dependency mechanism
 * to check for or delete child relations.	By the time we get here,
 * there are no children and we need only remove any pg_inherits rows
 * linking this relation to its parent(s).
 */
static void RelationRemoveInheritance(Oid relid)
{
    Relation catalogRelation;
    SysScanDesc scan;
    ScanKeyData key;
    HeapTuple tuple;

    catalogRelation = heap_open(InheritsRelationId, RowExclusiveLock);

    ScanKeyInit(&key, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId, true, NULL, 1, &key);

    while (HeapTupleIsValid(tuple = systable_getnext(scan)))
        simple_heap_delete(catalogRelation, &tuple->t_self);

    systable_endscan(scan);
    heap_close(catalogRelation, RowExclusiveLock);
}

/*
 *		DeleteRelationTuple
 *
 * Remove pg_class row for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
void DeleteRelationTuple(Oid relid)
{
    Relation pg_class_desc;
    HeapTuple tup;

    /* Grab an appropriate lock on the pg_class relation */
    pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);

    tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));

    /* delete the relation tuple from pg_class, and finish up */
    simple_heap_delete(pg_class_desc, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(pg_class_desc, RowExclusiveLock);
}

/*
 *		DeleteAttributeTuples
 *
 * Remove pg_attribute rows for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
void DeleteAttributeTuples(Oid relid)
{
    Relation attrel;
    SysScanDesc scan;
    ScanKeyData key[1];
    HeapTuple atttup;

    /* Grab an appropriate lock on the pg_attribute relation */
    attrel = heap_open(AttributeRelationId, RowExclusiveLock);

    /* Use the index to scan only attributes of the target relation */
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, NULL, 1, key);

    /* Delete all the matching tuples */
    while ((atttup = systable_getnext(scan)) != NULL)
        simple_heap_delete(attrel, &atttup->t_self);

    /* Clean up after the scan */
    systable_endscan(scan);
    heap_close(attrel, RowExclusiveLock);
}

/*
 *		DeleteSystemAttributeTuples
 *
 * Remove pg_attribute rows for system columns of the given relid.
 *
 * Note: this is only used when converting a table to a view.  Views don't
 * have system columns, so we should remove them from pg_attribute.
 */
void DeleteSystemAttributeTuples(Oid relid)
{
    Relation attrel;
    SysScanDesc scan;
    ScanKeyData key[2];
    HeapTuple atttup;

    /* Grab an appropriate lock on the pg_attribute relation */
    attrel = heap_open(AttributeRelationId, RowExclusiveLock);

    /* Use the index to scan only system attributes of the target relation */
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_attribute_attnum, BTLessEqualStrategyNumber, F_INT2LE, Int16GetDatum(0));

    scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, NULL, 2, key);

    /* Delete all the matching tuples */
    while ((atttup = systable_getnext(scan)) != NULL)
        simple_heap_delete(attrel, &atttup->t_self);

    /* Clean up after the scan */
    systable_endscan(scan);
    heap_close(attrel, RowExclusiveLock);
}

/*
 *		RemoveAttributeById
 *
 * This is the guts of ALTER TABLE DROP COLUMN: actually mark the attribute
 * deleted in pg_attribute.  We also remove pg_statistic entries for it.
 * (Everything else needed, such as getting rid of any pg_attrdef entry,
 * is handled by dependency.c.)
 *
 * During inplace or online upgrade, we may need to delete newly added columns
 * to perform upgrade rollback. In that case, we remove the attribute's
 * pg_attribute entry and decrease the relation's relnatts in pg_class.
 * We don't need to rewrite the whole catalog, since there should be no entry
 * of new schema version or, even if there was, they should have been removed
 * during proceeding rollback procedure.
 */
void RemoveAttributeById(Oid relid, AttrNumber attnum)
{
    Relation rel = NULL;
    Relation attr_rel = NULL;
    Relation pgclass = NULL;
    HeapTuple reltuple;
    HeapTuple atttuple;
    Form_pg_attribute attStruct;
    char newattname[NAMEDATALEN];
    bool isRedisDropColumn = false;

    /*
     * Grab an exclusive lock on the target table, which we will NOT release
     * until end of transaction.  (In the simple case where we are directly
     * dropping this column, AlterTableDropColumn already did this ... but
     * when cascading from a drop of some other object, we may not have any
     * lock.)
     */
    rel = relation_open(relid, AccessExclusiveLock);

    attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

    atttuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(atttuple)) /* shouldn't happen */
    {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u", attnum, relid)));
    }
    attStruct = (Form_pg_attribute)GETSTRUCT(atttuple);

    if (RelationIsRedistributeDest(rel) && (attnum > 0 && attnum == rel->rd_att->natts))
        isRedisDropColumn = true;

    if (attnum < 0 || isRedisDropColumn) {
        /* System attribute (probably OID) ... just delete the row */

        simple_heap_delete(attr_rel, &atttuple->t_self);
    } else {
        errno_t rc;
        /* Dropping user attributes is lots harder */

        /* Mark the attribute as dropped */
        attStruct->attisdropped = true;

        /*
         * Set the type OID to invalid.  A dropped attribute's type link
         * cannot be relied on (once the attribute is dropped, the type might
         * be too). Fortunately we do not need the type row --- the only
         * really essential information is the type's typlen and typalign,
         * which are preserved in the attribute's attlen and attalign.  We set
         * atttypid to zero here as a means of catching code that incorrectly
         * expects it to be valid.
         */
        attStruct->atttypid = InvalidOid;

        /* Remove any NOT NULL constraint the column may have */
        attStruct->attnotnull = false;

        /* We don't want to keep stats for it anymore */
        attStruct->attstattarget = 0;

        /*
         * Change the column name to something that isn't likely to conflict
         */
        rc =
            snprintf_s(newattname, sizeof(newattname), sizeof(newattname) - 1, "........pg.dropped.%d........", attnum);
        securec_check_ss(rc, "\0", "\0");
        (void)namestrcpy(&(attStruct->attname), newattname);

        simple_heap_update(attr_rel, &atttuple->t_self, atttuple);

        /* keep the system catalog indexes current */
        CatalogUpdateIndexes(attr_rel, atttuple);
    }

    /*
     * Because updating the pg_attribute row will trigger a relcache flush for
     * the target relation, we need not do anything else to notify other
     * backends of the change.
     */

    heap_close(attr_rel, RowExclusiveLock);

    if (attnum > 0) {
        if (RELATION_IS_GLOBAL_TEMP(rel)) {
            remove_gtt_att_statistic(relid, attnum);
        } else {
            RemoveStatistics<'c'>(relid, attnum);
        }
    }

    /* decrease the relation's relnatts in pg_class */
    if (isRedisDropColumn) {
        pgclass = heap_open(RelationRelationId, RowExclusiveLock);

        reltuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(reltuple))
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));

        ((Form_pg_class)GETSTRUCT(reltuple))->relnatts -= 1;
        simple_heap_update(pgclass, &reltuple->t_self, reltuple);

        /* keep catalog indexes current */
        CatalogUpdateIndexes(pgclass, reltuple);

        heap_freetuple(reltuple);
        heap_close(pgclass, RowExclusiveLock);
    }

    relation_close(rel, NoLock);
}

/*
 *		RemoveAttrDefault
 *
 * If the specified relation/attribute has a default, remove it.
 * (If no default, raise error if complain is true, else return quietly.)
 */
void RemoveAttrDefault(Oid relid, AttrNumber attnum, DropBehavior behavior, bool complain, bool internal)
{
    Relation attrdef_rel;
    ScanKeyData scankeys[2];
    SysScanDesc scan;
    HeapTuple tuple;
    bool found = false;

    attrdef_rel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

    ScanKeyInit(&scankeys[0], Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&scankeys[1], Anum_pg_attrdef_adnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

    scan = systable_beginscan(attrdef_rel, AttrDefaultIndexId, true, NULL, 2, scankeys);

    /* There should be at most one matching tuple, but we loop anyway */
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        ObjectAddress object;

        object.classId = AttrDefaultRelationId;
        object.objectId = HeapTupleGetOid(tuple);
        object.objectSubId = 0;

        performDeletion(&object, behavior, internal ? PERFORM_DELETION_INTERNAL : 0);

        found = true;
    }

    systable_endscan(scan);
    heap_close(attrdef_rel, RowExclusiveLock);

    if (complain && !found)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("could not find attrdef tuple for relation %u attnum %d", relid, attnum)));
}

/*
 *		RemoveAttrDefaultById
 *
 * Remove a pg_attrdef entry specified by OID.	This is the guts of
 * attribute-default removal.  Note it should be called via performDeletion,
 * not directly.
 */
void RemoveAttrDefaultById(Oid attrdefId)
{
    Relation attrdef_rel;
    Relation attr_rel;
    Relation myrel;
    ScanKeyData scankeys[1];
    SysScanDesc scan;
    HeapTuple tuple;
    Oid myrelid;
    AttrNumber myattnum;

    /* Grab an appropriate lock on the pg_attrdef relation */
    attrdef_rel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

    /* Find the pg_attrdef tuple */
    ScanKeyInit(&scankeys[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(attrdefId));

    scan = systable_beginscan(attrdef_rel, AttrDefaultOidIndexId, true, NULL, 1, scankeys);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("could not find tuple for attrdef %u", attrdefId)));

    myrelid = ((Form_pg_attrdef)GETSTRUCT(tuple))->adrelid;
    myattnum = ((Form_pg_attrdef)GETSTRUCT(tuple))->adnum;

    /* Get an exclusive lock on the relation owning the attribute */
    myrel = relation_open(myrelid, AccessExclusiveLock);

    /* Now we can delete the pg_attrdef row */
    simple_heap_delete(attrdef_rel, &tuple->t_self);

    systable_endscan(scan);
    heap_close(attrdef_rel, RowExclusiveLock);

    /* Fix the pg_attribute row */
    attr_rel = heap_open(AttributeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(myrelid), Int16GetDatum(myattnum));
    if (!HeapTupleIsValid(tuple)) /* shouldn't happen */
    {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u", myattnum, myrelid)));
    }

    ((Form_pg_attribute)GETSTRUCT(tuple))->atthasdef = false;

    simple_heap_update(attr_rel, &tuple->t_self, tuple);

    /* keep the system catalog indexes current */
    CatalogUpdateIndexes(attr_rel, tuple);

    /*
     * Our update of the pg_attribute row will force a relcache rebuild, so
     * there's nothing else to do here.
     */
    heap_close(attr_rel, RowExclusiveLock);

    /* Keep lock on attribute's rel until end of xact */
    relation_close(myrel, NoLock);
}

/*
 * Remove error info table file for bulkload
 * there are problems in a rollback transaction
 */
static void RemoveBulkLoadErrorTableFile(Oid databaseid, Oid relid)
{
    char filepath[PATH_MAX + 1];
    getErrorTableFilePath(filepath, PATH_MAX + 1, databaseid, relid);
    struct stat st;
    // is there an error table file
    if (0 == stat(filepath, &st)) {
        if (unlink(filepath)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", filepath)));
        }
    }
}

#ifdef ENABLE_MOT
static void MotFdwDropForeignRelation(Relation rel, FdwRoutine* fdwRoutine)
{
    /* Forward drop stmt to MOT FDW. */
    if (fdwRoutine->GetFdwType != NULL && fdwRoutine->GetFdwType() == MOT_ORC && fdwRoutine->ValidateTableDef != NULL) {
        DropForeignStmt stmt;
        stmt.type = T_DropForeignStmt;
        stmt.relkind = RELKIND_RELATION;
        stmt.reloid = RelationGetRelid(rel);
        stmt.indexoid = 0;
        stmt.name = rel->rd_rel->relname.data;

        fdwRoutine->ValidateTableDef((Node*)&stmt);
    }
}
#endif

/*
 * heap_drop_with_catalog	- removes specified relation from catalogs
 *
 * Note that this routine is not responsible for dropping objects that are
 * linked to the pg_class entry via dependencies (for example, indexes and
 * constraints).  Those are deleted by the dependency-tracing logic in
 * dependency.c before control gets here.  In general, therefore, this routine
 * should never be called directly; go through performDeletion() instead.
 */
void heap_drop_with_catalog(Oid relid)
{
    Relation rel;

    /*
     * Open and lock the relation.
     */
    rel = relation_open(relid, AccessExclusiveLock);
    if (RelationUsesSpaceType(rel->rd_rel->relpersistence) == SP_TEMP) {
        make_tmptable_cache_key(rel->rd_rel->relfilenode);
    }

    /*
     * There can no longer be anyone *else* touching the relation, but we
     * might still have open queries or cursors, or pending trigger events, in
     * our own session.
     */
    CheckTableNotInUse(rel, "DROP TABLE");

    /*
     * This effectively deletes all rows in the table, and may be done in a
     * serializable transaction.  In that case we must record a rw-conflict in
     * to this transaction from each transaction holding a predicate lock on
     * the table.
     */
    CheckTableForSerializableConflictIn(rel);

    /*
     * Delete pg_foreign_table tuple first.
     */
    bool flag = rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE || rel->rd_rel->relkind == RELKIND_STREAM;
    if (flag) {
        Relation foreignRel;
        HeapTuple tuple;

        foreignRel = heap_open(ForeignTableRelationId, RowExclusiveLock);

        tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for foreign table %u", relid)));

        /* remove error table file */
        RemoveBulkLoadErrorTableFile(foreignRel->rd_node.dbNode, relid);

        simple_heap_delete(foreignRel, &tuple->t_self);

        ReleaseSysCache(tuple);
        heap_close(foreignRel, RowExclusiveLock);

        /* @hdfs
         * When we drop a foreign partition table, we need to delete the corresponding
         * tuple in pg_partition.
         */
        FdwRoutine* fdwRoutine = GetFdwRoutineByRelId(relid);
        if (NULL != fdwRoutine->PartitionTblProcess) {
            fdwRoutine->PartitionTblProcess(NULL, relid, HDFS_DROP_PARTITIONED_FOREIGNTBL);
        }

#ifdef ENABLE_MOT
        /* Forward drop stmt to MOT FDW. */
        MotFdwDropForeignRelation(rel, fdwRoutine);
#endif
    }

    /* drop enty for pg_partition */
    if (RelationIsValuePartitioned(rel)) {
        HeapTuple partTuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, relid);
        if (partTuple) {
            Relation pg_partitionRel = heap_open(PartitionRelationId, RowExclusiveLock);
            simple_heap_delete(pg_partitionRel, &partTuple->t_self);

            heap_freetuple(partTuple);
            heap_close(pg_partitionRel, RowExclusiveLock);
        } else
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg(
                        "Catalog table pg_partition may get trashed on table %s as it is not consitant with pg_class",
                        RelationGetRelationName(rel))));
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        heapDropPartitionTable(rel);
    }

    /* We allow to drop global temp table only this session use it */
    CheckGttTableInUse(rel);

    /*
     * When dropping temp objects, we need further check whether current datanode is
     * suffered from an unclean shutdown without gsql reconnect, as in this case the
     * context of temp backend thread is inconsistant with other datanodes (timelineID),
     * in order to prevent user further operator temp object, we error-out the DROP
     * statement to tell user to have to re-connect server.
     */
    if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
    } else if (RelationIsLocalTemp(rel)) {
        if (!validateTempNamespace(u_sess->catalog_cxt.myTempNamespace))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                    errmsg("Temp tables are invalid because datanode %s restart. "
                           "Quit your session to clean invalid temp tables.",
                        g_instance.attr.attr_common.PGXCNodeName)));
    }

    /*
     * Schedule unlinking of the relation's physical files at commit.
     */
    if (rel->rd_rel->relkind != RELKIND_VIEW && rel->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
        rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE && rel->rd_rel->relkind != RELKIND_STREAM && 
        rel->rd_rel->relkind != RELKIND_CONTQUERY) {
        RelationDropStorage(rel);
    }

    /* Recode time of delete relation. */
    PgObjectType drop_type = GetPgObjectTypePgClass(rel->rd_rel->relkind);
    if (drop_type != OBJECT_TYPE_INVALID) {
        DeletePgObject(rel->rd_id, drop_type);
    }

    /*
     * Close relcache entry, but *keep* AccessExclusiveLock on the relation
     * until transaction commit.  This ensures no one else will try to do
     * something with the doomed relation.
     */
    if (ISMLOG(RelationGetForm(rel)->relname.data)) {
        char *base_relid_str = RelationGetForm(rel)->relname.data + MLOGLEN;
        Oid base_relid = atoi(base_relid_str);
        if (OidIsValid(base_relid)) {
            CacheInvalidateRelcacheByRelid(base_relid);
        }
    }
    relation_close(rel, NoLock);

    /*
     * Forget any ON COMMIT action for the rel
     */
    remove_on_commit_action(relid);

    /*
     * Flush the relation from the relcache.  We want to do this before
     * starting to remove catalog entries, just to be certain that no relcache
     * entry rebuild will happen partway through.  (That should not really
     * matter, since we don't do CommandCounterIncrement here, but let's be
     * safe.)
     */
    RelationForgetRelation(relid);

    /*
     * remove inheritance information
     */
    RelationRemoveInheritance(relid);

    /*
     * delete statistics
     */
    RemoveStatistics<'c'>(relid, 0);

    /*
     * delete hotkeys
     */
    pgstat_send_cleanup_hotkeys(InvalidOid, relid);

    /*
     * delete attribute tuples
     */
    DeleteAttributeTuples(relid);

    /*
     * delete relation tuple
     */
    DeleteRelationTuple(relid);
}

/*
 * Store a default expression for column attnum of relation rel.
 */
void StoreAttrDefault(Relation rel, AttrNumber attnum, Node* expr, char generatedCol)
{
    char* adbin = NULL;
    char* adsrc = NULL;
    Relation adrel;
    HeapTuple tuple;
    Datum values[Natts_pg_attrdef];
    Relation attrrel;
    HeapTuple atttup;
    Form_pg_attribute attStruct;
    Oid attrdefOid;
    ObjectAddress colobject, defobject;

    /*
     * Flatten expression to string form for storage.
     */
    adbin = nodeToString(expr);

    /*
     * Also deparse it to form the mostly-obsolete adsrc field.
     */
    adsrc = deparse_expression(
        expr, deparse_context_for(RelationGetRelationName(rel), RelationGetRelid(rel)), false, false);

    /*
     * Make the pg_attrdef entry.
     */
    values[Anum_pg_attrdef_adrelid - 1] = RelationGetRelid(rel);
    values[Anum_pg_attrdef_adnum - 1] = attnum;
    values[Anum_pg_attrdef_adbin - 1] = CStringGetTextDatum(adbin);
    values[Anum_pg_attrdef_adsrc - 1] = CStringGetTextDatum(adsrc);
    if (t_thrd.proc->workingVersionNum >= GENERATED_COL_VERSION_NUM) {
        values[Anum_pg_attrdef_adgencol - 1] = CharGetDatum(generatedCol);
    }

    adrel = heap_open(AttrDefaultRelationId, RowExclusiveLock);

    tuple = heap_form_tuple(adrel->rd_att, values, u_sess->catalog_cxt.nulls);
    attrdefOid = simple_heap_insert(adrel, tuple);

    CatalogUpdateIndexes(adrel, tuple);

    defobject.classId = AttrDefaultRelationId;
    defobject.objectId = attrdefOid;
    defobject.objectSubId = 0;

    heap_close(adrel, RowExclusiveLock);

    /* now can free some of the stuff allocated above */
    pfree(DatumGetPointer(values[Anum_pg_attrdef_adbin - 1]));
    pfree(DatumGetPointer(values[Anum_pg_attrdef_adsrc - 1]));
    heap_freetuple(tuple);
    pfree(adbin);
    pfree(adsrc);

    /*
     * Update the pg_attribute entry for the column to show that a default
     * exists.
     */
    attrrel = heap_open(AttributeRelationId, RowExclusiveLock);
    atttup = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(RelationGetRelid(rel)), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(atttup)) {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u", attnum, RelationGetRelid(rel))));
    }
    attStruct = (Form_pg_attribute)GETSTRUCT(atttup);
    if (!attStruct->atthasdef) {
        attStruct->atthasdef = true;
        simple_heap_update(attrrel, &atttup->t_self, atttup);
        /* keep catalog indexes current */
        CatalogUpdateIndexes(attrrel, atttup);
    }
    heap_close(attrrel, RowExclusiveLock);
    heap_freetuple(atttup);

    /*
     * Make a dependency so that the pg_attrdef entry goes away if the column
     * (or whole table) is deleted.
     */
    colobject.classId = RelationRelationId;
    colobject.objectId = RelationGetRelid(rel);
    colobject.objectSubId = attnum;

    recordDependencyOn(&defobject, &colobject, DEPENDENCY_AUTO);

    /*
     * Record dependencies on objects used in the expression, too.
     */
    if (generatedCol == ATTRIBUTE_GENERATED_STORED)
    {
        /*
         * Generated column: Dropping anything that the generation expression
         * refers to automatically drops the generated column.
         */
        recordDependencyOnSingleRelExpr(&colobject, expr, RelationGetRelid(rel),
                                        DEPENDENCY_AUTO,
                                        DEPENDENCY_AUTO);
    } else {
        /*
         * Normal default: Dropping anything that the default refers to
         * requires CASCADE and drops the default only.
         */
        recordDependencyOnExpr(&defobject, expr, NIL, DEPENDENCY_NORMAL);
    }
}

/*
 * Store a check-constraint expression for the given relation.
 *
 * Caller is responsible for updating the count of constraints
 * in the pg_class entry for the relation.
 */
static void StoreRelCheck(
    Relation rel, const char* ccname, Node* expr, bool is_validated, bool is_local, int inhcount, bool is_no_inherit)
{
    char* ccbin = NULL;
    char* ccsrc = NULL;
    List* varList = NIL;
    int keycount;
    int16* attNos = NULL;

    /*
     * Flatten expression to string form for storage.
     */
    ccbin = nodeToString(expr);

    /*
     * Also deparse it to form the mostly-obsolete consrc field.
     */
    ccsrc = deparse_expression(
        expr, deparse_context_for(RelationGetRelationName(rel), RelationGetRelid(rel)), false, false);

    /*
     * Find columns of rel that are used in expr
     *
     * NB: pull_var_clause is okay here only because we don't allow subselects
     * in check constraints; it would fail to examine the contents of
     * subselects.
     */
    varList = pull_var_clause(expr, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
    keycount = list_length(varList);

    if (keycount > 0) {
        ListCell* vl = NULL;
        int i = 0;

        attNos = (int16*)palloc(keycount * sizeof(int16));
        foreach (vl, varList) {
            Var* var = (Var*)lfirst(vl);
            int j;

            for (j = 0; j < i; j++)
                if (attNos[j] == var->varattno)
                    break;
            if (j == i)
                attNos[i++] = var->varattno;
        }
        keycount = i;
    } else
        attNos = NULL;

    /*
     * Create the Check Constraint
     */
    (void)CreateConstraintEntry(ccname, /* Constraint Name */
        RelationGetNamespace(rel),      /* namespace */
        CONSTRAINT_CHECK,               /* Constraint Type */
        false,                          /* Is Deferrable */
        false,                          /* Is Deferred */
        is_validated,
        RelationGetRelid(rel), /* relation */
        attNos,                /* attrs in the constraint */
        keycount,              /* # key attrs in the constraint */
        keycount,              /* # total attrs in the constraint */
        InvalidOid,            /* not a domain constraint */
        InvalidOid,            /* no associated index */
        InvalidOid,            /* Foreign key fields */
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        ' ',
        ' ',
        ' ',
        NULL,          /* not an exclusion constraint */
        expr,          /* Tree form of check constraint */
        ccbin,         /* Binary form of check constraint */
        ccsrc,         /* Source form of check constraint */
        is_local,      /* conislocal */
        inhcount,      /* coninhcount */
        is_no_inherit, /* connoinherit */
        NULL);         /* @hdfs softconstraint info */

    pfree(ccbin);
    pfree(ccsrc);
}

/*
 * Store defaults and constraints (passed as a list of CookedConstraint).
 *
 * NOTE: only pre-cooked expressions will be passed this way, which is to
 * say expressions inherited from an existing relation.  Newly parsed
 * expressions can be added later, by direct calls to StoreAttrDefault
 * and StoreRelCheck (see AddRelationNewConstraints()).
 */
static void StoreConstraints(Relation rel, List* cooked_constraints)
{
    int numchecks = 0;
    ListCell* lc = NULL;

    if (cooked_constraints == NULL)
        return; /* nothing to do */

    /*
     * Deparsing of constraint expressions will fail unless the just-created
     * pg_attribute tuples for this relation are made visible.	So, bump the
     * command counter.  CAUTION: this will cause a relcache entry rebuild.
     */
    CommandCounterIncrement();

    foreach (lc, cooked_constraints) {
        CookedConstraint* con = (CookedConstraint*)lfirst(lc);

        switch (con->contype) {
            case CONSTR_DEFAULT:
                StoreAttrDefault(rel, con->attnum, con->expr, 0);
                break;
            case CONSTR_GENERATED:
                StoreAttrDefault(rel, con->attnum, con->expr, ATTRIBUTE_GENERATED_STORED);
                break;
            case CONSTR_CHECK:
                StoreRelCheck(
                    rel, con->name, con->expr, !con->skip_validation, con->is_local, con->inhcount, con->is_no_inherit);
                numchecks++;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PARAMETER),
                        errmsg("unrecognized constraint type: %d", (int)con->contype)));
        }
    }

    if (numchecks > 0)
        SetRelationNumChecks(rel, numchecks);
}

/*
 * AddRelationNewConstraints
 *
 * Add new column default expressions and/or constraint check expressions
 * to an existing relation.  This is defined to do both for efficiency in
 * DefineRelation, but of course you can do just one or the other by passing
 * empty lists.
 *
 * rel: relation to be modified
 * newColDefaults: list of RawColumnDefault structures
 * newConstraints: list of Constraint nodes
 * allow_merge: TRUE if check constraints may be merged with existing ones
 * is_local: TRUE if definition is local, FALSE if it's inherited
 *
 * All entries in newColDefaults will be processed.  Entries in newConstraints
 * will be processed only if they are CONSTR_CHECK type.
 *
 * Returns a list of CookedConstraint nodes that shows the cooked form of
 * the default and constraint expressions added to the relation.
 *
 * NB: caller should have opened rel with AccessExclusiveLock, and should
 * hold that lock till end of transaction.	Also, we assume the caller has
 * done a CommandCounterIncrement if necessary to make the relation's catalog
 * tuples visible.
 */
List* AddRelationNewConstraints(
    Relation rel, List* newColDefaults, List* newConstraints, bool allow_merge, bool is_local)
{
    List* cookedConstraints = NIL;
    TupleDesc tupleDesc;
    TupleConstr* oldconstr = NULL;
    int numoldchecks;
    ParseState* pstate = NULL;
    RangeTblEntry* rte = NULL;
    int numchecks;
    List* checknames = NIL;
    ListCell* cell = NULL;
    Node* expr = NULL;
    CookedConstraint* cooked = NULL;

    /*
     * Get info about existing constraints.
     */
    tupleDesc = RelationGetDescr(rel);
    oldconstr = tupleDesc->constr;
    if (oldconstr != NULL)
        numoldchecks = oldconstr->num_check;
    else
        numoldchecks = 0;

    /*
     * Create a dummy ParseState and insert the target relation as its sole
     * rangetable entry.  We need a ParseState for transformExpr.
     */
    pstate = make_parsestate(NULL);
    rte = addRangeTableEntryForRelation(pstate, rel, NULL, false, true);
    addRTEtoQuery(pstate, rte, true, true, true);

    pstate->p_rawdefaultlist = newColDefaults;

    /*
     * Process column default expressions.
     */
    foreach (cell, newColDefaults) {
        RawColumnDefault* colDef = (RawColumnDefault*)lfirst(cell);
        Form_pg_attribute atp = rel->rd_att->attrs[colDef->attnum - 1];

        expr = cookDefault(pstate, colDef->raw_default, atp->atttypid, atp->atttypmod, NameStr(atp->attname),
            colDef->generatedCol);

        /*
         * If the expression is just a NULL constant, we do not bother to make
         * an explicit pg_attrdef entry, since the default behavior is
         * equivalent.  This applies to column defaults, but not for
         * generation expressions.
         *
         * Note a nonobvious property of this test: if the column is of a
         * domain type, what we'll get is not a bare null Const but a
         * CoerceToDomain expr, so we will not discard the default.  This is
         * critical because the column default needs to be retained to
         * override any default that the domain might have.
         */
        if (expr == NULL || (!colDef->generatedCol && IsA(expr, Const) && ((Const *)expr)->constisnull))
            continue;

        StoreAttrDefault(rel, colDef->attnum, expr, colDef->generatedCol);

        cooked = (CookedConstraint*)palloc(sizeof(CookedConstraint));
        cooked->contype = CONSTR_DEFAULT;
        cooked->name = NULL;
        cooked->attnum = colDef->attnum;
        cooked->expr = expr;
        cooked->skip_validation = false;
        cooked->is_local = is_local;
        cooked->inhcount = is_local ? 0 : 1;
        cooked->is_no_inherit = false;
        cookedConstraints = lappend(cookedConstraints, cooked);
    }

    pstate->p_rawdefaultlist = NIL;
    /*
     * Process constraint expressions.
     */
    numchecks = numoldchecks;
    checknames = NIL;
    foreach (cell, newConstraints) {
        Constraint* cdef = (Constraint*)lfirst(cell);
        char* ccname = NULL;

        if (cdef->contype != CONSTR_CHECK)
            continue;

        if (cdef->raw_expr != NULL) {
            Assert(cdef->cooked_expr == NULL);

            /*
             * Transform raw parsetree to executable expression, and verify
             * it's valid as a CHECK constraint.
             */
            expr = cookConstraint(pstate, cdef->raw_expr, RelationGetRelationName(rel));
        } else {
            Assert(cdef->cooked_expr != NULL);

            /*
             * Here, we assume the parser will only pass us valid CHECK
             * expressions, so we do no particular checking.
             */
            expr = (Node*)stringToNode(cdef->cooked_expr);
        }

        /*
         * Check name uniqueness, or generate a name if none was given.
         */
        if (cdef->conname != NULL) {
            ListCell* cell2 = NULL;

            ccname = cdef->conname;
            /* Check against other new constraints */
            /* Needed because we don't do CommandCounterIncrement in loop */
            foreach (cell2, checknames) {
                if (strcmp((char*)lfirst(cell2), ccname) == 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("check constraint \"%s\" already exists", ccname)));
            }

            /* save name for future checks */
            checknames = lappend(checknames, ccname);

            /*
             * Check against pre-existing constraints.	If we are allowed to
             * merge with an existing constraint, there's no more to do here.
             * (We omit the duplicate constraint from the result, which is
             * what ATAddCheckConstraint wants.)
             */
            if (MergeWithExistingConstraint(rel, ccname, expr, allow_merge, is_local, cdef->is_no_inherit))
                continue;
        } else {
            /*
             * When generating a name, we want to create "tab_col_check" for a
             * column constraint and "tab_check" for a table constraint.  We
             * no longer have any info about the syntactic positioning of the
             * constraint phrase, so we approximate this by seeing whether the
             * expression references more than one column.	(If the user
             * played by the rules, the result is the same...)
             *
             * Note: pull_var_clause() doesn't descend into sublinks, but we
             * eliminated those above; and anyway this only needs to be an
             * approximate answer.
             */
            List* vars = NIL;
            char* colname = NULL;

            vars = pull_var_clause(expr, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);

            /* eliminate duplicates */
            vars = list_union(NIL, vars);

            if (list_length(vars) == 1)
                colname = get_attname(RelationGetRelid(rel), ((Var*)linitial(vars))->varattno);
            else
                colname = NULL;

            ccname = ChooseConstraintName(
                RelationGetRelationName(rel), colname, "check", RelationGetNamespace(rel), checknames);

            /* save name for future checks */
            checknames = lappend(checknames, ccname);
        }

        /*
         * OK, store it.
         */
        StoreRelCheck(rel, ccname, expr, !cdef->skip_validation, is_local, is_local ? 0 : 1, cdef->is_no_inherit);

        numchecks++;

        cooked = (CookedConstraint*)palloc(sizeof(CookedConstraint));
        cooked->contype = CONSTR_CHECK;
        cooked->name = ccname;
        cooked->attnum = 0;
        cooked->expr = expr;
        cooked->skip_validation = cdef->skip_validation;
        cooked->is_local = is_local;
        cooked->inhcount = is_local ? 0 : 1;
        cooked->is_no_inherit = cdef->is_no_inherit;
        cookedConstraints = lappend(cookedConstraints, cooked);
    }

    /*
     * Update the count of constraints in the relation's pg_class tuple. We do
     * this even if there was no change, in order to ensure that an SI update
     * message is sent out for the pg_class tuple, which will force other
     * backends to rebuild their relcache entries for the rel. (This is
     * critical if we added defaults but not constraints.)
     */
    SetRelationNumChecks(rel, numchecks);

    /*
     * Lock the sequence that the constranits depend, we may lock other object
     * in the farther.
     */
    LockSeqConstraints(rel, cookedConstraints);

    return cookedConstraints;
}

/*
 * Check for a pre-existing check constraint that conflicts with a proposed
 * new one, and either adjust its conislocal/coninhcount settings or throw
 * error as needed.
 *
 * Returns TRUE if merged (constraint is a duplicate), or FALSE if it's
 * got a so-far-unique name, or throws error if conflict.
 *
 * XXX See MergeConstraintsIntoExisting too if you change this code.
 */
static bool MergeWithExistingConstraint(
    Relation rel, char* ccname, Node* expr, bool allow_merge, bool is_local, bool is_no_inherit)
{
    bool found = false;
    Relation conDesc = NULL;
    SysScanDesc conscan = NULL;
    ScanKeyData skey[2];
    HeapTuple tup = NULL;

    /* Search for a pg_constraint entry with same name and relation */
    conDesc = heap_open(ConstraintRelationId, ShareUpdateExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(ccname));

    ScanKeyInit(&skey[1],
        Anum_pg_constraint_connamespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetNamespace(rel)));

    conscan = systable_beginscan(conDesc, ConstraintNameNspIndexId, true, NULL, 2, skey);

    while (HeapTupleIsValid(tup = systable_getnext(conscan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);

        if (con->conrelid == RelationGetRelid(rel)) {
            /* Found it.  Conflicts if not identical check constraint */
            if (con->contype == CONSTRAINT_CHECK) {
                Datum val;
                bool isnull = false;

                val = fastgetattr(tup, Anum_pg_constraint_conbin, conDesc->rd_att, &isnull);
                if (isnull)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("null conbin for rel %s", RelationGetRelationName(rel))));
                if (equal(expr, stringToNode(TextDatumGetCString(val))))
                    found = true;
            }
            if (!found || !allow_merge)
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("constraint \"%s\" for relation \"%s\" already exists",
                            ccname,
                            RelationGetRelationName(rel))));

            tup = heap_copytuple(tup);
            con = (Form_pg_constraint)GETSTRUCT(tup);

            /* If the constraint is "no inherit" then cannot merge */
            if (con->connoinherit)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("constraint \"%s\" conflicts with non-inherited constraint on relation \"%s\"",
                            ccname,
                            RelationGetRelationName(rel))));

            if (is_local)
                con->conislocal = true;
            else
                con->coninhcount++;
            if (is_no_inherit) {
                Assert(is_local);
                con->connoinherit = true;
            }
            /* OK to update the tuple */
            ereport(NOTICE, (errmsg("merging constraint \"%s\" with inherited definition", ccname)));
            simple_heap_update(conDesc, &tup->t_self, tup);
            CatalogUpdateIndexes(conDesc, tup);
            break;
        }
    }

    systable_endscan(conscan);
    heap_close(conDesc, ShareUpdateExclusiveLock);

    return found;
}

/*
 * Update the count of constraints in the relation's pg_class tuple.
 *
 * Caller had better hold exclusive lock on the relation.
 *
 * An important side effect is that a SI update message will be sent out for
 * the pg_class tuple, which will force other backends to rebuild their
 * relcache entries for the rel.  Also, this backend will rebuild its
 * own relcache entry at the next CommandCounterIncrement.
 */
static void SetRelationNumChecks(Relation rel, int numchecks)
{
    Relation relrel;
    HeapTuple reltup;
    Form_pg_class relStruct;

    relrel = heap_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(rel))));
    relStruct = (Form_pg_class)GETSTRUCT(reltup);

    if (relStruct->relchecks != numchecks) {
        relStruct->relchecks = numchecks;

        simple_heap_update(relrel, &reltup->t_self, reltup);

        /* keep catalog indexes current */
        CatalogUpdateIndexes(relrel, reltup);
    } else {
        /* Skip the disk update, but force relcache inval anyway */
        CacheInvalidateRelcache(rel);
    }

    heap_freetuple(reltup);
    heap_close(relrel, RowExclusiveLock);
}

static bool ReferenceGenerated(const List *rawdefaultlist, AttrNumber attnum)
{
    ListCell *cell = NULL;

    foreach (cell, rawdefaultlist) {
        RawColumnDefault *colDef = (RawColumnDefault *)lfirst(cell);
        if (colDef->attnum == attnum && colDef->generatedCol) {
            return true;
        }
    }

    return false;
}

/*
 * Check for references to generated columns
 */
static bool CheckNestedGeneratedWalker(Node *node, ParseState *context)
{
    ParseState *pstate = context;
    if (node == NULL) {
        return false;
    } else if (IsA(node, Var)) {
        Var *var = (Var *)node;
        RangeTblEntry *rte = rt_fetch(var->varno, pstate->p_rtable);
        Oid relid = rte->relid;
        AttrNumber attnum = var->varattno;

        if (OidIsValid(relid) && AttributeNumberIsValid(attnum) &&
            (ReferenceGenerated(pstate->p_rawdefaultlist, attnum) || GetGenerated(relid, attnum))) {
            ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("cannot use generated column \"%s\" in column generation expression",
                get_attname(relid, attnum)),
                errdetail("A generated column cannot reference another generated column."),
                parser_errposition(pstate, var->location)));
        }

        return false;
    } else {
        return expression_tree_walker(node, (bool (*)())CheckNestedGeneratedWalker, context);
    }
}

static bool CheckNestedGenerated(ParseState *pstate, Node *node)
{
    return CheckNestedGeneratedWalker(node, pstate);
}

Node* parseParamRef(ParseState* pstate, ParamRef* pref)
{
    Node* param_expr = NULL;
    if (likely(u_sess->parser_cxt.param_info != NULL)) {
        ParamListInfo params_info = (ParamListInfo)u_sess->parser_cxt.param_info;
        int i = pref->number - 1;
        char* str_expr = Datum_to_string(params_info->params[i].value, params_info->params[i].ptype, params_info->params[i].isnull);
        Value* val = (Value*)palloc0(sizeof(Value));
        val->type = T_String;
        val->val.str = str_expr;
        if (params_info->params[i].isnull)
            val->type = T_Null;
        param_expr = (Node*)make_const(pstate, val, 0);
        pfree_ext(val);
        if (pref->number == params_info->numParams && u_sess->parser_cxt.ddl_pbe_context != NULL) {
            MemoryContextDelete(u_sess->parser_cxt.ddl_pbe_context);
            if (((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) && IsConnFromCoord())) {
                u_sess->parser_cxt.param_info = NULL;
            }
            u_sess->parser_cxt.ddl_pbe_context = NULL;
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("u_sess->parser_cxt.param_info should not be NULL")));
    }
    return param_expr;
}

/*
 * Take a raw default and convert it to a cooked format ready for
 * storage.
 *
 * Parse state should be set up to recognize any vars that might appear
 * in the expression.  (Even though we plan to reject vars, it's more
 * user-friendly to give the correct error message than "unknown var".)
 *
 * If atttypid is not InvalidOid, coerce the expression to the specified
 * type (and typmod atttypmod).   attname is only needed in this case:
 * it is used in the error message, if any.
 */
Node *cookDefault(ParseState *pstate, Node *raw_default, Oid atttypid, int32 atttypmod, char *attname,
    char generatedCol)
{
    Node* expr = NULL;

    Assert(raw_default != NULL);

    /* Version control for DDL PBE */
    if (t_thrd.proc->workingVersionNum >= DDL_PBE_VERSION_NUM)
        pstate->p_paramref_hook = parseParamRef;
    /*
     * Transform raw parsetree to executable expression.
     */
    pstate->p_expr_kind = generatedCol ? EXPR_KIND_GENERATED_COLUMN : EXPR_KIND_COLUMN_DEFAULT;
    expr = transformExpr(pstate, raw_default);
    pstate->p_expr_kind = EXPR_KIND_NONE;

    if (generatedCol == ATTRIBUTE_GENERATED_STORED)
    {
        (void)CheckNestedGenerated(pstate, expr);

        if (contain_mutable_functions(expr))
            ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("generation expression is not immutable")));
    }

    /* Version control for DDL PBE */
    if (t_thrd.proc->workingVersionNum >= DDL_PBE_VERSION_NUM)
        pstate->p_paramref_hook = NULL;
    /*
     * Make sure default expr does not refer to any vars.
     */
    if (contain_var_clause(expr) && !generatedCol)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("cannot use column references in default expression")));

    /*
     * Make sure default expr does not refer to rownum.
     */
#ifndef ENABLE_MULTIPLE_NODES	 
    ExcludeRownumExpr(pstate, expr);
#endif
    /*
     * It can't return a set either.
     */
    if (expression_returns_set(expr))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("%s expression must not return a set", generatedCol ? "generated column" : "default")));

    /*
     * No subplans or aggregates, either...
     */
    if (pstate->p_hasSubLinks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot use subquery in %s expression", generatedCol ? "generated column" : "default")));
    if (pstate->p_hasAggs)
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
            errmsg("cannot use aggregate function in %s expression", generatedCol ? "generated column" : "default")));
    if (pstate->p_hasWindowFuncs)
        ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR),
            errmsg("cannot use window function in %s expression", generatedCol ? "generated column" : "default")));

    /*
     * Coerce the expression to the correct type and typmod, if given. This
     * should match the parser's processing of non-defaulted expressions ---
     * see transformAssignedExpr().
     */
    if (OidIsValid(atttypid)) {
        Oid type_id = exprType(expr);

        expr = coerce_to_target_type(pstate, expr, type_id, atttypid, atttypmod, COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST, -1);
        if (expr == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("column \"%s\" is of type %s but %s expression is of type %s", attname, format_type_be(atttypid),
                generatedCol ? "generated column" : "default", format_type_be(type_id)),
                errhint("You will need to rewrite or cast the expression.")));
    }

    /*
     * Finally, take care of collations in the finished expression.
     */
    assign_expr_collations(pstate, expr);

    return expr;
}

/*
 * Take a raw CHECK constraint expression and convert it to a cooked format
 * ready for storage.
 *
 * Parse state must be set up to recognize any vars that might appear
 * in the expression.
 */
static Node* cookConstraint(ParseState* pstate, Node* raw_constraint, char* relname)
{
    Node* expr = NULL;

    /*
     * Transform raw parsetree to executable expression.
     */
    expr = transformExpr(pstate, raw_constraint);

    /*
     * Make sure it yields a boolean result.
     */
    expr = coerce_to_boolean(pstate, expr, "CHECK");

    /*
     * Take care of collations.
     */
    assign_expr_collations(pstate, expr);

    /*
     * Make sure no outside relations are referred to.
     */
    if (list_length(pstate->p_rtable) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("only table \"%s\" can be referenced in check constraint", relname)));

    /*
     * No subplans or aggregates, either...
     */
    if (pstate->p_hasSubLinks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in check constraint")));
    if (pstate->p_hasAggs)
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in check constraint")));
    if (pstate->p_hasWindowFuncs)
        ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("cannot use window function in check constraint")));

    return expr;
}

/*
 * RemoveStatistics --- remove entries in pg_statistic for a rel or column
 *
 * If attnum is zero, remove all entries for rel;
 * else remove only the one(s) for that column.
 *
 * This function was enhanced by multi-column statistics.
 */
template <char starelkind>
void RemoveStatistics(Oid relid, AttrNumber attnum)
{
    Relation pgstatistic;
    SysScanDesc scan;
    ScanKeyData key[3];
    int nkeys;
    HeapTuple tuple;

    /* We want to find all statistics for this table */
    ScanKeyInit(&key[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    if (starelkind == STARELKIND_CLASS) {
        ScanKeyInit(
            &key[1], Anum_pg_statistic_starelkind, BTEqualStrategyNumber, F_CHAREQ, ObjectIdGetDatum(STARELKIND_CLASS));
    }
    /* retain this interface for partition */
    else if (starelkind == STARELKIND_PARTITION) {
        ScanKeyInit(&key[1],
            Anum_pg_statistic_starelkind,
            BTEqualStrategyNumber,
            F_CHAREQ,
            ObjectIdGetDatum(STARELKIND_PARTITION));
    }
    /* invalid starelkind, report ERROR */
    else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER), errmsg("invalid starelkind for pg_statistic")));
    }

    if (0 == attnum) {
        nkeys = 2;
    } else {
        ScanKeyInit(&key[2], Anum_pg_statistic_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));
        nkeys = 3;
    }

    pgstatistic = heap_open(StatisticRelationId, RowExclusiveLock);
    scan = systable_beginscan(pgstatistic, StatisticRelidKindAttnumInhIndexId, true, NULL, nkeys, key);

    /* we must loop even when attnum != 0, in case of inherited stats */
    while (HeapTupleIsValid(tuple = systable_getnext(scan)))
        simple_heap_delete(pgstatistic, &tuple->t_self);

    systable_endscan(scan);
    heap_close(pgstatistic, RowExclusiveLock);

    /*
     * NOTE: scan key is reused with the first two keys. Since the two keys have
     * the same attrnum in pg_statistic and pg_statistic_ext, we don't need to change it
     */
    nkeys = 2;
    pgstatistic = heap_open(StatisticExtRelationId, RowExclusiveLock);
    scan = systable_beginscan(pgstatistic, StatisticExtRelidKindInhKeyIndexId, true, NULL, nkeys, key);

    /* we must loop here, in case of inherited stats and extended stats */
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        if (0 == attnum)
            simple_heap_delete(pgstatistic, &tuple->t_self);
        else {
            bool isnull = false;
            Datum attnums = heap_getattr(tuple, Anum_pg_statistic_ext_stakey, RelationGetDescr(pgstatistic), &isnull);

            int2vector* keys = (int2vector*)DatumGetPointer(attnums);
            int key_nums = keys->dim1;
            int i;
            for (i = 0; i < key_nums; i++) {
                if (attnum == keys->values[i])
                    break;
            }
            /* found multi-column stat that contains certain attnum */
            if (i < key_nums)
                simple_heap_delete(pgstatistic, &tuple->t_self);
        }
    }

    systable_endscan(scan);
    heap_close(pgstatistic, RowExclusiveLock);
}

/*
 * RelationTruncateIndexes - truncate all indexes associated
 * with the heap relation to zero tuples.
 *
 * The routine will truncate and then reconstruct the indexes on
 * the specified relation.	Caller must hold exclusive lock on rel.
 */
static void RelationTruncateIndexes(Relation heapRelation, LOCKMODE lockmode)
{
    ListCell* indlist = NULL;

    /* Ask the relcache to produce a list of the indexes of the rel */
    foreach (indlist, RelationGetIndexList(heapRelation)) {
        Oid indexId = lfirst_oid(indlist);
        Relation currentIndex;
        IndexInfo* indexInfo = NULL;

        /* Open the index relation; use exclusive lock, just to be sure */
        currentIndex = index_open(indexId, lockmode);

        /* Fetch info needed for index_build */
        indexInfo = BuildIndexInfo(currentIndex);

        /*
         * Now truncate the actual file (and discard buffers).
         */
        RelationTruncate(currentIndex, 0);
        
        /* truncate psort relation */
        if (unlikely(currentIndex->rd_rel->relam == PSORT_AM_OID)) {
            Relation psort_rel = heap_open(currentIndex->rd_rel->relcudescrelid, lockmode);
            heap_truncate_one_rel(psort_rel);
            heap_close(psort_rel, NoLock);
        }

        /* Initialize the index and rebuild */
        /* Note: we do not need to re-establish pkey setting */
        index_build(heapRelation, NULL, currentIndex, NULL, indexInfo, false, true, INDEX_CREATE_NONE_PARTITION);

        /* We're done with this index */
        index_close(currentIndex, NoLock);
    }
}

/*
 *	 heap_truncate
 *
 *	 This routine deletes all data within all the specified relations.
 *
 * This is not transaction-safe!  There is another, transaction-safe
 * implementation in commands/tablecmds.c.	We now use this only for
 * ON COMMIT truncation of temporary tables, where it doesn't matter.
 */
void heap_truncate(List* relids)
{
    List* relations = NIL;
    ListCell* cell = NULL;

    /* Open relations for processing, and grab exclusive access on each */
    foreach (cell, relids) {
        Oid rid = lfirst_oid(cell);
        Relation rel;
        LOCKMODE lockmode = AccessExclusiveLock;

        /* truncate global temp table only need RowExclusiveLock */
        if (get_rel_persistence(rid) == RELPERSISTENCE_GLOBAL_TEMP) {
            lockmode = RowExclusiveLock;
        }

        rel = heap_open(rid, lockmode);
        relations = lappend(relations, rel);
    }

    /* Don't allow truncate on tables that are referenced by foreign keys */
    heap_truncate_check_FKs(relations, true);

    /* OK to do it */
    foreach (cell, relations) {
        Relation rel = (RelationData*)lfirst(cell);

        /* Truncate the relation */
        heap_truncate_one_rel(rel);
        pgstat_count_truncate(rel);

        /* Close the relation, but keep exclusive lock on it until commit */
        heap_close(rel, NoLock);
    }
}

/* truncate one partition of ColumnStore in the same xact */
static void TruncateColCStorePartitionInSameXact(Relation rel, Partition p)
{
    /* truncate cudesc heap relation */
    Relation cudesc_rel = heap_open(p->pd_part->relcudescrelid, AccessExclusiveLock);
    heap_truncate_one_rel(cudesc_rel);
    heap_close(cudesc_rel, NoLock);

    /* truncate delta heap relation */
    Relation delta_rel = heap_open(p->pd_part->reldeltarelid, AccessExclusiveLock);
    heap_truncate_one_rel(delta_rel);
    heap_close(delta_rel, NoLock);

    /* truncate each column */
    Relation partRel = partitionGetRelation(rel, p);
    CStore::TruncateStorageInSameXact(partRel);
    releaseDummyRelation(&partRel);
}

/*
 *	 heap_truncate_one_rel
 *
 *	 This routine deletes all data within the specified relation.
 *
 * This is not transaction-safe, because the truncation is done immediately
 * and cannot be rolled back later.  Caller is responsible for having
 * checked permissions etc, and must have obtained AccessExclusiveLock.
 */
void heap_truncate_one_rel(Relation rel)
{
    Oid toastrelid;
    LOCKMODE lockmode = AccessExclusiveLock;

    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        if (!gtt_storage_attached(RelationGetRelid(rel)))
            return;

        /*
         * Truncate global temp table only need RowExclusiveLock
         */
        lockmode = RowExclusiveLock;
    }

#ifdef ENABLE_MOT
    if (RelationIsForeignTable(rel) && isMOTFromTblOid(RelationGetRelid(rel))) {
        FdwRoutine* fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(rel));

        if (fdwroutine->TruncateForeignTable != NULL) {
            fdwroutine->TruncateForeignTable(NULL, rel);
        }
    } else if (!RELATION_IS_PARTITIONED(rel)) {
#else
    if (!RELATION_IS_PARTITIONED(rel)) {
#endif
        /* Truncate the actual file (and discard buffers) */
        RelationTruncate(rel, 0);

        /* If the relation is a cloumn store */
        if (RelationIsColStore(rel)) {
            /* cudesc */
            Relation cudesc_rel = heap_open(rel->rd_rel->relcudescrelid, lockmode);
            heap_truncate_one_rel(cudesc_rel);
            heap_close(cudesc_rel, NoLock);

            /* delta */
            Relation delta_rel = heap_open(rel->rd_rel->reldeltarelid, lockmode);
            heap_truncate_one_rel(delta_rel);
            heap_close(delta_rel, NoLock);

            /* each column */
            CStore::TruncateStorageInSameXact(rel);
        }

        /* If the relation has indexes, truncate the indexes too */
        RelationTruncateIndexes(rel, lockmode);

        /* If there is a toast table, truncate that too */
        toastrelid = rel->rd_rel->reltoastrelid;
        if (OidIsValid(toastrelid)) {
            Relation toastrel = heap_open(toastrelid, lockmode);

            RelationTruncate(toastrel, 0);
            RelationTruncateIndexes(toastrel, lockmode);
            /* keep the lock... */
            heap_close(toastrel, NoLock);
        }
    } else { /* partitioned table */
        List* partOidList = NIL;
        List* indexList = NIL;
        ListCell* indCell = NULL;
        Relation currentIndex = NULL;
        List* currentParttiionIndexList = NIL;
        ListCell* partCell = NULL;

        /* truncate each partition */
        partOidList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, rel->rd_id);
        foreach (partCell, partOidList) {
            Partition p = partitionOpen(rel, HeapTupleGetOid((HeapTuple)lfirst(partCell)), lockmode);

            PartitionTruncate(rel, p, 0);

            /* truncate before reindex */
            if (RelationIsColStore(rel)) {
                TruncateColCStorePartitionInSameXact(rel, p);
            }
            partitionClose(rel, p, NoLock);
        }

        /* process index */
        indexList = RelationGetIndexList(rel);
        foreach (indCell, indexList) {
            ListCell* cell1 = NULL;
            IndexInfo* indexInfo = NULL;
            Oid indexId = lfirst_oid(indCell);
            currentIndex = index_open(indexId, lockmode);

            indexInfo = BuildIndexInfo(currentIndex);

            currentParttiionIndexList = searchPgPartitionByParentId(PART_OBJ_TYPE_INDEX_PARTITION, indexId);

            foreach (cell1, currentParttiionIndexList) {
                Partition indexPart =
                    partitionOpen(currentIndex, HeapTupleGetOid((HeapTuple)lfirst(cell1)), lockmode);
                Partition p;

                PartitionTruncate(currentIndex, indexPart, 0);
                /* truncate psort relation */
                if (unlikely(currentIndex->rd_rel->relam == PSORT_AM_OID)) {
                    Relation psort_rel = heap_open(currentIndex->rd_rel->relcudescrelid, lockmode);
                    heap_truncate_one_rel(psort_rel);
                    heap_close(psort_rel, NoLock);
                }

                p = partitionOpen(rel, indexPart->pd_part->indextblid, NoLock);
                index_build(rel, p, currentIndex, indexPart, indexInfo, false, true, INDEX_CREATE_LOCAL_PARTITION);

                partitionClose(rel, p, NoLock);
                partitionClose(currentIndex, indexPart, NoLock);
            }
            freePartList(currentParttiionIndexList);

            index_close(currentIndex, NoLock);
        }

        /* process toast table for non-bucketed relation */
        foreach (partCell, partOidList) {
            HeapTuple tup = (HeapTuple)lfirst(partCell);

            Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(tup);

            if (partForm->reltoastrelid != InvalidOid) {
                Relation toastrel = heap_open(partForm->reltoastrelid, lockmode);

                RelationTruncate(toastrel, 0);
                RelationTruncateIndexes(toastrel, lockmode);
                /* keep the lock... */
                heap_close(toastrel, NoLock);
            }
        }

        freePartList(partOidList);
    }
	// for GTT
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        up_gtt_relstats(rel, 0, 0, 0, u_sess->utils_cxt.RecentXmin);
    }
}

/*
 * heap_truncate_check_FKs
 *		Check for foreign keys referencing a list of relations that
 *		are to be truncated, and raise error if there are any
 *
 * We disallow such FKs (except self-referential ones) since the whole point
 * of TRUNCATE is to not scan the individual rows to be thrown away.
 *
 * This is split out so it can be shared by both implementations of truncate.
 * Caller should already hold a suitable lock on the relations.
 *
 * tempTables is only used to select an appropriate error message.
 */
void heap_truncate_check_FKs(List* relations, bool tempTables)
{
    List* oids = NIL;
    List* dependents = NIL;
    ListCell* cell = NULL;

    /*
     * Build a list of OIDs of the interesting relations.
     *
     * If a relation has no triggers, then it can neither have FKs nor be
     * referenced by a FK from another table, so we can ignore it.
     */
    foreach (cell, relations) {
        Relation rel = (Relation)lfirst(cell);

        if (rel->rd_rel->relhastriggers)
            oids = lappend_oid(oids, RelationGetRelid(rel));
    }

    /*
     * Fast path: if no relation has triggers, none has FKs either.
     */
    if (oids == NIL)
        return;

    /*
     * Otherwise, must scan pg_constraint.	We make one pass with all the
     * relations considered; if this finds nothing, then all is well.
     */
    dependents = heap_truncate_find_FKs(oids);
    if (dependents == NIL)
        return;

    /*
     * Otherwise we repeat the scan once per relation to identify a particular
     * pair of relations to complain about.  This is pretty slow, but
     * performance shouldn't matter much in a failure path.  The reason for
     * doing things this way is to ensure that the message produced is not
     * dependent on chance row locations within pg_constraint.
     */
    foreach (cell, oids) {
        Oid relid = lfirst_oid(cell);
        ListCell* cell2 = NULL;

        dependents = heap_truncate_find_FKs(list_make1_oid(relid));

        foreach (cell2, dependents) {
            Oid relid2 = lfirst_oid(cell2);

            if (!list_member_oid(oids, relid2)) {
                char* relname = get_rel_name(relid);
                char* relname2 = get_rel_name(relid2);

                if (tempTables)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("unsupported ON COMMIT and foreign key combination"),
                            errdetail(
                                "Table \"%s\" references \"%s\", but they do not have the same ON COMMIT setting.",
                                relname2,
                                relname)));
                else
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot truncate a table referenced in a foreign key constraint"),
                            errdetail("Table \"%s\" references \"%s\".", relname2, relname),
                            errhint("Truncate table \"%s\" at the same time, "
                                    "or use TRUNCATE ... CASCADE.",
                                relname2)));
            }
        }
    }
}

/*
 * heap_truncate_find_FKs
 *		Find relations having foreign keys referencing any of the given rels
 *
 * Input and result are both lists of relation OIDs.  The result contains
 * no duplicates, does *not* include any rels that were already in the input
 * list, and is sorted in OID order.  (The last property is enforced mainly
 * to guarantee consistent behavior in the regression tests; we don't want
 * behavior to change depending on chance locations of rows in pg_constraint.)
 *
 * Note: caller should already have appropriate lock on all rels mentioned
 * in relationIds.	Since adding or dropping an FK requires exclusive lock
 * on both rels, this ensures that the answer will be stable.
 */
List* heap_truncate_find_FKs(List* relationIds)
{
    List* result = NIL;
    Relation fkeyRel;
    SysScanDesc fkeyScan;
    HeapTuple tuple;

    /*
     * Must scan pg_constraint.  Right now, it is a seqscan because there is
     * no available index on confrelid.
     */
    fkeyRel = heap_open(ConstraintRelationId, AccessShareLock);

    fkeyScan = systable_beginscan(fkeyRel, InvalidOid, false, NULL, 0, NULL);

    while (HeapTupleIsValid(tuple = systable_getnext(fkeyScan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);

        /* Not a foreign key */
        if (con->contype != CONSTRAINT_FOREIGN)
            continue;

        /* Not referencing one of our list of tables */
        if (!list_member_oid(relationIds, con->confrelid))
            continue;

        /* Add referencer unless already in input or result list */
        if (!list_member_oid(relationIds, con->conrelid))
            result = insert_ordered_unique_oid(result, con->conrelid);
    }

    systable_endscan(fkeyScan);
    heap_close(fkeyRel, AccessShareLock);

    return result;
}

/*
 * insert_ordered_unique_oid
 *		Insert a new Oid into a sorted list of Oids, preserving ordering,
 *		and eliminating duplicates
 *
 * Building the ordered list this way is O(N^2), but with a pretty small
 * constant, so for the number of entries we expect it will probably be
 * faster than trying to apply qsort().  It seems unlikely someone would be
 * trying to truncate a table with thousands of dependent tables ...
 */
static List* insert_ordered_unique_oid(List* list, Oid datum)
{
    ListCell* prev = NULL;

    /* Does the datum belong at the front? */
    if (list == NIL || datum < linitial_oid(list))
        return lcons_oid(datum, list);
    /* Does it match the first entry? */
    if (datum == linitial_oid(list))
        return list; /* duplicate, so don't insert */
    /* No, so find the entry it belongs after */
    prev = list_head(list);
    for (;;) {
        ListCell* curr = lnext(prev);

        if (curr == NULL || datum < lfirst_oid(curr))
            break; /* it belongs after 'prev', before 'curr' */

        if (datum == lfirst_oid(curr))
            return list; /* duplicate, so don't insert */

        prev = curr;
    }
    /* Insert datum into list after 'prev' */
    (void)lappend_cell_oid(list, prev, datum);
    return list;
}

/*
 * @@GaussDB@@
 * Brief		: buildPartitionKey
 * Description	: build partitioin key attribute NO. array based on table's TupleDesc
 * Notes		:
 */
int2vector* buildPartitionKey(List* keys, TupleDesc tupledsc)
{
    ListCell* cell = NULL;
    ColumnRef* col = NULL;
    int i = 0;
    int j = 0;
    int attnum = tupledsc->natts;
    int partkeyNum = keys->length;
    char* columName = NULL;
    bool finded = false;
    Form_pg_attribute* attrs = tupledsc->attrs;
    int2vector* partkey = NULL;

    partkey = buildint2vector(NULL, partkeyNum);
    foreach (cell, keys) {
        col = (ColumnRef*)lfirst(cell);
        columName = ((Value*)linitial(col->fields))->val.str;
        finded = false;
        for (j = 0; j < attnum; j++) {
            if (strcmp(columName, attrs[j]->attname.data) == 0) {
                partkey->values[i] = attrs[j]->attnum;
                finded = true;
                break;
            }
        }

        if (!finded) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                    (errmsg("buildPartitionKey(): partKeys specified NONE IS found'"))));
        }

        i++;
    }
    return partkey;
}

static oidvector* BuildIntervalTablespace(const IntervalPartitionDefState* intervalPartDef)
{
    if (intervalPartDef->intervalTablespaces == NULL || intervalPartDef->intervalTablespaces->length == 0) {
        return NULL;
    }
    oidvector* tablespaceVec = buildoidvector(NULL, intervalPartDef->intervalTablespaces->length);
    ListCell* cell = NULL;
    int i = 0;
    const char* tablespaceName;
    Oid tableSpaceOid;
    AclResult aclresult;

    foreach (cell, intervalPartDef->intervalTablespaces) {
        tablespaceName = ((Value*)lfirst(cell))->val.str;
        tableSpaceOid = get_tablespace_oid(tablespaceName, false);

        if (tableSpaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
            aclresult = pg_tablespace_aclcheck(tableSpaceOid, GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK) {
                aclcheck_error(aclresult, ACL_KIND_TABLESPACE, tablespaceName);
            }
        }
        tablespaceVec->values[i] = tableSpaceOid;
        i++;
    }
    return tablespaceVec;
}

static Datum BuildInterval(Node* partInterval)
{
    Assert(IsA(partInterval, A_Const));
    A_Const* constNode = (A_Const*)partInterval;
    Assert(IsA(&constNode->val, String));

    ArrayBuildState* astate = NULL;
    astate = accumArrayResult(
        astate, PointerGetDatum(cstring_to_text(constNode->val.val.str)), false, TEXTOID, CurrentMemoryContext);
    return makeArrayResult(astate, CurrentMemoryContext);
}

/*
 * @@GaussDB@@
 * Brief		: AddNewPartitionTuple
 * Description	: Insert a entry to pg_partition. The entry is for partitioned-table or partition.
 * Notes		:
 */
void addNewPartitionTuple(Relation pg_part_desc, Partition new_part_desc, int2vector* pkey, oidvector* intablespace,
    Datum interval, Datum maxValues, Datum transitionPoint, Datum reloptions)
{
    Form_pg_partition new_part_tup = new_part_desc->pd_part;
    /*
     * first we update some of the information in our uncataloged relation's
     * relation descriptor.
     */
    /* The relation is real, but as yet empty */
    new_part_tup->relpages = 0;
    new_part_tup->reltuples = 0;
    new_part_tup->relallvisible = 0;
    /* Initialize relfrozenxid */
    /*
     * Initialize to the minimum XID that could put tuples in the table.
     * We know that no xacts older than RecentXmin are still running, so
     * that will do.
     */
    if (new_part_tup->parttype == PART_OBJ_TYPE_PARTED_TABLE) {
        new_part_tup->relfrozenxid = (ShortTransactionId)InvalidTransactionId;
    } else {
        Assert(new_part_tup->parttype == PART_OBJ_TYPE_TABLE_PARTITION ||
               new_part_tup->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION);
        new_part_tup->relfrozenxid = (ShortTransactionId)u_sess->utils_cxt.RecentXmin;
    }

    /* Now build and insert the tuple */
    insertPartitionEntry(pg_part_desc,
        new_part_desc,
        new_part_desc->pd_id,
        pkey,
        intablespace,
        interval,
        maxValues,
        transitionPoint,
        reloptions,
        new_part_tup->parttype);
}

static void deletePartitionTuple(Oid part_id)
{
    Relation pg_partition_desc;
    HeapTuple tup;

    /* Grab an appropriate lock on the pg_partition relation */

    pg_partition_desc = heap_open(PartitionRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_id));

    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", part_id)));
    }

    /* delete the relation tuple from pg_partition, and finish up */
    else {
        simple_heap_delete(pg_partition_desc, &tup->t_self);
    }

    ReleaseSysCache(tup);

    heap_close(pg_partition_desc, RowExclusiveLock);
}

void dropToastTableOnPartition(Oid partId)
{
    ObjectAddresses* objects = NULL;
    ObjectAddress obj;
    Oid partToastRelid = InvalidOid;
    Form_pg_partition partitionTuple = NULL;
    HeapTuple partitionTupleRaw = NULL;

    /*partToastRelid MUST be a valid oid*/
    partitionTupleRaw = SearchSysCache1((int)PARTRELID, ObjectIdGetDatum(partId));
    if (!PointerIsValid(partitionTupleRaw)) {
        return;
    }
    partitionTuple = (Form_pg_partition)GETSTRUCT(partitionTupleRaw);
    partToastRelid = partitionTuple->reltoastrelid;
    if (!OidIsValid(partToastRelid)) {
        ReleaseSysCache(partitionTupleRaw);
        return;
    }

    /* OK, we're ready to delete toast table on partition*/
    objects = new_object_addresses();
    obj.classId = RelationRelationId;
    obj.objectId = partToastRelid;
    obj.objectSubId = 0;
    add_exact_object_address(&obj, objects);

    /*call performMultipleDeletions, it then delete
     * a, pg_toast_oid type object in pg_type
     * b, pg_toast_oid_index index object in pg_class
     * c, pg_toast_oid toast relation in pg_class.
     */
    performMultipleDeletions(objects, DROP_CASCADE, 0);

    /*cleanup*/
    free_object_addresses(objects);
    ReleaseSysCache(partitionTupleRaw);
}

void dropCuDescTableOnPartition(Oid partId)
{
    ObjectAddresses* objects = NULL;
    ObjectAddress obj;
    Oid partCuDescRelid = InvalidOid;
    Form_pg_partition partitionTuple = NULL;
    HeapTuple partitionTupleRaw = NULL;

    /*partToastRelid MUST be a valid oid*/
    partitionTupleRaw = SearchSysCache1((int)PARTRELID, ObjectIdGetDatum(partId));
    if (!PointerIsValid(partitionTupleRaw)) {
        return;
    }
    partitionTuple = (Form_pg_partition)GETSTRUCT(partitionTupleRaw);
    partCuDescRelid = partitionTuple->relcudescrelid;
    if (!OidIsValid(partCuDescRelid)) {
        ReleaseSysCache(partitionTupleRaw);
        return;
    }

    /* OK, we're ready to delete toast table on partition*/
    objects = new_object_addresses();
    obj.classId = RelationRelationId;
    obj.objectId = partCuDescRelid;
    obj.objectSubId = 0;
    add_exact_object_address(&obj, objects);

    /*call performMultipleDeletions, it then delete
     * a, pg_toast_oid type object in pg_type
     * b, pg_toast_oid_index index object in pg_class
     * c, pg_toast_oid toast relation in pg_class.
     */
    performMultipleDeletions(objects, DROP_CASCADE, 0);

    /*cleanup*/
    free_object_addresses(objects);
    ReleaseSysCache(partitionTupleRaw);
}

void dropDeltaTableOnPartition(Oid partId)
{
    ObjectAddresses* objects = NULL;
    ObjectAddress obj;
    Oid partDeltaTableRelid = InvalidOid;
    Form_pg_partition partitionTuple = NULL;
    HeapTuple partitionTupleRaw = NULL;

    /*partToastRelid MUST be a valid oid*/
    partitionTupleRaw = SearchSysCache1((int)PARTRELID, ObjectIdGetDatum(partId));
    if (!PointerIsValid(partitionTupleRaw)) {
        return;
    }
    partitionTuple = (Form_pg_partition)GETSTRUCT(partitionTupleRaw);
    partDeltaTableRelid = partitionTuple->reldeltarelid;
    if (!OidIsValid(partDeltaTableRelid)) {
        ReleaseSysCache(partitionTupleRaw);
        return;
    }

    /* OK, we're ready to delete toast table on partition*/
    objects = new_object_addresses();
    obj.classId = RelationRelationId;
    obj.objectId = partDeltaTableRelid;
    obj.objectSubId = 0;
    add_exact_object_address(&obj, objects);

    /*call performMultipleDeletions, it then delete
     * a, pg_toast_oid type object in pg_type
     * b, pg_toast_oid_index index object in pg_class
     * c, pg_toast_oid toast relation in pg_class.
     */
    performMultipleDeletions(objects, DROP_CASCADE, 0);

    /*cleanup*/
    free_object_addresses(objects);
    ReleaseSysCache(partitionTupleRaw);
}

/*
 * @@GaussDB@@
 * Brief        :
 * Description  : Build local PartitionData instance, which is not in partition hashtable
 * Notes        :
 * Parameters   : part_name: partition name
 *                for_partitioned_table: true for partitioned table, false for partition
 *                part_tablespace: partition's tablespace.
 *                part_id: partition's oid. This parameter cannot be InvalidOid.
 *                relbufferpool: partition's bufferpool. All partitions of one partitioned table have same bufferpool.

 *                partFileNode: whether an existing index definition is
                                compatible with a prospective index definition,
                                such that the existing index storage
                                could become the storage of the new index,
                                avoiding a rebuild.
 *
 */
Partition heapCreatePartition(const char* part_name, bool for_partitioned_table, Oid part_tablespace, Oid part_id,
    Oid partFileNode, Oid bucketOid, Oid ownerid, StorageType storage_type, bool newcbi, Datum reloptions)
{
    Partition new_part_desc = NULL;
    bool createStorage = false;
    bool isbucket = false;

    /* The caller must have provided an OID for the partition. */
    Assert(OidIsValid(part_id));

    /*
     * Never allow a pg_partition entry to explicitly specify the database's
     * default tablespace in reltablespace; force it to zero instead. This
     * ensures that if the database is cloned with a different default
     * tablespace, the pg_class entry will still match where CREATE DATABASE
     * will put the physically copied relation.
     *
     * Yes, this is a bit of a hack.
     */
    part_tablespace = ConvertToPgclassRelTablespaceOid(part_tablespace);

    if (!for_partitioned_table) {
        if (!OidIsValid(partFileNode)) {
            createStorage = true;
            if (storage_type == SEGMENT_PAGE) {
                Assert(part_tablespace != GLOBALTABLESPACE_OID);
                isbucket = BUCKET_OID_IS_VALID(bucketOid) && !newcbi;
                partFileNode = (Oid)seg_alloc_segment(ConvertToRelfilenodeTblspcOid(part_tablespace),
                                                      u_sess->proc_cxt.MyDatabaseId, isbucket, InvalidBlockNumber);
                ereport(LOG, (errmsg("Segment Partition %s(%u) set relfilenode %u xid %lu", part_name, part_id,
                    partFileNode, GetCurrentTransactionIdIfAny())));
            } else {
                partFileNode = part_id;
            } 
        }
        if (u_sess->proc_cxt.IsBinaryUpgrade) {
            createStorage = true;
            if (storage_type == SEGMENT_PAGE) {
                Assert(part_tablespace != GLOBALTABLESPACE_OID);
                BlockNumber preassignedBlock = OidIsValid(partFileNode) ? partFileNode : InvalidBlockNumber;
                isbucket = BUCKET_OID_IS_VALID(bucketOid) && !newcbi;
                partFileNode = (Oid)seg_alloc_segment(ConvertToRelfilenodeTblspcOid(part_tablespace),
                                                      u_sess->proc_cxt.MyDatabaseId, isbucket, preassignedBlock);
            }
        }
    }

    /*
     * build the partcache entry.
     */
    new_part_desc = PartitionBuildLocalPartition(part_name,
        part_id,      /* partition oid */
        partFileNode, /* partition's file node, same as partition oid*/
        part_tablespace,
        for_partitioned_table ? HEAP_DISK : storage_type,
        reloptions);

    /*
     * Save newcbi as a context indicator to 
     * avoid missing information in later index building process.
     */
    new_part_desc->newcbi = newcbi;

    /*
     * if this pg_partition entry is for partitioned table, then part_filenode is invalid.
     * if this pg_partition entry is for partition, then part_filenode must be specified.
     */
    if (!for_partitioned_table && createStorage) {
        PartitionOpenSmgr(new_part_desc);
        if (newcbi) {
            /* using a dummy relation to pass on newcbi flag */
            RelationData dummyrel;
            dummyrel.rd_id = InvalidOid;
            dummyrel.newcbi = true;
            RelationCreateStorage(new_part_desc->pd_node,
                RELPERSISTENCE_PERMANENT, /* partition table's persitence MUST be 'p'(permanent table)*/
                ownerid,
                bucketOid,
                &dummyrel);
        } else {
            RelationCreateStorage(new_part_desc->pd_node,
                RELPERSISTENCE_PERMANENT, /* partition table's persitence MUST be 'p'(permanent table)*/
                ownerid,
                bucketOid);
        }
    }

    return new_part_desc;
    
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void heapDropPartitionToastList(List* toastList)
{
    ObjectAddresses* objects = NULL;
    ObjectAddress obj;
    Oid toastRelid = InvalidOid;
    ListCell* cell = NULL;

    /* OK, we're ready to delete toast table on partition*/
    objects = new_object_addresses();

    foreach (cell, toastList) {
        toastRelid = lfirst_oid(cell);
        if (OidIsValid(toastRelid)) {
            obj.classId = RelationRelationId;
            obj.objectId = toastRelid;
            obj.objectSubId = 0;
            add_exact_object_address(&obj, objects);
        }
    }

    /*call performMultipleDeletions, it then delete
     * a, pg_toast_oid type object in pg_type
     * b, pg_toast_oid_index index object in pg_class
     * c, pg_toast_oid toast relation in pg_class.
     */
    performMultipleDeletions(objects, DROP_CASCADE, 0);

    /*cleanup*/
    free_object_addresses(objects);
}

void heapDropSubPartitionList(Relation rel, Oid partId)
{
    ListCell* cell = NULL;
    List *partCacheList = NIL;
    HeapTuple partTuple = NULL;
    Oid subPartOid = InvalidOid;
    Form_pg_partition partForm = NULL;
    Oid toastOid = InvalidOid;
    Oid cuDesc = InvalidOid;
    Oid delta = InvalidOid;
    List *toastOidList = NIL;
    List *cuDescOidList = NIL;
    List *deltaOidList = NIL;
    List *subPartOidList = NIL;

    partCacheList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION, partId);
    foreach (cell, partCacheList) {
        partTuple = (HeapTuple)lfirst(cell);

        if (PointerIsValid(partTuple)) {
            subPartOid = HeapTupleGetOid(partTuple);
            subPartOidList = lappend_oid(subPartOidList, subPartOid);

            partForm = (Form_pg_partition)GETSTRUCT(partTuple);
            toastOid = partForm->reltoastrelid;
            cuDesc = partForm->relcudescrelid;
            delta = partForm->reldeltarelid;

            if (OidIsValid(toastOid)) {
                toastOidList = lappend_oid(toastOidList, toastOid);
            }

            if (OidIsValid(cuDesc)) {
                cuDescOidList = lappend_oid(cuDescOidList, cuDesc);
            }

            if (OidIsValid(delta)) {
                deltaOidList = lappend_oid(deltaOidList, delta);
            }
        }
    }

    /*
     * drop table partition's toast table
     */
    if (PointerIsValid(toastOidList)) {
        heapDropPartitionToastList(toastOidList);
        list_free(toastOidList);
    }

    if (PointerIsValid(cuDescOidList)) {
        heapDropPartitionToastList(cuDescOidList);
        list_free(cuDescOidList);
    }

    if (PointerIsValid(deltaOidList)) {
        heapDropPartitionToastList(deltaOidList);
        list_free(deltaOidList);
    }

    /*
     * drop table partition
     */
    if (PointerIsValid(subPartOidList)) {
        heapDropPartitionList(rel, subPartOidList);
        list_free(subPartOidList);
    }
    freePartList(partCacheList);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void heapDropPartitionList(Relation rel, List* partitionList)
{
    List* newPartList = NIL;
    ListCell* cell = NULL;
    Oid partId = InvalidOid;
    Partition part = NULL;
    Relation partRel = NULL;

    /*check input parameters validity*/
    Assert(PointerIsValid(rel));
    Assert(RELATION_IS_PARTITIONED(rel));
    Assert(PointerIsValid(partitionList));

    /*
     * step 1: delete depending objs on partition list
     */
    foreach (cell, partitionList) {
        partId = lfirst_oid(cell);
        if (OidIsValid(partId)) {
            part = partitionOpen(rel, partId, AccessExclusiveLock);
            if (RelationIsSubPartitioned(rel)) {
                partRel = partitionGetRelation(rel, part);
                heapDropSubPartitionList(partRel, partId);
                releaseDummyRelation(&partRel);
            }
            newPartList = lappend(newPartList, part);
        }
    }

    /*
     * step 2: iterate partList, delete one by one
     */
    foreach (cell, newPartList) {
        part = (Partition)lfirst(cell);
        heapDropPartition(rel, part);
    }

    /*
     * clean up
     */
    list_free(newPartList);
}
/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		: the caller MUST have AccessExclusiveLock lock on partition
 */
void heapDropPartition(Relation relation, Partition part)
{
    Oid partid = InvalidOid;
    Relation partRel = NULL;

    /*
     * Open and lock the relation.
     */
    Assert(PointerIsValid(part));

    partid = part->pd_id;

    checkPartNotInUse(part, "DROP PARTITION");

    if (!(RelationIsSubPartitioned(relation) && part->pd_part->parttype == PART_OBJ_TYPE_TABLE_PARTITION)) {
        partRel = partitionGetRelation(relation, part);

        TransferPredicateLocksToHeapRelation(partRel);

        /*
         * Schedule unlinking of the relation's physical files at commit.
         */
        RelationDropStorage(partRel);
        releaseDummyRelation(&partRel);
    }

    partitionClose(relation, part, NoLock);
    /*
     * Flush the partition from the partcache.  We want to do this before
     * starting to remove catalog entries, just to be certain that no partcache
     * entry rebuild will happen partway through.  (That should not really
     * matter, since we don't do CommandCounterIncrement here, but let's be
     * safe.)
     */
    PartitionForgetPartition(partid);

    /*
     * delete statistics
     */
    RemoveStatistics<'p'>(partid, 0);

    /*
     * delete partition tuple
     */
    deletePartitionTuple(partid);
}

static void heapDropPartitionTable(Relation relation)
{
    List* partCacheList = NIL;
    List* partitionOidList = NIL;
    List* toastOidList = NIL;
    List* cuDescOidList = NIL;
    List* deltaOidList = NIL;
    Oid partOid = InvalidOid;
    Oid toastOid = InvalidOid;
    Oid cuDesc = InvalidOid;
    Oid delta = InvalidOid;
    Partition partTable = NULL;
    HeapTuple partTuple = NULL;
    HeapTuple tableTuple = NULL;
    Form_pg_partition partForm = NULL;
    ListCell* cell = NULL;

    partCacheList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, RelationGetRelid(relation));
    tableTuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, RelationGetRelid(relation));

    /*first form partition list and partitioned table list
     *the opened partition must already LOCKED before realy dropped.
     */
    foreach (cell, partCacheList) {
        partTuple = (HeapTuple)lfirst(cell);

        if (PointerIsValid(partTuple)) {
            partOid = HeapTupleGetOid(partTuple);
            partitionOidList = lappend_oid(partitionOidList, partOid);

            partForm = (Form_pg_partition)GETSTRUCT(partTuple);
            toastOid = partForm->reltoastrelid;
            cuDesc = partForm->relcudescrelid;
            delta = partForm->reldeltarelid;

            if (OidIsValid(toastOid)) {
                toastOidList = lappend_oid(toastOidList, toastOid);
            }

            if (OidIsValid(cuDesc)) {
                cuDescOidList = lappend_oid(cuDescOidList, cuDesc);
            }

            if (OidIsValid(delta)) {
                deltaOidList = lappend_oid(deltaOidList, delta);
            }
        }
    }

    /*
     * drop table partition's toast table
     */
    if (PointerIsValid(toastOidList)) {
        heapDropPartitionToastList(toastOidList);
        list_free(toastOidList);
    }

    if (PointerIsValid(cuDescOidList)) {
        heapDropPartitionToastList(cuDescOidList);
        list_free(cuDescOidList);
    }

    if (PointerIsValid(deltaOidList)) {
        heapDropPartitionToastList(deltaOidList);
        list_free(deltaOidList);
    }

    /*
     * drop table partition
     */
    if (PointerIsValid(partitionOidList)) {
        heapDropPartitionList(relation, partitionOidList);
        list_free(partitionOidList);
    }

    /*
     * drop partitioned table
     */
    if (PointerIsValid(tableTuple)) {
        partOid = HeapTupleGetOid(tableTuple);
        partTable = partitionOpen(relation, partOid, AccessExclusiveLock);
        heapDropPartition(relation, partTable);
        heap_freetuple(tableTuple);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for partitioned table %u in pg_partition", RelationGetRelid(relation))));
    }

    freePartList(partCacheList);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void heapDropPartitionIndex(Relation parentIndex, Oid partIndexId)
{
    Partition partIndex = NULL;
    Relation partRel = NULL;
    ObjectAddress obj;

    partIndex = partitionOpen(parentIndex, partIndexId, AccessExclusiveLock);
    partRel = partitionGetRelation(parentIndex, partIndex);

    // delete CStore index
    //
    if (partRel->rd_rel->relcudescrelid != InvalidOid) {
        obj.classId = RelationRelationId;
        obj.objectId = partRel->rd_rel->relcudescrelid;
        obj.objectSubId = 0;
        performDeletion(&obj, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* Deltete index on part delta. */
    if (RelationIsCUFormat(parentIndex) && partRel->rd_index != NULL && partRel->rd_index->indisunique) {
        obj.classId = RelationRelationId;
        obj.objectId = GetDeltaIdxFromCUIdx(RelationGetRelid(partRel), true);
        obj.objectSubId = 0;
        performDeletion(&obj, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }
#endif

    /*
     * we should process the predicate lock on the partition
     */
    TransferPredicateLocksToHeapRelation(partRel);

    RelationDropStorage(partRel);

    partitionClose(parentIndex, partIndex, NoLock);

    PartitionForgetPartition(partIndexId);

    deletePartitionTuple(partIndexId);

    releaseDummyRelation(&partRel);
}

void GetNewPartitionOidAndNewPartrelfileOid(List *subPartitionDefState, Oid *newPartitionOid, Oid *newPartrelfileOid,
                                            Relation pgPartRel, Oid newPartitionTableSpaceOid)
{
    if (subPartitionDefState != NULL) {
        if (u_sess->proc_cxt.IsBinaryUpgrade &&
            OidIsValid(u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid)) {
            *newPartitionOid = u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid;
            u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid = InvalidOid;
            *newPartrelfileOid = u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_rfoid = InvalidOid;
        } else {
            *newPartitionOid = GetNewOid(pgPartRel);
        }
    } else {
        if (u_sess->proc_cxt.IsBinaryUpgrade && binary_upgrade_is_next_part_pg_partition_oid_valid()) {
            *newPartitionOid = binary_upgrade_get_next_part_pg_partition_oid();
            *newPartrelfileOid = binary_upgrade_get_next_part_pg_partition_rfoid();
        } else {
            *newPartitionOid = GetNewRelFileNode(newPartitionTableSpaceOid, pgPartRel,
                                                 RELPERSISTENCE_PERMANENT); /* partition's persistence only
                                                                               can be 'p'(permanent table) */
        }
    }
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: add new partition, USED by addNewPartitionTuplesForPartition() iteration
 *                             and ADD PARTITION
 * Notes		:
 * Parameters   : pg_partition_rel:  RelationData pointer for pg_partition,
 *                                   the caller MUST get RowExclusiveLock on pg_partition
 *                partTableOid: partitioned table's oid
 *                partTablespace: partitioned table's tablespace.
 *                               If partition's tablespace is not provided, it will inherit from parent partitioned
 * table. partBufferPool:  partitioned table's BufferPoolId. All partitions of one partitioned table have same
 * BufferPoolId. newPartDef: partition description.
 *
 */
Oid heapAddRangePartition(Relation pgPartRel, Oid partTableOid, Oid partTablespace, Oid bucketOid,
    RangePartitionDefState *newPartDef, Oid ownerid, Datum reloptions, const bool *isTimestamptz,
    StorageType storage_type, LOCKMODE partLockMode, int2vector* subpartition_key, bool isSubpartition)
{
    Datum boundaryValue = (Datum)0;
    Oid newPartitionOid = InvalidOid;
    Oid newPartitionTableSpaceOid = InvalidOid;
    Relation relation;
    Partition newPartition;
    Oid newPartrelfileOid = InvalidOid;

    /*missing partition definition structure*/
    if (!PointerIsValid(newPartDef)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing definition for new partition")));
    }
    /*boundary and boundary length check*/
    if (!PointerIsValid(newPartDef->boundary)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("boundary not defined for new partition")));
    }
    if (newPartDef->boundary->length > MAX_PARTITIONKEY_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                errmsg("too many partition keys, allowed is %d", MAX_PARTITIONKEY_NUM)));
    }

    /*new partition name check*/
    if (!PointerIsValid(newPartDef->partitionName)) {
        ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg("partition name is invalid")));
    }

    /* transform boundary value */
    boundaryValue = transformPartitionBoundary(newPartDef->boundary, isTimestamptz);

    /* get partition tablespace oid */
    if (newPartDef->tablespacename) {
        newPartitionTableSpaceOid = get_tablespace_oid(newPartDef->tablespacename, false);
    }

    if (!OidIsValid(newPartitionTableSpaceOid)) {
        newPartitionTableSpaceOid = partTablespace;
    }

    /* Check permissions except when using database's default */
    if (OidIsValid(newPartitionTableSpaceOid) && newPartitionTableSpaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        AclResult aclresult;

        aclresult = pg_tablespace_aclcheck(newPartitionTableSpaceOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(newPartitionTableSpaceOid));
        }
    }

    /* create partition */
    GetNewPartitionOidAndNewPartrelfileOid(newPartDef->subPartitionDefState, &newPartitionOid, &newPartrelfileOid,
                                           pgPartRel, newPartitionTableSpaceOid);

    if (partLockMode != NoLock) {
        LockPartitionOid(partTableOid, (uint32)newPartitionOid, partLockMode);
    }
    bool forPartitionTable = false;
    if (isSubpartition) { /* Level-2 partition of subpartition */
        forPartitionTable = false;
    } else if (newPartDef->subPartitionDefState != NULL) { /* Level-2 partition of subpartition */
        forPartitionTable = true;
    } else { /* Common partition */
        forPartitionTable = false;
    }
    newPartition = heapCreatePartition(newPartDef->partitionName, /* partition's name */
        forPartitionTable,                                                    /* false for partition */
        newPartitionTableSpaceOid,                                /* partition's tablespace*/
        newPartitionOid,                                          /* partition's oid*/
        newPartrelfileOid,
        bucketOid,
        ownerid,
        storage_type,
        false,
        reloptions);

    Assert(newPartitionOid == PartitionGetPartid(newPartition));
    if (isSubpartition) {
        InitSubPartitionDef(newPartition, partTableOid, PART_STRATEGY_RANGE);
    } else {
        InitPartitionDef(newPartition, partTableOid, PART_STRATEGY_RANGE);
    }

    /* step 3: insert into pg_partition tuple */
    addNewPartitionTuple(pgPartRel, /* RelationData pointer for pg_partition */
        newPartition,               /* PartitionData pointer for partition */
        subpartition_key,                       /* */
        NULL,
        (Datum)0,      /* interval*/
        boundaryValue, /* max values */
        (Datum)0,      /* transition point */
        reloptions);

    if (isSubpartition) {
        PartitionCloseSmgr(newPartition);
        Partition part = newPartition;
        if (PartitionIsBucket(newPartition)) {
            part = newPartition->parent;
            bucketClosePartition(newPartition);
        }
        PartitionClose(part);
    } else {
        relation = relation_open(partTableOid, NoLock);
        PartitionCloseSmgr(newPartition);

        partitionClose(relation, newPartition, NoLock);
        relation_close(relation, NoLock);
    }
    return newPartitionOid;
}

#define IsDigital(_ch) (((_ch) >= '0') && ((_ch) <= '9'))

static unsigned ExtractIntervalPartNameSuffix(const char* partName)
{
    if (partName == NULL) {
        return 0;
    }

    size_t len = strlen(partName);
    size_t constPartLen = strlen(INTERVAL_PARTITION_NAME_PREFIX);
    /* 5 is length of MAX_PARTITION_NUM */
    if (len <= constPartLen || len > constPartLen + INTERVAL_PARTITION_NAME_SUFFIX_LEN) {
        return 0;
    }

    if (strncmp(partName, INTERVAL_PARTITION_NAME_PREFIX, constPartLen) != 0) {
        return 0;
    }

    for (size_t i = constPartLen; i < len; ++i) {
        if (!IsDigital(partName[i])) {
            return 0;
        }
    }

    return (unsigned)atoi(partName + constPartLen);
}

int RangeElementOidCmp(const void* a, const void* b)
{
    const RangeElement* rea = (const RangeElement*)a;
    const RangeElement* reb = (const RangeElement*)b;

    if (rea->partitionOid < reb->partitionOid) {
        return 1;
    }

    if (rea->partitionOid == reb->partitionOid) {
        return 0;
    }

    return -1;
}

char* GenIntervalPartitionName(Relation rel)
{
    unsigned suffix = 0;
    Oid existingPartOid;
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;
    RangeElement* eles = CopyRangeElementsWithoutBoundary(partMap->rangeElements, partMap->rangeElementsNum);
    /* sort desc by oid */
    qsort(eles, partMap->rangeElementsNum, sizeof(RangeElement), RangeElementOidCmp);
    for (int i = 0; i < partMap->rangeElementsNum; ++i) {
        /* merge or split may result in range oid bigger than interval range oid */
        if (!eles[i].isInterval) {
            continue;
        }

        char* name = PartitionOidGetName(eles[i].partitionOid);
        if ((suffix = ExtractIntervalPartNameSuffix(name)) != 0) {
            pfree(name);
            break;
        }
    }
    pfree(eles);

    char* partName = (char*)palloc0(NAMEDATALEN);
    error_t rc;
    while (true) {
        ++suffix;
        suffix = (suffix % MAX_PARTITION_NUM == 0 ? MAX_PARTITION_NUM : suffix % MAX_PARTITION_NUM);
        rc = snprintf_s(partName, NAMEDATALEN, NAMEDATALEN - 1, INTERVAL_PARTITION_NAME_PREFIX_FMT, suffix);
        securec_check_ss(rc, "\0", "\0");
        existingPartOid = partitionNameGetPartitionOid(
            rel->rd_id, partName, PART_OBJ_TYPE_TABLE_PARTITION, AccessShareLock, true, false, NULL, NULL, NoLock);
        if (!OidIsValid(existingPartOid)) {
            return partName;
        }
    }
}

Oid GetRecentUsedTablespace(Relation rel)
{
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;
    Assert(partMap->rangeElementsNum >= 1);
    RangeElement* maxOidEle = &partMap->rangeElements[0];
    for (int i = 1; i < partMap->rangeElementsNum; ++i) {
        if (partMap->rangeElements[i].partitionOid > maxOidEle->partitionOid) {
            maxOidEle = &partMap->rangeElements[i];
        }
    }

    /* no interval partition yet */
    if (!maxOidEle->isInterval) {
        return InvalidOid;
    }
    return PartitionOidGetTablespace(maxOidEle->partitionOid);
}

Oid ChooseIntervalTablespace(Relation rel)
{
    const oidvector* tablespaceVec = ((RangePartitionMap*)rel->partMap)->intervalTablespace;
    Assert(tablespaceVec->dim1 >= 1);
    if (tablespaceVec->dim1 == 1) {
        return tablespaceVec->values[0];
    }

    const Oid recentUsed = GetRecentUsedTablespace(rel);
    if (!OidIsValid(recentUsed)) {
        return tablespaceVec->values[0];
    }

    int i = 0;
    for (; i < tablespaceVec->dim1; ++i) {
        if (tablespaceVec->values[i] == recentUsed) {
            break;
        }
    }

    return tablespaceVec->values[(i + 1) % tablespaceVec->dim1];
}

Oid HeapAddIntervalPartition(Relation pgPartRel, Relation rel, Oid partTableOid, Oid partrelfileOid, Oid partTablespace,
    Oid bucketOid, Datum boundaryValue, Oid ownerid, Datum reloptions, StorageType storage_type)
{
    Oid newPartitionOid = InvalidOid;
    Oid newPartitionTableSpaceOid = InvalidOid;
    Relation relation;
    Partition newPartition;

    if (((RangePartitionMap*)rel->partMap)->intervalTablespace != NULL) {
        newPartitionTableSpaceOid = ChooseIntervalTablespace(rel);
    }

    if (!OidIsValid(newPartitionTableSpaceOid)) {
        newPartitionTableSpaceOid = partTablespace;
    }

    /* Check permissions except when using database's default */
    if (OidIsValid(newPartitionTableSpaceOid) && newPartitionTableSpaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        AclResult aclresult = pg_tablespace_aclcheck(newPartitionTableSpaceOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(newPartitionTableSpaceOid));
        }
    }

    /* create partition */
    if (!OidIsValid(partrelfileOid) && u_sess->proc_cxt.IsBinaryUpgrade &&
        binary_upgrade_is_next_part_pg_partition_oid_valid()) {
        newPartitionOid = binary_upgrade_get_next_part_pg_partition_oid();
        partrelfileOid = binary_upgrade_get_next_part_pg_partition_rfoid();
    } else if (!OidIsValid(partrelfileOid)) {
        newPartitionOid = GetNewRelFileNode(newPartitionTableSpaceOid,
            pgPartRel,
            RELPERSISTENCE_PERMANENT); /* partition's persistence only can be 'p'(permanent table) */
    } else {
        Assert(t_thrd.xact_cxt.inheritFileNode);
        ereport(NOTICE, (errmsg("Define inheritFileNode %u for new interval partition", partrelfileOid)));
    }

    LockPartitionOid(partTableOid, (uint32)newPartitionOid, AccessExclusiveLock);
    char* partName = GenIntervalPartitionName(rel);

    newPartition = heapCreatePartition(partName, /* partition's name */
        false,                                   /* false for partition */
        newPartitionTableSpaceOid,               /* partition's tablespace */
        newPartitionOid,                         /* partition's oid */
        partrelfileOid,
        bucketOid,
        ownerid,
        storage_type,
        false,
        reloptions);
    pfree(partName);

    Assert(newPartitionOid == PartitionGetPartid(newPartition));
    InitPartitionDef(newPartition, partTableOid, PART_STRATEGY_INTERVAL);

    /* step 3: insert into pg_partition tuple*/
    addNewPartitionTuple(pgPartRel, /* RelationData pointer for pg_partition */
        newPartition,               /* PartitionData pointer for partition */
        NULL,
        NULL,
        (Datum)0,      /* interval */
        boundaryValue, /* max values */
        (Datum)0,      /* transition point */
        reloptions);

    relation = relation_open(partTableOid, NoLock);
    PartitionCloseSmgr(newPartition);

    partitionClose(relation, newPartition, NoLock);
    relation_close(relation, NoLock);
    return newPartitionOid;
}

Oid HeapAddListPartition(Relation pgPartRel, Oid partTableOid, Oid partTablespace, Oid bucketOid,
    ListPartitionDefState* newListPartDef, Oid ownerid, Datum reloptions, const bool* isTimestamptz,
    StorageType storage_type, int2vector* subpartition_key, bool isSubpartition)
{
    Datum boundaryValue = (Datum)0;
    Oid newListPartitionOid = InvalidOid;
    Oid newPartitionTableSpaceOid = InvalidOid;
    Oid partrelfileOid = InvalidOid;
    Relation relation;
    Partition newListPartition;
    /*missing partition definition structure*/
    if (!PointerIsValid(newListPartDef)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing definition for new partition")));
    }
    /*boundary and boundary length check*/
    if (!PointerIsValid(newListPartDef->boundary)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("boundary not defined for new partition")));
    }
    if (newListPartDef->boundary->length > PARTKEY_VALUE_MAXNUM) {
        ereport(ERROR,
            (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                errmsg("too many partition keys, allowed is %d", PARTKEY_VALUE_MAXNUM)));
    }

    /*new partition name check*/
    if (!PointerIsValid(newListPartDef->partitionName)) {
        ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg("partition name is invalid")));
    }

    /* transform boundary value */
    boundaryValue = transformListBoundary(newListPartDef->boundary, isTimestamptz);

    /* get partition tablespace oid */
    if (newListPartDef->tablespacename) {
        newPartitionTableSpaceOid = get_tablespace_oid(newListPartDef->tablespacename, false);
    }

    if (!OidIsValid(newPartitionTableSpaceOid)) {
        newPartitionTableSpaceOid = partTablespace;
    }

    /* Check permissions except when using database's default */
    if (OidIsValid(newPartitionTableSpaceOid) && newPartitionTableSpaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        AclResult aclresult = pg_tablespace_aclcheck(newPartitionTableSpaceOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(newPartitionTableSpaceOid));
        }
    }

    /* create partition */
    GetNewPartitionOidAndNewPartrelfileOid(newListPartDef->subPartitionDefState, &newListPartitionOid, &partrelfileOid,
                                           pgPartRel, newPartitionTableSpaceOid);

    LockPartitionOid(partTableOid, (uint32)newListPartitionOid, AccessExclusiveLock);

    bool forPartitionTable = false;
    if (isSubpartition) { /* Level-2 partition of subpartition */
        forPartitionTable = false;
    } else if (newListPartDef->subPartitionDefState != NULL) { /* Level-2 partition of subpartition */
        forPartitionTable = true;
    } else { /* Common partition */
        forPartitionTable = false;
    }
    newListPartition = heapCreatePartition(newListPartDef->partitionName, forPartitionTable, newPartitionTableSpaceOid,
                                           newListPartitionOid, partrelfileOid, bucketOid, ownerid, storage_type,false,
        reloptions);

    Assert(newListPartitionOid == PartitionGetPartid(newListPartition));

    if (isSubpartition) {
        InitSubPartitionDef(newListPartition, partTableOid, PART_STRATEGY_LIST);
    } else {
        InitPartitionDef(newListPartition, partTableOid, PART_STRATEGY_LIST);
    }

    /* step 3: insert into pg_partition tuple */
    addNewPartitionTuple(pgPartRel, /* RelationData pointer for pg_partition */
        newListPartition,               /* PartitionData pointer for partition */
        subpartition_key,                       /* */
        NULL,
        (Datum)0,      /* interval*/
        boundaryValue, /* max values */
        (Datum)0,      /* transition point */
        reloptions);

    if (isSubpartition) {
        PartitionCloseSmgr(newListPartition);
        Partition part = newListPartition;
        if (PartitionIsBucket(newListPartition)) {
            part = newListPartition->parent;
            bucketClosePartition(newListPartition);
        }
        PartitionClose(part);
    } else {
        relation = relation_open(partTableOid, NoLock);
        PartitionCloseSmgr(newListPartition);

        partitionClose(relation, newListPartition, NoLock);
        relation_close(relation, NoLock);
    }
    return newListPartitionOid;
}

Timestamp Align2UpBoundary(Timestamp value, Interval* intervalValue, Timestamp boundary)
{
    Timestamp nearbyBoundary = boundary;
    Interval* diff = DatumGetIntervalP(timestamp_mi(value, boundary));
    /* approximate multiple */
    int multiple = (int)(INTERVAL_TO_USEC(diff) / INTERVAL_TO_USEC(intervalValue));
    pfree(diff);
    if (multiple != 0) {
        Interval* integerInterval = DatumGetIntervalP(interval_mul(intervalValue, (float8)multiple));
        nearbyBoundary = DatumGetTimestamp(timestamp_pl_interval(boundary, integerInterval));
        pfree(integerInterval);
    }
    if (nearbyBoundary <= value) {
        while (true) {
            nearbyBoundary = DatumGetTimestamp(timestamp_pl_interval(nearbyBoundary, intervalValue));
            if (nearbyBoundary > value) {
                return nearbyBoundary;
            }
        }
    } else {
        while (true) {
            Timestamp res = DatumGetTimestamp(timestamp_mi_interval(nearbyBoundary, intervalValue));
            if (res <= value) {
                return nearbyBoundary;
            }
            nearbyBoundary = res;
        }
    }
}

Datum Timestamp2Boundarys(Relation rel, Timestamp ts)
{
    Const consts;
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;
    Const* lastPartBoundary = partMap->rangeElements[partMap->rangeElementsNum - 1].boundary[0];
    bool isTimestamptz = lastPartBoundary->consttype == TIMESTAMPTZOID;
    Datum columnRaw;
    if (lastPartBoundary->consttype == DATEOID) {
        columnRaw = timestamp2date(ts);
    } else {
        columnRaw = TimestampGetDatum(ts);
    }
    int2vector* partKeyColumn = partMap->partitionKey;
    Assert(partKeyColumn->dim1 == 1);

    (void)transformDatum2Const(rel->rd_att, partKeyColumn->values[0], columnRaw, false, &consts);
    List* bondary = list_make1(&consts);
    Datum res = transformPartitionBoundary(bondary, &isTimestamptz);
    list_free(bondary);
    return res;
}

Datum GetPartBoundaryByTuple(Relation rel, HeapTuple tuple)
{
    RangePartitionMap* partMap = (RangePartitionMap*)rel->partMap;
    int2vector* partKeyColumn = partMap->partitionKey;
    Assert(partKeyColumn->dim1 == 1);
    Assert(partMap->type.type == PART_TYPE_INTERVAL);
    Assert(partMap->rangeElementsNum >= 1);

    Const* lastPartBoundary = partMap->rangeElements[partMap->rangeElementsNum - 1].boundary[0];
    Assert(lastPartBoundary->consttype == TIMESTAMPOID || lastPartBoundary->consttype == TIMESTAMPTZOID ||
           lastPartBoundary->consttype == DATEOID);

    bool isNull = false;
    Datum columnRaw;
    Timestamp value;
    Timestamp boundaryTs;

    columnRaw = tableam_tops_tuple_fast_getattr(tuple, partKeyColumn->values[0], rel->rd_att, &isNull);

    if (lastPartBoundary->consttype == DATEOID) {
        value = date2timestamp(DatumGetDateADT(columnRaw));
        boundaryTs = date2timestamp(DatumGetDateADT(lastPartBoundary->constvalue));
    } else {
        value = DatumGetTimestamp(columnRaw);
        boundaryTs = DatumGetTimestamp(lastPartBoundary->constvalue);
    }
    return Timestamp2Boundarys(rel, Align2UpBoundary(value, partMap->intervalValue, boundaryTs));
}

Oid AddNewIntervalPartition(Relation rel, void* insertTuple, bool isDDL)
{
    Relation pgPartRel = NULL;
    Oid newPartOid = InvalidOid;
    Datum newRelOptions;
    Datum relOptions;
    HeapTuple tuple;
    bool isNull = false;
    List* oldRelOptions = NIL;
    Oid bucketOid;

    if (rel->partMap->isDirty) {
        CacheInvalidateRelcache(rel);
    }

    /*
     * to avoid dead lock, we should release AccessShareLock on ADD_PARTITION_ACTION
     * locked by the transaction before aquire AccessExclusiveLock.
     */
    UnlockRelationForAccessIntervalPartTabIfHeld(rel);
    /* it will accept invalidation messages generated by other sessions in lockRelationForAddIntervalPartition. */
    LockRelationForAddIntervalPartition(rel);
    partitionRoutingForTuple(rel, insertTuple, u_sess->catalog_cxt.route);

    /* if the partition exists, return partition's oid */
    if (u_sess->catalog_cxt.route->fileExist) {
        Assert(OidIsValid(u_sess->catalog_cxt.route->partitionId));
        /* we should take AccessShareLock again before release AccessExclusiveLock for consistency. */
        LockRelationForAccessIntervalPartitionTab(rel);
        UnlockRelationForAddIntervalPartition(rel);
        return u_sess->catalog_cxt.route->partitionId;
    }

    /* can not add more partition, because more enough */
    if ((getNumberOfPartitions(rel) + 1) > MAX_PARTITION_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("too many partitions for partitioned table"),
                errhint("Number of partitions can not be more than %d", MAX_PARTITION_NUM)));
    }

    /* whether has the unusable local index */
    if (!checkRelationLocalIndexesUsable(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("can't add partition bacause the relation %s has unusable local index",
                    NameStr(rel->rd_rel->relname)),
                errhint("please reindex the unusable index first.")));
    }

    pgPartRel = relation_open(PartitionRelationId, RowExclusiveLock);

    /* add new partition entry in pg_partition */
    /* TRANSFORM into target first */
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel->rd_id));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("cache lookup failed for relation %u", rel->rd_id)));
    }
    relOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);

    oldRelOptions = untransformRelOptions(relOptions);
    newRelOptions = transformRelOptions((Datum)0, oldRelOptions, NULL, NULL, false, false);
    ReleaseSysCache(tuple);

    if (oldRelOptions != NIL) {
        list_free_ext(oldRelOptions);
    }

    bucketOid = RelationGetBucketOid(rel);
    newPartOid = HeapAddIntervalPartition(pgPartRel,
        rel,
        rel->rd_id,
        InvalidOid,
        rel->rd_rel->reltablespace,
        bucketOid,
        GetPartBoundaryByTuple(rel, (HeapTuple)insertTuple),
        rel->rd_rel->relowner,
        (Datum)newRelOptions,
        RelationGetStorageType(rel));

    CommandCounterIncrement();
    addIndexForPartition(rel, newPartOid);

    addToastTableForNewPartition(rel, newPartOid);
    /* invalidate relation */
    CacheInvalidateRelcache(rel);

    /* close relation, done */
    relation_close(pgPartRel, NoLock);

    /*
     * We must bump the command counter to make the newly-created
     * table partition visible for using.
     */
    CommandCounterIncrement();

    /*
     * If add interval partition in the DDL, do not need to change the csn
     * because the scn has been changed in the DDL.
     */
    if (!isDDL) {
        UpdatePgObjectChangecsn(RelationGetRelid(rel), rel->rd_rel->relkind);
    }

    return newPartOid;
}

Oid HeapAddHashPartition(Relation pgPartRel, Oid partTableOid, Oid partTablespace, Oid bucketOid,
    HashPartitionDefState* newHashPartDef, Oid ownerid, Datum reloptions, const bool* isTimestamptz,
    StorageType storage_type, int2vector* subpartition_key, bool isSubpartition)
{
    Datum boundaryValue = (Datum)0;
    Oid newHashPartitionOid = InvalidOid;
    Oid newPartitionTableSpaceOid = InvalidOid;
    Oid partrelfileOid = InvalidOid;
    Relation relation;
    Partition newHashPartition;

    /*missing partition definition structure*/
    if (!PointerIsValid(newHashPartDef)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("missing definition for new partition")));
    }
    /*boundary and boundary length check*/
    if (!PointerIsValid(newHashPartDef->boundary)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("boundary not defined for new partition")));
    }
    if (newHashPartDef->boundary->length != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                errmsg("too many partition keys, allowed is 1")));
    }

    /*new partition name check*/
    if (!PointerIsValid(newHashPartDef->partitionName)) {
        ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg("partition name is invalid")));
    }

    /* transform boundary value */
    bool isTime = false;
    boundaryValue = transformPartitionBoundary(newHashPartDef->boundary, &isTime);

    /* get partition tablespace oid */
    if (newHashPartDef->tablespacename) {
        newPartitionTableSpaceOid = get_tablespace_oid(newHashPartDef->tablespacename, false);
    }

    if (!OidIsValid(newPartitionTableSpaceOid)) {
        newPartitionTableSpaceOid = partTablespace;
    }

    /* Check permissions except when using database's default */
    if (OidIsValid(newPartitionTableSpaceOid) && newPartitionTableSpaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        AclResult aclresult;

        aclresult = pg_tablespace_aclcheck(newPartitionTableSpaceOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(newPartitionTableSpaceOid));
        }
    }

    /* create partition */
    GetNewPartitionOidAndNewPartrelfileOid(newHashPartDef->subPartitionDefState, &newHashPartitionOid, &partrelfileOid,
                                           pgPartRel, newPartitionTableSpaceOid);

    LockPartitionOid(partTableOid, (uint32)newHashPartitionOid, AccessExclusiveLock);

    bool forPartitionTable = false;
    if (isSubpartition) { /* Level-2 partition of subpartition */
        forPartitionTable = false;
    } else if (newHashPartDef->subPartitionDefState != NULL) { /* Level-2 partition of subpartition */
        forPartitionTable = true;
    } else { /* Common partition */
        forPartitionTable = false;
    }
    newHashPartition = heapCreatePartition(newHashPartDef->partitionName,
                                           forPartitionTable,
                                           newPartitionTableSpaceOid,
                                           newHashPartitionOid,
                                           partrelfileOid,
                                           bucketOid,
                                           ownerid,
                                           storage_type,
                                           false,
                                           reloptions);

    Assert(newHashPartitionOid == PartitionGetPartid(newHashPartition));
    if (isSubpartition) {
        InitSubPartitionDef(newHashPartition, partTableOid, PART_STRATEGY_HASH);
    } else {
        InitPartitionDef(newHashPartition, partTableOid, PART_STRATEGY_HASH);
    }

    /* step 3: insert into pg_partition tuple */
    addNewPartitionTuple(pgPartRel, /* RelationData pointer for pg_partition */
        newHashPartition,               /* PartitionData pointer for partition */
        subpartition_key,                       /* */
        NULL,
        (Datum)0,      /* interval*/
        boundaryValue, /* max values */
        (Datum)0,      /* transition point */
        reloptions);

    if (isSubpartition) {
        PartitionCloseSmgr(newHashPartition);
        Partition part = newHashPartition;
        if (PartitionIsBucket(newHashPartition)) {
            part = newHashPartition->parent;
            bucketClosePartition(newHashPartition);
        }
        PartitionClose(part);
    } else {
        relation = relation_open(partTableOid, NoLock);
        PartitionCloseSmgr(newHashPartition);

        partitionClose(relation, newHashPartition, NoLock);
        relation_close(relation, NoLock);
    }
    return newHashPartitionOid;
}

static void addNewPartitionTupleForValuePartitionedTable(Relation pg_partition_rel, const char* relname,
    const Oid reloid, const Oid reltablespaceid, const TupleDesc reltupledesc, const PartitionState* partTableState,
    Datum reloptions)
{
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    HeapTuple tup;
    int2vector* partition_key_attr_no = buildPartitionKey(partTableState->partitionKey, reltupledesc);
    errno_t errorno = EOK;

    errorno = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(errorno, "\0", "\0");

    errorno = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(errorno, "\0", "\0");

    values[Anum_pg_partition_relname - 1] = NameGetDatum(relname);
    values[Anum_pg_partition_parttype - 1] = CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE);
    values[Anum_pg_partition_parentid - 1] = ObjectIdGetDatum(reloid);
    values[Anum_pg_partition_rangenum - 1] = UInt32GetDatum(0);
    values[Anum_pg_partition_intervalnum - 1] = UInt32GetDatum(0);
    values[Anum_pg_partition_partstrategy - 1] = CharGetDatum(partTableState->partitionStrategy);
    values[Anum_pg_partition_relfilenode - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reltablespace - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relpages - 1] = Float8GetDatum(0);
    values[Anum_pg_partition_reltuples - 1] = Float8GetDatum(0);
    values[Anum_pg_partition_relallvisible - 1] = UInt32GetDatum(0);
    values[Anum_pg_partition_reltoastrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reltoastidxid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_indextblid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_deltarelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reldeltaidx - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relcudescrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relcudescidx - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_indisusable - 1] = BoolGetDatum(true);
    values[Anum_pg_partition_relfrozenxid - 1] = ShortTransactionIdGetDatum(InvalidTransactionId);

    /*partition key*/
    values[Anum_pg_partition_partkey - 1] = PointerGetDatum(partition_key_attr_no);

    nulls[Anum_pg_partition_intablespace - 1] = true;
    nulls[Anum_pg_partition_intspnum - 1] = true;

    /*interval*/
    nulls[Anum_pg_partition_interval - 1] = true;

    /*maxvalue*/
    nulls[Anum_pg_partition_boundaries - 1] = true;

    /*transit point*/
    nulls[Anum_pg_partition_transit - 1] = true;

    /*reloptions*/
    if (reloptions != (Datum)0) {
        values[Anum_pg_partition_reloptions - 1] = reloptions;
        nulls[Anum_pg_partition_reloptions - 1] = false;
    } else {
        nulls[Anum_pg_partition_reloptions - 1] = true;
    }
    values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(InvalidTransactionId);
#ifndef ENABLE_MULTIPLE_NODES
    values[Anum_pg_partition_relminmxid - 1] = TransactionIdGetDatum(InvalidMultiXactId);
#endif
    /*form a tuple using values and null array, and insert it*/
    tup = heap_form_tuple(RelationGetDescr(pg_partition_rel), values, nulls);
    HeapTupleSetOid(tup, InvalidOid);
    (void)simple_heap_insert(pg_partition_rel, tup);
    CatalogUpdateIndexes(pg_partition_rel, tup);

    heap_freetuple(tup);

    if (partition_key_attr_no != NULL) {
        pfree(partition_key_attr_no);
    }
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: Add new partition tuple into pg_partition for partitioned-table
 * Notes		:
 * Parameters : pg_partition_rel:  RelationData pointer for pg_partition,
 *                                   the caller MUST get RowExclusiveLock on pg_partition
 *                relname:       partitioned table's name
 *                reloid :       partitioned table's oid in pg_class
 *                reltupledesc : TupleDesc for partitioned tuple, it is used to get partition key attribute number.
 *                partTableState: partition definition of partitioned table.
 */
static void addNewPartitionTupleForTable(Relation pg_partition_rel, const char* relname, const Oid reloid,
    const Oid reltablespaceid, const TupleDesc reltupledesc, const PartitionState* partTableState, Oid ownerid,
    Datum reloptions)
{
    Datum interval = (Datum)0;
    Datum transition_point = (Datum)0;
    Oid new_partition_oid = InvalidOid;
    int2vector* partition_key_attr_no = NULL;
    oidvector* interval_talespace = NULL;
    Relation relation = NULL;
    Partition new_partition = NULL;
    Datum newOptions;

    Oid new_partition_rfoid = InvalidOid;

    Assert(pg_partition_rel);
    if (partTableState->partitionStrategy != 'l' && partTableState->partitionStrategy != 'h') {
        RangePartitionDefState* lastPartition = NULL;
        lastPartition = (RangePartitionDefState*)lfirst(partTableState->partitionList->tail);

        if (lastPartition->boundary->length > 4) {
            ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                    errmsg("number of partition key columns MUST less or equal than 4")));
        }
    }

    partition_key_attr_no = buildPartitionKey(partTableState->partitionKey, reltupledesc);
    if (partTableState->intervalPartDef != NULL) {
        interval_talespace = BuildIntervalTablespace(partTableState->intervalPartDef);
        interval = BuildInterval(partTableState->intervalPartDef->partInterval);
    }

    /*step1: create partition relation, initialize and set tuple properties*/
    if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid)) {
        new_partition_oid = u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid;
        u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid = InvalidOid;
        new_partition_rfoid = u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_rfoid;
        u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_rfoid = InvalidOid;
    } else {
        new_partition_oid = GetNewOid(pg_partition_rel);
    }

    new_partition = heapCreatePartition(relname, /* partitioned table's name*/
        true,                                    /* this entry is for partitioned table */
        reltablespaceid,                         /*tablespace's oid for partitioned table */
        new_partition_oid,                       /* partitioned table's oid in pg_partition*/
        new_partition_rfoid,
        InvalidOid,
        ownerid,
        HEAP_DISK,
        false,
        reloptions);

    Assert(new_partition_oid == PartitionGetPartid(new_partition));
    new_partition->pd_part->parttype = PART_OBJ_TYPE_PARTED_TABLE;
    new_partition->pd_part->parentid = reloid;
    new_partition->pd_part->rangenum = 0;
    new_partition->pd_part->intervalnum = 0;
    new_partition->pd_part->partstrategy = partTableState->partitionStrategy;
    new_partition->pd_part->reltoastrelid = InvalidOid;
    new_partition->pd_part->reltoastidxid = InvalidOid;
    new_partition->pd_part->indextblid = InvalidOid;
    new_partition->pd_part->reldeltarelid = InvalidOid;
    new_partition->pd_part->reldeltaidx = InvalidOid;
    new_partition->pd_part->relcudescrelid = InvalidOid;
    new_partition->pd_part->relcudescidx = InvalidOid;
    new_partition->pd_part->indisusable = true;

    /* Update reloptions with wait_clean_gpi=n */
    newOptions = SetWaitCleanGpiRelOptions(reloptions, false);

    /*step 2: insert into pg_partition tuple*/
    addNewPartitionTuple(pg_partition_rel, /* RelationData pointer for pg_partition */
        new_partition,                     /* Local PartitionData pointer for new partition */
        partition_key_attr_no,             /* number array for partition key column of partitioned table*/
        interval_talespace,
        interval,         /* interval partitioned table's interval*/
        (Datum)0,         /* partitioned table's boundary value is empty in pg_partition */
        transition_point, /* interval's partitioned table's transition point*/
        newOptions);
    relation = relation_open(reloid, NoLock);
    partitionClose(relation, new_partition, NoLock);
    relation_close(relation, NoLock);

    if (partition_key_attr_no != NULL) {
        pfree(partition_key_attr_no);
    }

    if (interval_talespace != NULL) {
        pfree(interval_talespace);
    }

    if (interval != 0) {
        pfree(DatumGetPointer(interval));
    }
}

bool IsExistDefaultSubpartitionName(List *partitionNameList, char *defaultPartitionName)
{
    ListCell *cell = NULL;
    foreach (cell, partitionNameList) {
        char *partitionName = (char *)lfirst(cell);
        if (!strcmp(partitionName, defaultPartitionName)) {
            return true;
        }
    }
    return false;
}

static void MakeDefaultSubpartitionName(PartitionState *partitionState, char **subPartitionName,
    const char *partitionName)
{
    int numLen = 0;
    int subPartitionNameLen = 0;
    List *partitionNameList = NIL;
    if (PointerIsValid(partitionState->partitionList)) {
        partitionNameList = GetPartitionNameList(partitionState->partitionList);
    } else if (PointerIsValid(partitionState->partitionNameList)) {
        partitionNameList = partitionState->partitionNameList;
    }


    for (int i = 1; i < INT32_MAX_VALUE; i++) {
        numLen = (int)log10(i) + 1;
        subPartitionNameLen = strlen(partitionName) + strlen("_subpartdefault") + numLen + 1;
        if (subPartitionNameLen > NAMEDATALEN) {
            subPartitionNameLen = strlen("sys_subpartdefault") + numLen + 1;
            *subPartitionName = (char *)palloc0(subPartitionNameLen);
            errno_t rc = snprintf_s(*subPartitionName, subPartitionNameLen, subPartitionNameLen - 1,
                                    "sys_subpartdefault%d", i);
            securec_check_ss(rc, "\0", "\0");
        } else {
            *subPartitionName = (char *)palloc0(subPartitionNameLen);
            errno_t rc = snprintf_s(*subPartitionName, subPartitionNameLen, subPartitionNameLen - 1,
                                    "%s_subpartdefault%d", partitionName, i);
            securec_check_ss(rc, "\0", "\0");
        }

        bool isExist = IsExistDefaultSubpartitionName(partitionNameList, *subPartitionName);
        if (isExist) {
            pfree_ext(*subPartitionName);
        } else {
            break;
        }
    }

    if (PointerIsValid(partitionState->partitionList)) {
        list_free_ext(partitionNameList);
    }
}

static ListPartitionDefState *MakeListDefaultSubpartition(PartitionState *partitionState, char *partitionName,
                                                   char *tablespacename)
{
    ListPartitionDefState *subPartitionDefState = makeNode(ListPartitionDefState);
    MakeDefaultSubpartitionName(partitionState, &subPartitionDefState->partitionName, partitionName);

    Const *boundaryDefault = makeNode(Const);
    boundaryDefault->ismaxvalue = true;
    boundaryDefault->location = -1;
    subPartitionDefState->boundary = list_make1(boundaryDefault);
    subPartitionDefState->tablespacename = pstrdup(tablespacename);
    return subPartitionDefState;
}

static HashPartitionDefState *MakeHashDefaultSubpartition(PartitionState *partitionState, char *partitionName,
                                                   char *tablespacename)
{
    HashPartitionDefState *subPartitionDefState = makeNode(HashPartitionDefState);
    MakeDefaultSubpartitionName(partitionState, &subPartitionDefState->partitionName, partitionName);

    Const *boundaryDefault = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), false, true);
    subPartitionDefState->boundary = list_make1(boundaryDefault);
    subPartitionDefState->tablespacename = pstrdup(tablespacename);
    return subPartitionDefState;
}

static RangePartitionDefState *MakeRangeDefaultSubpartition(PartitionState *partitionState, char *partitionName,
                                                     char *tablespacename)
{
    RangePartitionDefState *subPartitionDefState = makeNode(RangePartitionDefState);
    MakeDefaultSubpartitionName(partitionState, &subPartitionDefState->partitionName, partitionName);

    Const *boundaryDefault = makeNode(Const);
    boundaryDefault->ismaxvalue = true;
    subPartitionDefState->boundary = list_make1(boundaryDefault);
    subPartitionDefState->tablespacename = pstrdup(tablespacename);
    return subPartitionDefState;
}

static void getSubPartitionInfo(char partitionStrategy, Node *partitionDefState,
                         List **subPartitionDefState, char **partitionName, char **tablespacename)
{
    if (partitionStrategy == PART_STRATEGY_LIST) {
        *subPartitionDefState = ((ListPartitionDefState *)partitionDefState)->subPartitionDefState;
        *partitionName = ((ListPartitionDefState *)partitionDefState)->partitionName;
        *tablespacename = ((ListPartitionDefState *)partitionDefState)->tablespacename;
    } else if (partitionStrategy == PART_STRATEGY_HASH) {
        *subPartitionDefState = ((HashPartitionDefState *)partitionDefState)->subPartitionDefState;
        *partitionName = ((HashPartitionDefState *)partitionDefState)->partitionName;
        *tablespacename = ((HashPartitionDefState *)partitionDefState)->tablespacename;
    } else {
        *subPartitionDefState = ((RangePartitionDefState *)partitionDefState)->subPartitionDefState;
        *partitionName = ((RangePartitionDefState *)partitionDefState)->partitionName;
        *tablespacename = ((RangePartitionDefState *)partitionDefState)->tablespacename;
    }
}

Node *MakeDefaultSubpartition(PartitionState *partitionState, Node *partitionDefState)
{
    PartitionState *subPartitionState = partitionState->subPartitionState;
    List *subPartitionDefStateList = NIL;
    char *partitionName = NULL;
    char *tablespacename = NULL;
    char partitionStrategy = partitionState->partitionStrategy;
    char subPartitionStrategy = subPartitionState->partitionStrategy;

    getSubPartitionInfo(partitionStrategy, partitionDefState, &subPartitionDefStateList, &partitionName,
                        &tablespacename);
    if (subPartitionStrategy == PART_STRATEGY_LIST) {
        ListPartitionDefState *subPartitionDefState =
            MakeListDefaultSubpartition(partitionState, partitionName, tablespacename);
        return (Node *)subPartitionDefState;
    } else if (subPartitionStrategy == PART_STRATEGY_HASH) {
        HashPartitionDefState *subPartitionDefState =
            MakeHashDefaultSubpartition(partitionState, partitionName, tablespacename);
        return (Node *)subPartitionDefState;
    } else {
        RangePartitionDefState *subPartitionDefState =
            MakeRangeDefaultSubpartition(partitionState, partitionName, tablespacename);
        return (Node *)subPartitionDefState;
    }
    return NULL;
}

List *addNewSubPartitionTuplesForPartition(Relation pgPartRel, Oid partTableOid, Oid partTablespace,
                                                 Oid bucketOid, Oid ownerid, Datum reloptions,
                                                 const bool *isTimestamptz, StorageType storage_type,
                                                 PartitionState *partitionState, Node *partitionDefState,
                                                 LOCKMODE partLockMode)
{
    List *subpartOidList = NIL;
    if (partitionState->subPartitionState == NULL) {
        return NIL;
    }

    PartitionState *subPartitionState = partitionState->subPartitionState;
    List *subPartitionDefStateList = NIL;
    char *partitionName = NULL;
    char *tablespacename = NULL;
    ListCell *lc = NULL;
    Oid subpartOid = InvalidOid;
    char partitionStrategy = partitionState->partitionStrategy;
    char subPartitionStrategy = subPartitionState->partitionStrategy;
    getSubPartitionInfo(partitionStrategy, partitionDefState, &subPartitionDefStateList, &partitionName,
                        &tablespacename);
    foreach (lc, subPartitionDefStateList) {
        if (subPartitionStrategy == PART_STRATEGY_LIST) {
            ListPartitionDefState *subPartitionDefState = (ListPartitionDefState *)lfirst(lc);
            subpartOid = HeapAddListPartition(pgPartRel, partTableOid, partTablespace, bucketOid,
                subPartitionDefState, ownerid, reloptions, isTimestamptz, storage_type, NULL, true);
        } else if (subPartitionStrategy == PART_STRATEGY_HASH) {
            HashPartitionDefState *subPartitionDefState = (HashPartitionDefState *)lfirst(lc);
            subpartOid = HeapAddHashPartition(pgPartRel, partTableOid, partTablespace, bucketOid,
                subPartitionDefState, ownerid, reloptions, isTimestamptz, storage_type, NULL, true);
        } else {
            RangePartitionDefState *subPartitionDefState = (RangePartitionDefState *)lfirst(lc);
            subpartOid = heapAddRangePartition(pgPartRel, partTableOid, partTablespace, bucketOid,
                subPartitionDefState, ownerid, reloptions, isTimestamptz, storage_type, partLockMode, NULL, true);
        }
        if (OidIsValid(subpartOid)) {
            subpartOidList = lappend_oid(subpartOidList, subpartOid);
        }
    }

    return subpartOidList;
}

/*
 * check whether the partion key has timestampwithzone type.
 */
static void IsPartitionKeyContainTimestampwithzoneType(const PartitionState *partTableState, const TupleDesc tupledesc,
                                                       bool *isTimestamptz, int partKeyNum)
{
    ListCell *partKeyCell = NULL;
    ColumnRef *col = NULL;
    char *columName = NULL;
    int partKeyIdx = 0;
    int attnum = tupledesc->natts;
    Form_pg_attribute *attrs = tupledesc->attrs;

    foreach (partKeyCell, partTableState->partitionKey) {
        col = (ColumnRef *)lfirst(partKeyCell);
        columName = ((Value *)linitial(col->fields))->val.str;

        isTimestamptz[partKeyIdx] = false;
        for (int i = 0; i < attnum; i++) {
            if (TIMESTAMPTZOID == attrs[i]->atttypid && 0 == strcmp(columName, attrs[i]->attname.data)) {
                isTimestamptz[partKeyIdx] = true;
                break;
            }
        }
        partKeyIdx++;
    }

    Assert(partKeyIdx == partKeyNum);
}

/* if partition's tablespace is not provided, it will inherit from parent partitioned
 * so for a subpartition, the tablespace itself is used, if not exist, inherit from the partition, and if
 * still not exist, inherit from the table */
Oid GetPartTablespaceOidForSubpartition(Oid reltablespace, const char* partTablespacename)
{
    Oid partTablespaceOid = InvalidOid;
    /* get partition tablespace oid */
    if (PointerIsValid(partTablespacename)) {
        partTablespaceOid = get_tablespace_oid(partTablespacename, false);
    }
    if (!OidIsValid(partTablespaceOid)) {
        partTablespaceOid = reltablespace;
    }
    return partTablespaceOid;
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	: add new partition tuples of partitioned table into pg_partition
 * Notes		:
 * Parameters   : pg_partition_rel:  RelationData pointer for pg_partition,
 *                                   the caller MUST get RowExclusiveLock on pg_partition
 *                relid: partitioned table's oid
 *                reltablespace: partitioned table's tablespace.
 *                               If partition's tablespace is not provided, it will inherit from parent partitioned
 * table. BufferPoolId:  partitioned table's BufferPoolId. All partitions of one partitioned table have same
 * BufferPoolId. partTableState: partition schema of partitioned table which is being created right now.
 *
 */
static void addNewPartitionTuplesForPartition(Relation pg_partition_rel, Oid relid,
    Oid reltablespace, Oid bucketOid, PartitionState* partTableState, Oid ownerid, Datum reloptions,
    const TupleDesc tupledesc, char strategy, StorageType storage_type, LOCKMODE partLockMode)
{
    int partKeyNum = list_length(partTableState->partitionKey);
    bool isTimestamptzForPartKey[partKeyNum];
    memset_s(isTimestamptzForPartKey, sizeof(isTimestamptzForPartKey), 0, sizeof(isTimestamptzForPartKey));
    IsPartitionKeyContainTimestampwithzoneType(partTableState, tupledesc, isTimestamptzForPartKey, partKeyNum);

    bool *isTimestamptzForSubPartKey = NULL;
    if (partTableState->subPartitionState != NULL) {
        int subPartKeyNum = list_length(partTableState->subPartitionState->partitionKey);
        isTimestamptzForSubPartKey = (bool*)palloc0(sizeof(bool) * subPartKeyNum);
        IsPartitionKeyContainTimestampwithzoneType(partTableState->subPartitionState, tupledesc,
                                                   isTimestamptzForSubPartKey, subPartKeyNum);
    }

    ListCell* cell = NULL;

    Assert(pg_partition_rel);

    List *subPartitionKey = NIL;
    int2vector* subpartition_key_attr_no = NULL;
    if (partTableState->subPartitionState != NULL) {
        subPartitionKey = partTableState->subPartitionState->partitionKey;
        subpartition_key_attr_no = buildPartitionKey(subPartitionKey, tupledesc);
    }

    /*insert partition entries into pg_partition*/
    foreach (cell, partTableState->partitionList) {
        if (strategy == PART_STRATEGY_LIST) {
            ListPartitionDefState* partitionDefState = (ListPartitionDefState*)lfirst(cell);
            if (partTableState->subPartitionState != NULL && partitionDefState->subPartitionDefState == NULL) {
                Node *subPartitionDefState = MakeDefaultSubpartition(partTableState, (Node *)partitionDefState);
                partitionDefState->subPartitionDefState =
                    lappend(partitionDefState->subPartitionDefState, subPartitionDefState);
            }
            Oid partitionOid = HeapAddListPartition(pg_partition_rel,
                relid,
                reltablespace,
                bucketOid,
                partitionDefState,
                ownerid,
                reloptions,
                isTimestamptzForPartKey,
                storage_type,
                subpartition_key_attr_no);

            Oid partTablespaceOid =
                GetPartTablespaceOidForSubpartition(reltablespace, partitionDefState->tablespacename);
            List *subpartitionOidList = addNewSubPartitionTuplesForPartition(pg_partition_rel, partitionOid,
                partTablespaceOid, bucketOid, ownerid, reloptions, isTimestamptzForSubPartKey, storage_type,
                partTableState, (Node *)partitionDefState, partLockMode);
            if (subpartitionOidList != NIL) {
                list_free_ext(subpartitionOidList);
            }
        } else if (strategy == PART_STRATEGY_HASH) {
            HashPartitionDefState* partitionDefState = (HashPartitionDefState*)lfirst(cell);
            if (partTableState->subPartitionState != NULL && partitionDefState->subPartitionDefState == NULL) {
                Node *subPartitionDefState = MakeDefaultSubpartition(partTableState, (Node *)partitionDefState);
                partitionDefState->subPartitionDefState =
                    lappend(partitionDefState->subPartitionDefState, subPartitionDefState);
            }
            Oid partitionOid = HeapAddHashPartition(pg_partition_rel,
                relid,
                reltablespace,
                bucketOid,
                partitionDefState,
                ownerid,
                reloptions,
                isTimestamptzForPartKey,
                storage_type,
                subpartition_key_attr_no);

            Oid partTablespaceOid =
                GetPartTablespaceOidForSubpartition(reltablespace, partitionDefState->tablespacename);
            List *subpartitionOidList = addNewSubPartitionTuplesForPartition(pg_partition_rel, partitionOid,
                partTablespaceOid, bucketOid, ownerid, reloptions, isTimestamptzForSubPartKey, storage_type,
                partTableState, (Node *)partitionDefState, partLockMode);
            if (subpartitionOidList != NIL) {
                list_free_ext(subpartitionOidList);
            }
        } else {
            RangePartitionDefState* partitionDefState = (RangePartitionDefState*)lfirst(cell);
            if (partTableState->subPartitionState != NULL && partitionDefState->subPartitionDefState == NULL) {
                Node *subPartitionDefState = MakeDefaultSubpartition(partTableState, (Node *)partitionDefState);
                partitionDefState->subPartitionDefState =
                    lappend(partitionDefState->subPartitionDefState, subPartitionDefState);
            }
            Oid partitionOid = heapAddRangePartition(pg_partition_rel,
                relid,
                reltablespace,
                bucketOid,
                (RangePartitionDefState*)lfirst(cell),
                ownerid,
                reloptions,
                isTimestamptzForPartKey,
                storage_type,
                partLockMode,
                subpartition_key_attr_no);

            Oid partTablespaceOid =
                GetPartTablespaceOidForSubpartition(reltablespace, partitionDefState->tablespacename);
            List *subpartitionOidList = addNewSubPartitionTuplesForPartition(pg_partition_rel, partitionOid,
                partTablespaceOid, bucketOid, ownerid, reloptions, isTimestamptzForSubPartKey, storage_type,
                partTableState, (Node *)partitionDefState, partLockMode);
            if (subpartitionOidList != NIL) {
                list_free_ext(subpartitionOidList);
            }
        }
    }
    if (isTimestamptzForSubPartKey != NULL) {
        pfree_ext(isTimestamptzForSubPartKey);
    }
}

void heap_truncate_one_part(Relation rel, Oid partOid)
{
    SubTransactionId mySubid;
    Partition p = partitionOpen(rel, partOid, AccessExclusiveLock);
    Oid toastOid = p->pd_part->reltoastrelid;
    List* partIndexlist = NULL;
    Relation parentIndex = NULL;
    mySubid = GetCurrentSubTransactionId();
    MultiXactId multiXid = GetOldestMultiXactId();;

    partIndexlist = searchPartitionIndexesByblid(partOid);

    /*
     * Truncate a partition need a new relfilenode.
     * Otherwise, recovery may fail in the following scenarios:
     *
     *  start transaction;
     *  create table p1 *** partition by ***;
     *  copy p1 from ***;
     *	alter table p1 truncate partition ***;
     *	commit;
     *	copy p1 from ***;
     *	update p1 set ***;
     */

    CheckTableForSerializableConflictIn(rel);

    PartitionSetNewRelfilenode(rel, p, u_sess->utils_cxt.RecentXmin,
                               RelationIsColStore(rel) ? InvalidMultiXactId : multiXid);

    /* truncate the toast table */
    if (toastOid != InvalidOid) {
        Relation toastRel = heap_open(toastOid, AccessExclusiveLock);
        RelationSetNewRelfilenode(toastRel, u_sess->utils_cxt.RecentXmin, multiXid);
        heap_close(toastRel, NoLock);
    }

    /* rebuild part index */
    if (PointerIsValid(partIndexlist)) {
        ListCell* lc = NULL;
        Oid partIndId = InvalidOid;
        Oid parentIndId = InvalidOid;
        HeapTuple partIndexTuple = NULL;
        Partition indexPart = NULL;

        foreach (lc, partIndexlist) {
            partIndexTuple = (HeapTuple)lfirst(lc);
            parentIndId = (((Form_pg_partition)GETSTRUCT(partIndexTuple)))->parentid;
            partIndId = HeapTupleGetOid(partIndexTuple);
            parentIndex = index_open(parentIndId, AccessShareLock);
            indexPart = partitionOpen(parentIndex, partIndId, AccessExclusiveLock);
            reindex_partIndex(rel, p, parentIndex, indexPart);
            partitionClose(parentIndex, indexPart, NoLock);
            index_close(parentIndex, NoLock);
        }
    }

    /* rebuild the toast index */
    if (toastOid != InvalidOid) {
        (void)ReindexRelation(toastOid, 0, REINDEX_BTREE_INDEX, NULL);
    }

    freePartList(partIndexlist);
    partitionClose(rel, p, NoLock);
}

/*
 * Compute the hash value of a hash-bucket tuple
 * */
int2 computeTupleBucketId(Relation rel, HeapTuple tuple)
{
    TupleDesc tup_desc = rel->rd_att;
    int2vector* col_ids = rel->rd_bucketkey->bucketKey; /* vector of column id */
    MultiHashKey mkeys;

    Assert(REALTION_BUCKETKEY_VALID(rel));
    mkeys.keyNum = (uint32)col_ids->dim1;
    mkeys.keyTypes = rel->rd_bucketkey->bucketKeyType;
    mkeys.locatorType = LOCATOR_TYPE_HASH;

    /* set the mutikey values */
    mkeys.keyValues = (Datum*)palloc(mkeys.keyNum * sizeof(Datum));
    mkeys.isNulls = (bool*)palloc(mkeys.keyNum * sizeof(bool));

    for (int i = 0; i < col_ids->dim1; i++) {
        mkeys.isNulls[i] = false;
        mkeys.keyValues[i] =
            fastgetattr((tuple), col_ids->values[i], tup_desc, &mkeys.isNulls[i]);  // get the column value
    }

    uint32 hashValue = hash_multikey(&mkeys);

    pfree(mkeys.keyValues);
    pfree(mkeys.isNulls);

    return GetBucketID(hashValue, rel->rd_bucketmapsize);
}

int lookupHBucketid(oidvector *buckets, int low, int2 bktId)
{
    int high = buckets->dim1 - 1;
    Assert(low <= high);
    /* binary search */
    while (low <= high) {
        int  mid = (high + low) / 2;
        int2 bid = buckets->values[mid];
        if (bktId == bid) {
            return mid;
        } else if (bktId > bid) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return -1;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get special table partition for a tuple
 *			: create a partition if necessary
 * Description	:
 * Notes		:
 */
Oid heapTupleGetPartitionId(Relation rel, void *tuple, bool isDDL)
{
    Oid partitionid = InvalidOid;

    /* get routing result */
    partitionRoutingForTuple(rel, tuple, u_sess->catalog_cxt.route);

    /* if the partition exists, return partition's oid */
    if (u_sess->catalog_cxt.route->fileExist) {
        Assert(OidIsValid(u_sess->catalog_cxt.route->partitionId));
        partitionid = u_sess->catalog_cxt.route->partitionId;
        return partitionid;
    }

    /*
     * feedback for non-existing table partition.
     *   If the routing result indicates a range partition, give error report
     */
    switch (u_sess->catalog_cxt.route->partArea) {
        /*
         * If it is a range partition, give error report
         */
        case PART_AREA_RANGE: {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND), errmsg("inserted partition key does not map to any table partition")));
        } break;
        case PART_AREA_INTERVAL: {
            return AddNewIntervalPartition(rel, tuple, isDDL);
        } break;
        case PART_AREA_LIST: {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND), errmsg("inserted partition key does not map to any table partition")));
        } break;
        case PART_AREA_HASH: {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND), errmsg("inserted partition key does not map to any table partition")));
        } break;
        /* never happen; just to be self-contained */
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND),
                    errmsg("Inserted partition key does not map to any table partition"),
                    errdetail("Unrecognized PartitionArea %d", u_sess->catalog_cxt.route->partArea)));
        } break;
    }

    return partitionid;
}

Oid heapTupleGetSubPartitionId(Relation rel, void *tuple)
{
    Oid partitionId = InvalidOid;
    Oid subPartitionId = InvalidOid;
    Partition part = NULL;
    Relation partRel = NULL;
    /* get partititon oid for the record */
    partitionId = heapTupleGetPartitionId(rel, tuple);
    part = partitionOpen(rel, partitionId, RowExclusiveLock);
    partRel = partitionGetRelation(rel, part);
    /* get subpartititon oid for the record */
    subPartitionId = heapTupleGetPartitionId(partRel, tuple);

    releaseDummyRelation(&partRel);
    partitionClose(rel, part, RowExclusiveLock);

    return subPartitionId;
}

static bool binary_upgrade_is_next_part_pg_partition_oid_valid()
{
    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_oid) {
        return false;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid >=
        u_sess->upg_cxt.binary_upgrade_max_part_pg_partition_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt
                .binary_upgrade_next_part_pg_partition_oid[u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid])) {
        return false;
    }

    return true;
}

static Oid binary_upgrade_get_next_part_pg_partition_oid()
{
    Oid old_part_pg_partition_oid = InvalidOid;
    if (false == binary_upgrade_is_next_part_pg_partition_oid_valid()) {
        return InvalidOid;
    }

    old_part_pg_partition_oid =
        u_sess->upg_cxt
            .binary_upgrade_next_part_pg_partition_oid[u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_pg_partition_oid[u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid++;

    return old_part_pg_partition_oid;
}

static Oid binary_upgrade_get_next_part_pg_partition_rfoid()
{
    Oid old_part_pg_partition_rfoid = InvalidOid;

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_rfoid) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_rfoid >=
        u_sess->upg_cxt.binary_upgrade_max_part_pg_partition_rfoid) {
        return InvalidOid;
    }

    old_part_pg_partition_rfoid =
        u_sess->upg_cxt
            .binary_upgrade_next_part_pg_partition_rfoid[u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_rfoid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_pg_partition_rfoid[u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_rfoid++;

    return old_part_pg_partition_rfoid;
}

bool binary_upgrade_is_next_part_toast_pg_class_oid_valid()
{
    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_oid) {
        return false;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid >=
        u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_oid
                        [u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid])) {
        return false;
    }

    return true;
}

static Oid binary_upgrade_get_next_part_toast_pg_class_oid()
{
    Oid old_part_toast_pg_class_oid = InvalidOid;
    if (false == binary_upgrade_is_next_part_toast_pg_class_oid_valid()) {
        return InvalidOid;
    }

    old_part_toast_pg_class_oid =
        u_sess->upg_cxt
            .binary_upgrade_next_part_toast_pg_class_oid[u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_toast_pg_class_oid[u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid++;

    return old_part_toast_pg_class_oid;
}

static Oid binary_upgrade_get_next_part_toast_pg_class_rfoid()
{
    Oid old_part_toast_pg_class_rfoid = InvalidOid;

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_rfoid) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_rfoid >=
        u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_class_rfoid) {
        return InvalidOid;
    }

    old_part_toast_pg_class_rfoid = u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_rfoid
                                        [u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_rfoid];

    u_sess->upg_cxt
        .binary_upgrade_next_part_toast_pg_class_rfoid[u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_rfoid++;

    return old_part_toast_pg_class_rfoid;
}

static int TransformClusterColNameList(Oid relId, List* colList, int16* attnums)
{
    ListCell* l = NULL;
    int attnum;

    attnum = 0;
    foreach (l, colList) {
        char* attname = strVal(lfirst(l));
        HeapTuple atttuple;

        atttuple = SearchSysCacheAttName(relId, attname);
        if (!HeapTupleIsValid(atttuple))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", attname)));
        if (attnum >= INDEX_MAX_KEYS)
            ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_COLUMNS),
                    errmsg("cannot have more than %d keys in a cluster key", INDEX_MAX_KEYS)));
        if ((((Form_pg_attribute)GETSTRUCT(atttuple))->atttypid) == HLL_OID)
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("partial cluster key don't support HLL type")));
        attnums[attnum] = ((Form_pg_attribute)GETSTRUCT(atttuple))->attnum;
        ReleaseSysCache(atttuple);
        attnum++;
    }

    return attnum;
}

void SetRelHasClusterKey(Relation rel, bool has)
{
    Relation relrel;
    HeapTuple reltup;
    Form_pg_class relStruct;

    relrel = heap_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(rel))));

    relStruct = (Form_pg_class)GETSTRUCT(reltup);

    if (relStruct->relhasclusterkey != has) {
        /* update relhasclusterkey */
        relStruct->relhasclusterkey = has;

        simple_heap_update(relrel, &reltup->t_self, reltup);

        /* keep catalog indexes current */
        CatalogUpdateIndexes(relrel, reltup);
    } else {
        /* Skip the disk update, but force relcache inval anyway */
        CacheInvalidateRelcache(rel);
    }

    heap_freetuple(reltup);
    heap_close(relrel, RowExclusiveLock);
}

/*
 * Add cluster key constraint for relation.
 */
List* AddRelClusterConstraints(Relation rel, List* clusterKeys)
{
    if (!RelationIsColStore(rel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("partial cluster key constraint does not support row/timeseries store")));
    }

    ListCell* cell = NULL;

    int nKeys = list_length(clusterKeys);

    foreach (cell, clusterKeys) {
        Constraint* cdef = (Constraint*)lfirst(cell);

        Assert(cdef->contype == CONSTR_CLUSTER);

        List* colNameList = cdef->keys;
        int colNum = list_length(colNameList);

        int16* attNums = (int16*)palloc(sizeof(int16) * colNum);

        TransformClusterColNameList(rel->rd_id, colNameList, attNums);

        char* conname = cdef->conname;
        if (conname != NULL) {
            if (ConstraintNameIsUsed(CONSTRAINT_RELATION, RelationGetRelid(rel), RelationGetNamespace(rel), conname))
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("constraint \"%s\" for relation \"%s\" already exists",
                            conname,
                            RelationGetRelationName(rel))));
        } else {

            conname =
                ChooseConstraintName(RelationGetRelationName(rel), NULL, "cluster", RelationGetNamespace(rel), NIL);
        }

        /*
         * Create the Check Constraint
         */
        CreateConstraintEntry(conname, /* Constraint Name */
            RelationGetNamespace(rel), /* namespace */
            CONSTRAINT_CLUSTER,        /* Constraint Type */
            false,                     /* Is Deferrable */
            false,                     /* Is Deferred */
            true,                      /* Is Validated */
            RelationGetRelid(rel),     /* relation */
            attNums,                   /* attrs in the constraint */
            colNum,                    /* # key attrs in the constraint */
            colNum,                    /* total # attrs (include attrs and key attrs) in the constraint */
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
            NULL,                   /* not an exclusion constraint */
            NULL,                   /* Tree form of check constraint */
            NULL,                   /* Binary form of check constraint */
            NULL,                   /* Source form of check constraint */
            true,                   /* conislocal */
            0,                      /* coninhcount */
            true,                   /* connoinherit */
            cdef->inforConstraint); /* @hdfs informational constraint */

        pfree(attNums);
    }

    if (nKeys > 0)
        SetRelHasClusterKey(rel, true);

    return NULL;
}

/*
 * @hdfs
 * Brief        : Check the constraint from pg_constraint.
 * Description  : Check the constraint from pg_constraint. According to ccname, judge whether
                   existence of the same constraint in pg_constraint for this rel or not.
 * Input        : ccname, constraint name to be found.
                  rel, target relation. Check if the rel has the same constraint named as ccname.
 * Output       : none.
 * Return Value : If we find the same constraint in rel return true, otherwise return false.
 * Notes        : none.
 */
bool FindExistingConstraint(const char* ccname, Relation rel)
{
    bool found = false;
    Relation conDesc;
    SysScanDesc conscan;
    ScanKeyData skey[2];
    HeapTuple tup;

    /* Search for a pg_constraint entry with same name and relation */
    conDesc = heap_open(ConstraintRelationId, ShareUpdateExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(ccname));

    ScanKeyInit(&skey[1],
        Anum_pg_constraint_connamespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetNamespace(rel)));

    conscan = systable_beginscan(conDesc, ConstraintNameNspIndexId, true, NULL, 2, skey);

    while (HeapTupleIsValid(tup = systable_getnext(conscan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);

        if (0 == pg_strcasecmp(NameStr(con->conname), ccname)) {
            /* Found it. */
            found = true;
            break;
        }
    }

    systable_endscan(conscan);
    heap_close(conDesc, ShareUpdateExclusiveLock);

    return found;
}

/**
 * @Description: Build the column map. Store the column number using
 * bitmap method.
 * @in tuple_desc, A tuple descriptor.
 * @return reutrn the column map.
 */
char* make_column_map(TupleDesc tuple_desc)
{
#define COLS_IN_BYTE 8

    Form_pg_attribute* attrs = tuple_desc->attrs;
    char* col_map = (char*)palloc0((MaxHeapAttributeNumber + COLS_IN_BYTE) / COLS_IN_BYTE);
    int col_cnt;

    Assert(tuple_desc->natts > 0);
    for (col_cnt = 0; col_cnt < tuple_desc->natts; col_cnt++) {

        if (!attrs[col_cnt]->attisdropped && attrs[col_cnt]->attnum > 0) {
            col_map[attrs[col_cnt]->attnum >> 3] |= (1 << (attrs[col_cnt]->attnum % COLS_IN_BYTE));
        }
    }

    return col_map;
}

/**
 * @Description: check whether the partition keys has timestampwithzone type.
 * @input: partTableRel, the partition table relation.
 * @return: a bool array to indicate the result. The length of array is equal to the number of partition keys.
 * @Notes: remember to pfree the array.
 */
bool* CheckPartkeyHasTimestampwithzone(Relation partTableRel, bool isForSubPartition)
{
    Relation pgPartRel = NULL;
    HeapTuple partitionTableTuple = NULL;
    Datum partkey_raw = (Datum)0;
    bool isNull = false;
    int n_key_column = 0;
    ArrayType* partkey_columns = NULL;
    int16* attnums = NULL;
    int relationAttNumber = 0;
    TupleDesc relationTupleDesc = NULL;
    Form_pg_attribute* relationAtts = NULL;

    pgPartRel = relation_open(PartitionRelationId, AccessShareLock);

    if (isForSubPartition || RelationIsPartitionOfSubPartitionTable(partTableRel)) {
        partitionTableTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partTableRel->rd_id));
    } else {
        partitionTableTuple =
        searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, ObjectIdGetDatum(partTableRel->rd_id));
    }

    if (NULL == partitionTableTuple) {
        relation_close(pgPartRel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null partition key value for relation \"%s\" in check partkey type.",
                    RelationGetRelationName(partTableRel))));
    }

    partkey_raw = heap_getattr(partitionTableTuple, Anum_pg_partition_partkey, RelationGetDescr(pgPartRel), &isNull);

    if (isNull) {
        relation_close(pgPartRel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("null partition key value for relation \"%s\" in check partkey type.",
                    RelationGetRelationName(partTableRel))));
    }
    /*  convert Datum to ArrayType*/
    partkey_columns = DatumGetArrayTypeP(partkey_raw);

    /* Get number of partition key columns from int2verctor*/
    n_key_column = ARR_DIMS(partkey_columns)[0];

    /*CHECK: the ArrayType of partition key is valid*/
    if (ARR_NDIM(partkey_columns) != 1 || n_key_column < 0 || n_key_column > RANGE_PARTKEYMAXNUM ||
        ARR_HASNULL(partkey_columns) || ARR_ELEMTYPE(partkey_columns) != INT2OID) {
        relation_close(pgPartRel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("partition key column's number of relation \"%s\" is not a 1-D smallint array in check partkey "
                       "type.",
                    RelationGetRelationName(partTableRel))));
    }
    Assert(n_key_column <= RANGE_PARTKEYMAXNUM);
    /* Get int2 array of partition key column numbers*/
    attnums = (int16*)ARR_DATA_PTR(partkey_columns);

    relationTupleDesc = partTableRel->rd_att;
    relationAttNumber = relationTupleDesc->natts;
    relationAtts = relationTupleDesc->attrs;

    bool* isTimestamptz = (bool*)palloc0(sizeof(bool) * n_key_column);

    for (int i = 0; i < n_key_column; i++) {
        int attnum = (int)(attnums[i]);
        if (attnum >= 1 && attnum <= relationAttNumber) {
            if (relationAtts[attnum - 1]->atttypid == TIMESTAMPTZOID) {
                isTimestamptz[i] = true;
            }
        } else {
            relation_close(pgPartRel, AccessShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("partition key column's number of %s not in the range of all its columns",
                        RelationGetRelationName(partTableRel))));
        }
    }

    if (isForSubPartition || RelationIsPartitionOfSubPartitionTable(partTableRel))
        ReleaseSysCache(partitionTableTuple);
    else
        heap_freetuple_ext(partitionTableTuple);

    relation_close(pgPartRel, AccessShareLock);
    return isTimestamptz;
}

/*
 * LockSeqConstraints
 *
 * Lock the sequence that the Constraint depend
 */
static void LockSeqConstraints(Relation rel, List* Constraints)
{
    if (rel == NULL || Constraints == NULL) {
        return;
    }

    /* We only Lock sequence now, so just check the func involved sequece */
    contain_func_context context =
        init_contain_func_context(list_make4_oid(NEXTVALFUNCOID, CURRVALFUNCOID, SETVAL1FUNCOID, SETVAL3FUNCOID), true);
    ListCell* lc = NULL;
    ListCell* fitem = NULL;
    foreach (lc, Constraints) {
        CookedConstraint* constr = (CookedConstraint*)lfirst(lc);

        /* contype should be CONSTR_DEFAULT or CONSTR_CHECK */
        Assert(constr->contype == CONSTR_DEFAULT || constr->contype == CONSTR_CHECK);

        (void)contains_specified_func(constr->expr, &context);

        foreach (fitem, context.func_exprs) {
            FuncExpr* func = (FuncExpr*)lfirst(fitem);
            Node* node = (Node*)linitial(func->args);
            if (IsA(node, Const)) {
                Oid seqid = ((Const*)node)->constvalue;
                LockRelationOid(seqid, AccessShareLock);
                Relation seq = RelationIdGetRelation(seqid);
                if (!RelationIsValid(seq) || !RelationIsSequnce(seq)) {
                    if (constr->contype == CONSTR_DEFAULT) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_PARAMETER),
                                errmsg("Object in DEFAULT constraints of column \"%s\" doesn't exists",
                                    quote_identifier((char*)attnumAttName(rel, constr->attnum)))));
                    } else {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_PARAMETER),
                                errmsg("Object in CHECK constraints doesn't exists")));
                    }
                }
                RelationClose(seq);
            }
        }

        if (context.func_exprs) {
            list_free(context.func_exprs);
            context.func_exprs = NIL;
        }
    }

    list_free(context.funcids);
    context.funcids = NIL;

    return;
}

/*
 * Check whether a materialized view is in an initial, unloaded state.
 *
 * The check here must match what is set up in heap_create_init_fork().
 * Currently the init fork is an empty file.  A missing heap is also
 * considered to be unloaded.
 */
bool
heap_is_matview_init_state(Relation rel)
{
    Assert(rel->rd_rel->relkind == RELKIND_MATVIEW);

    /* no storage is allocated on CN, so always false */
    if (IS_PGXC_COORDINATOR)
        return false;

    RelationOpenSmgr(rel);

    if (!smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
        return true;
    }

    return (smgrnblocks(rel->rd_smgr, MAIN_FORKNUM) < 1);
}

/* Get index's indnkeyatts value with indexTuple */
int GetIndexKeyAttsByTuple(Relation relation, HeapTuple indexTuple)
{
    bool isnull = false;
    Datum indkeyDatum;
    TupleDesc tupleDesc = RelationIsValid(relation) ? RelationGetDescr(relation) : GetDefaultPgIndexDesc();
    Form_pg_index index = (Form_pg_index)GETSTRUCT(indexTuple);

    /*
     * This scenario will only occur after the upgrade, This scenario means that
     * the indnatts and Indnkeyatts of all current indexes are equal
     */
    if (heap_attisnull(indexTuple, Anum_pg_index_indnkeyatts, NULL)) {
        return index->indnatts;
    }

    indkeyDatum = fastgetattr(indexTuple, Anum_pg_index_indnkeyatts, tupleDesc, &isnull);
    Assert(!isnull);

    return DatumGetInt16(indkeyDatum);
}

void AddOrDropUidsAttr(Oid relOid, bool oldRelHasUids, bool newRelHasUids)
{
    /* reloption uids don't change, do nothing */
    if (oldRelHasUids == newRelHasUids) {
        return;
    }
    /* insert uid attr if new reloption has uid */
    if (newRelHasUids) {
        Relation rel = heap_open(AttributeRelationId, RowExclusiveLock);
        CatalogIndexState indstate = CatalogOpenIndexes(rel);
        FormData_pg_attribute attStruct;
        errno_t rc;
        int uidAttrNum = -UidAttributeNumber;
        rc = memcpy_s(&attStruct, sizeof(FormData_pg_attribute),
            (char*)SysAtt[uidAttrNum - 1], sizeof(FormData_pg_attribute));
        securec_check(rc, "\0", "\0");
        /* Fill in the correct relation OID in the copied tuple */
        attStruct.attrelid = relOid;
        InsertPgAttributeTuple(rel, &attStruct, indstate);
        CatalogCloseIndexes(indstate);
        heap_close(rel, RowExclusiveLock);

        /* Also insert uid backup table */
        InsertUidEntry(relOid);
    } else { /* delete attr if new reloption does not has uid */
        ObjectAddress object;
        object.classId = RelationRelationId;
        object.objectId = relOid;
        object.objectSubId = UidAttributeNumber;
        performDeletion(&object, DROP_RESTRICT, 0);
    }
}

