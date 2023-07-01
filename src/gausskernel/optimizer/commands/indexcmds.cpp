/* -------------------------------------------------------------------------
 *
 * indexcmds.cpp
 *	  openGauss define and remove index code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/indexcmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_delta.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "catalog/heap.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#ifdef PGXC
#include "optimizer/pgxcship.h"
#include "parser/parse_utilcmd.h"
#include "pgxc/pgxc.h"
#endif
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/tcap.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"

#include "securec.h"

/* non-export function prototypes */
void CheckPredicate(Expr* predicate);
Oid GetIndexOpClass(List* opclass, Oid attrType, const char* accessMethodName, Oid accessMethodId);
static char* ChooseIndexName(const char* tabname, Oid namespaceId, const List* colnames, const List* exclusionOpNames,
    bool primary, bool isconstraint, bool psort = false);
static char* ChoosePartitionIndexName(const char* partname, Oid partitionedindexId,
    List* colnames, /* List *exclusionOpNames, */
    bool primary, bool isconstraint, bool psort = false);

static char* ChoosePartitionName(
    const char* name1, const char* name2, const char* label, Oid partitionedrelid, char partType);
static char* ChooseIndexNameAddition(const List* colnames);
static void RangeVarCallbackForReindexIndex(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* arg);
static void checkTableForReindexConcurrently(Relation heapRelation);
static bool checkIndexForReindexConcurrently(Relation indexRelation, bool reindexIndex);
static bool checkIndexPartitionForReindexConcurrently(Relation indexRelation, Oid indexPartitionOid);
static List* getToastOidsInReindexConcurrently(Relation heapRelation, Oid heapPartitionId);
static void prepareReindexTableConcurrently(Oid relationOid, Oid relationPartOid, List** rt_heapRelationIds,
    List** rt_heapPartitionIds, List** rt_indexIds, List** rt_indexPartIds, MemoryContext private_context);
static void prepareReindexIndexConcurrently(Oid relationOid, Oid relationPartOid, List** rt_heapRelationIds,
    List** rt_heapPartitionIds, List** rt_indexIds, List** rt_indexPartIds, MemoryContext private_context);
static bool ReindexRelationConcurrently(Oid relationOid, Oid relationPartOid, AdaptMem* memInfo = NULL, bool dbWide = false);
static bool columnIsExist(Relation rel, const Form_pg_attribute attTup, const List* indexParams);
static bool relationHasInformationalPrimaryKey(const Relation conrel);
static void handleErrMsgForInfoCnstrnt(const IndexStmt* stmt, const Relation rel);
static void buildConstraintNameForInfoCnstrnt(
    const IndexStmt* stmt, Relation rel, char** indexRelationName, Oid namespaceId, const List* indexColNames);
static Oid buildInformationalConstraint(
    IndexStmt* stmt, Oid indexRelationId, const char* indexRelationName, Relation rel, IndexInfo* indexInfo, Oid namespaceId);
static bool CheckGlobalIndexCompatible(Oid relOid, bool isGlobal, const IndexInfo* indexInfo, Oid methodOid);
static bool CheckIndexMethodConsistency(HeapTuple indexTuple, Relation indexRelation, Oid currMethodOid);
static bool CheckSimpleAttrsConsistency(HeapTuple tarTuple, const int16* currAttrsArray, int currKeyNum);
static int AttrComparator(const void* a, const void* b);
static void AddIndexColumnForGpi(IndexStmt* stmt);
static void AddIndexColumnForCbi(IndexStmt* stmt);
static void CheckIndexParamsNumber(IndexStmt* stmt);
static bool CheckIdxParamsOwnPartKey(Relation rel, const List* indexParams);
static bool CheckWhetherForbiddenFunctionalIdx(Oid relationId, Oid namespaceId, List* indexParams);

struct ReindexIndexCallbackState {
    bool concurrent;        /* flag from statement */
    Oid  locked_table_oid;  /* tracks previously locked table */
};

/*
 * CheckIndexCompatible
 *		Determine whether an existing index definition is compatible with a
 *		prospective index definition, such that the existing index storage
 *		could become the storage of the new index, avoiding a rebuild.
 *
 * 'heapRelation': the relation the index would apply to.
 * 'accessMethodName': name of the AM to use.
 * 'attributeList': a list of IndexElem specifying columns and expressions
 *		to index on.
 * 'exclusionOpNames': list of names of exclusion-constraint operators,
 *		or NIL if not an exclusion constraint.
 *
 * This is tailored to the needs of ALTER TABLE ALTER TYPE, which recreates
 * any indexes that depended on a changing column from their pg_get_indexdef
 * or pg_get_constraintdef definitions.  We omit some of the sanity checks of
 * DefineIndex.  We assume that the old and new indexes have the same number
 * of columns and that if one has an expression column or predicate, both do.
 * Errors arising from the attribute list still apply.
 *
 * Most column type changes that can skip a table rewrite do not invalidate
 * indexes.  We ackowledge this when all operator classes, collations and
 * exclusion operators match.  Though we could further permit intra-opfamily
 * changes for btree and hash indexes, that adds subtle complexity with no
 *  concrete benefit for core types. Note, that INCLUDE columns aren't
 * checked by this function, for them it's enough that table rewrite is
 * skipped.

 * When a comparison or exclusion operator has a polymorphic input type, the
 * actual input types must also match.	This defends against the possibility
 * that operators could vary behavior in response to get_fn_expr_argtype().
 * At present, this hazard is theoretical: check_exclusion_constraint() and
 * all core index access methods decline to set fn_expr for such calls.
 *
 * We do not yet implement a test to verify compatibility of expression
 * columns or predicates, so assume any such index is incompatible.
 */
bool CheckIndexCompatible(Oid oldId, char* accessMethodName, List* attributeList, List* exclusionOpNames)
{
    bool isconstraint = false;
    Oid* typeObjectId = NULL;
    Oid* collationObjectId = NULL;
    Oid* classObjectId = NULL;
    Oid accessMethodId;
    Oid relationId;
    HeapTuple tuple;
    Form_pg_index indexForm;
    Form_pg_am accessMethodForm;
    bool amcanorder = false;
    int16* coloptions = NULL;
    IndexInfo* indexInfo = NULL;
    int numberOfAttributes;
    int old_natts;
    bool isnull = false;
    bool ret = true;
    oidvector* old_indclass = NULL;
    oidvector* old_indcollation = NULL;
    Relation irel;
    int i = 0;
    Datum d;

    /* Caller should already have the relation locked in some way. */
    relationId = IndexGetRelation(oldId, false);

    /*
     * We can pretend isconstraint = false unconditionally.  It only serves to
     * decide the text of an error message that should never happen for us.
     */
    isconstraint = false;

    numberOfAttributes = list_length(attributeList);
    if (numberOfAttributes <= 0 || numberOfAttributes > INDEX_MAX_KEYS) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("the number of attributes is illegal")));
    }

    /* look up the access method */
    tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("access method \"%s\" does not exist", accessMethodName)));
    accessMethodId = HeapTupleGetOid(tuple);
    accessMethodForm = (Form_pg_am)GETSTRUCT(tuple);
    amcanorder = accessMethodForm->amcanorder;
    ReleaseSysCache(tuple);

    /*
     * Compute the operator classes, collations, and exclusion operators for
     * the new index, so we can test whether it's compatible with the existing
     * one.  Note that ComputeIndexAttrs might fail here, but that's OK:
     * DefineIndex would have called this function with the same arguments
     * later on, and it would have failed then anyway. Our attributeList
     * contains only key attributes, thus we're filling ii_NumIndexAttrs and
     * ii_NumIndexKeyAttrs with same value.
     */
    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = numberOfAttributes;
    indexInfo->ii_NumIndexKeyAttrs = numberOfAttributes;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    typeObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    collationObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    classObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    coloptions = (int16*)palloc(numberOfAttributes * sizeof(int16));
    ComputeIndexAttrs(indexInfo,
        typeObjectId,
        collationObjectId,
        classObjectId,
        coloptions,
        attributeList,
        exclusionOpNames,
        relationId,
        accessMethodName,
        accessMethodId,
        amcanorder,
        isconstraint);

    /* Get the soon-obsolete pg_index tuple. */
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(oldId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", oldId)));
    indexForm = (Form_pg_index)GETSTRUCT(tuple);

    /*
     * We don't assess expressions or predicates; assume incompatibility.
     * Also, if the index is invalid for any reason, treat it as incompatible.
     */
    if (!(heap_attisnull(tuple, Anum_pg_index_indpred, NULL) && heap_attisnull(tuple, Anum_pg_index_indexprs, NULL) &&
            IndexIsValid(indexForm))) {
        ReleaseSysCache(tuple);
        return false;
    }

    /* Any change in operator class or collation breaks compatibility. */
    old_natts = GetIndexKeyAttsByTuple(NULL, tuple);
    Assert(old_natts == numberOfAttributes);

    d = SysCacheGetAttr(INDEXRELID, tuple, Anum_pg_index_indcollation, &isnull);
    Assert(!isnull);
    old_indcollation = (oidvector*)DatumGetPointer(d);

    d = SysCacheGetAttr(INDEXRELID, tuple, Anum_pg_index_indclass, &isnull);
    Assert(!isnull);
    old_indclass = (oidvector*)DatumGetPointer(d);

    ret = (memcmp(old_indclass->values, classObjectId, old_natts * sizeof(Oid)) == 0 &&
           memcmp(old_indcollation->values, collationObjectId, old_natts * sizeof(Oid)) == 0);

    ReleaseSysCache(tuple);

    if (!ret) {
        return false;
    }

    /* For polymorphic opcintype, column type changes break compatibility. */
    irel = index_open(oldId, AccessShareLock); /* caller probably has a lock */
    for (i = 0; i < old_natts; i++) {
        if (IsPolymorphicType(get_opclass_input_type(classObjectId[i])) &&
            irel->rd_att->attrs[i].atttypid != typeObjectId[i]) {
            ret = false;
            break;
        }
    }

    /* Any change in exclusion operator selections breaks compatibility. */
    if (ret && indexInfo->ii_ExclusionOps != NULL) {
        Oid* old_operators = NULL;
        Oid* old_procs = NULL;
        uint16* old_strats = NULL;

        RelationGetExclusionInfo(irel, &old_operators, &old_procs, &old_strats);
        ret = memcmp(old_operators, indexInfo->ii_ExclusionOps, old_natts * sizeof(Oid)) == 0;

        /* Require an exact input type match for polymorphic operators. */
        if (ret) {
            for (i = 0; i < old_natts && ret; i++) {
                Oid left, right;

                op_input_types(indexInfo->ii_ExclusionOps[i], &left, &right);
                if ((IsPolymorphicType(left) || IsPolymorphicType(right)) &&
                    irel->rd_att->attrs[i].atttypid != typeObjectId[i]) {
                    ret = false;
                    break;
                }
            }
        }
    }

    index_close(irel, NoLock);
    return ret;
}

static void CheckPartitionUniqueKey(Relation rel, int2vector *partKey, IndexStmt *stmt, int numberOfAttributes)
{
    int j;

    if (partKey->dim1 > numberOfAttributes) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("unique local index columns must contain all the partition keys")));
    }

    for (j = 0; j < partKey->dim1; j++) {
        int2 attNum = partKey->values[j];
        Form_pg_attribute att_tup = &rel->rd_att->attrs[attNum - 1];

        if (!columnIsExist(rel, att_tup, stmt->indexParams)) {
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("unique local index columns must contain all the partition keys and collation must be default "
                        "collation")));
        }
    }
}

static void CheckPartitionIndexDef(IndexStmt* stmt, List *partitionidlist)
{
    List *partitionIndexdef = (List*)stmt->partClause;

    int partitionLens = list_length(partitionidlist);
    int idfLens = list_length(partitionIndexdef);

    if (partitionLens > idfLens) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Not enough index partition defined")));
    } else if (partitionLens < idfLens) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("number of partitions of LOCAL index must equal that of the "
                       "underlying table")));
    }

    return;
}

/*
 * Extract SubPartitionIdfs when CREATE INDEX with subpartitions.
 */
static List *ExtractSubPartitionIdf(IndexStmt* stmt, List *subPartitionOidList, List *partitionIndexdef)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    int subpartitionLens = 0;
    int expectedSubLens = 0;
    List *subPartitionIdf = NIL;

    partitionIndexdef = (List*)stmt->partClause;
    List *backupIdxdef = (List *)copyObject(partitionIndexdef);
    int partitionLen = list_length(partitionIndexdef);

    /* Fast check partition length */
    if (partitionLen != list_length(subPartitionOidList)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Wrong number of partitions when create index specify subpartition.")));
    }

    /* if part contain tablespace, and subpart has no tablespace definition, set subpart tablespace */
    foreach(lc1, partitionIndexdef) {
        RangePartitionindexDefState *idxPartdef = (RangePartitionindexDefState*)lfirst(lc1);
        if (idxPartdef->tablespace == NULL) {
            continue;
        }
 
        foreach(lc2, idxPartdef->sublist) {
            RangePartitionindexDefState *idxSubPartdef = (RangePartitionindexDefState*)lfirst(lc2);
            if (idxSubPartdef->tablespace == NULL) {
                idxSubPartdef->tablespace = pstrdup(idxPartdef->tablespace);
            }
        }
    }

    /* Next check specify subpartition with metadata in pg_partition */
    foreach(lc1, subPartitionOidList) {
        List *subPartOids = (List *)lfirst(lc1);
        int subLens = list_length(subPartOids);

        foreach(lc2, partitionIndexdef) {
            RangePartitionindexDefState *idxDef = (RangePartitionindexDefState*)lfirst(lc2);
            int idfLens = list_length(idxDef->sublist);

            if (subLens == idfLens) {
                subPartitionIdf = lappend(subPartitionIdf, copyObject(idxDef->sublist));
                partitionIndexdef = list_delete(partitionIndexdef, lfirst(lc2));
                break;
            }
        }

        expectedSubLens += list_length(subPartOids);
    }

    /* Fail exactly match if partitionIndexdef */
    if (partitionIndexdef != NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Cannot match subpartitions when create subpartition indexes.")));
    }

    /* Count sum of subpartitions */
    foreach(lc1, backupIdxdef) {
        RangePartitionindexDefState *def = (RangePartitionindexDefState*)lfirst(lc1);
        subpartitionLens += list_length(def->sublist);
    }

    /* Check total subpartition number */
    if (subpartitionLens != expectedSubLens) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Wrong number of subpartitions when create index specify subpartition.")));
    }

    list_free_ext(backupIdxdef);
    return subPartitionIdf;
}

static bool CheckIndexIncludingParams(IndexStmt* stmt)
{
    if (!PointerIsValid(stmt)) {
        return false;
    }

    int nparams = list_length(stmt->indexIncludingParams);
    ListCell* cell = NULL;
    /* For global partition index, tableoid will be added automatically in AddIndexColumnForGpi, so remove the count */
    if (stmt->isGlobal) {
        foreach (cell, stmt->indexIncludingParams) {
            IndexElem* param = (IndexElem*)lfirst(cell);
            if (strcmp(param->name, "tableoid") == 0) {
                nparams--;
                break;
            }
        }
    }
    /* For crossbucket index, tablebucketid will be added automatically in AddIndexColumnForCbi, so remove the count */
    if (stmt->crossbucket) {
        foreach (cell, stmt->indexIncludingParams) {
            IndexElem* param = (IndexElem*)lfirst(cell);
            if (strcmp(param->name, "tablebucketid") == 0) {
                nparams--;
                break;
            }
        }
    }

    return (nparams > 0);
}

static bool CheckLedgerIndex_walker(Node* node, int* context)
{
    if (IsA(node, Var)) {
        Var *var = (Var*)node;
        if (var->varattno == *context) {
            return true;
        }
    }
    if (IsA(node, IndexElem)) {
        IndexElem *elem = (IndexElem *)node;
        if (elem->name && strcmp(elem->name, "hash") == 0) {
            return true;
        }
        if (elem->expr) {
            return CheckLedgerIndex_walker(elem->expr, context);
        }

        return false;
    }
    return expression_tree_walker(node, (bool (*)())CheckLedgerIndex_walker, context);
}

/* index expression of ledger user table is not support "hash" column */
static bool CheckLedgerIndex(Relation rel, Node *node)
{
    int hash_attrno = user_hash_attrno(rel->rd_att);

    hash_attrno = hash_attrno + 1;  /* in Var, attrno start at 1 */

    return CheckLedgerIndex_walker(node, &hash_attrno);
}

void SetPartionIndexType(IndexStmt* stmt, Relation rel, bool is_alter_table)
{
    if (!RELATION_IS_PARTITIONED(rel)) {
        return;
    }

    if (stmt->unique && (!stmt->isPartitioned || is_alter_table)) {
        /*
         * If index key of unique or primary key index include the partition key,
         * default index is set to local index. Otherwise, set to global index.
         */
        if (RelationIsSubPartitioned(rel)) {
            List *partOidList = relationGetPartitionOidList(rel);
            Assert(list_length(partOidList) != 0);
            Partition part = partitionOpen(rel, linitial_oid(partOidList), NoLock);
            Relation partRel = partitionGetRelation(rel, part);
            stmt->isGlobal = !(CheckIdxParamsOwnPartKey(rel, stmt->indexParams) &&
                                CheckIdxParamsOwnPartKey(partRel, stmt->indexParams));
            releaseDummyRelation(&partRel);
            partitionClose(rel, part, NoLock);
            if (partOidList != NULL) {
                releasePartitionOidList(&partOidList);
            }
        } else {
            stmt->isGlobal = !CheckIdxParamsOwnPartKey(rel, stmt->indexParams);
        }
    } else if (!stmt->isPartitioned) {
        /* default partition index is set to Global index */
        stmt->isGlobal = (!DEFAULT_CREATE_LOCAL_INDEX ? true : stmt->isGlobal);
    }
    stmt->isPartitioned = true;
#ifndef ENABLE_MULTIPLE_NODES
    if (stmt->unique && stmt->isPartitioned && RelationIsCUFormat(rel)) {
        /* CStore unique index must include the partition key and set to local index */
        stmt->isGlobal = false;
    }
#endif
}

/*
 * WaitForOlderSnapshots
 *
 * Wait for transactions that might have an older snapshot than the given xmin
 * limit, because it might not contain tuples deleted just before it has
 * been taken. Obtain a list of VXIDs of such transactions, and wait for them 
 * individually. This is used when building an index concurrently.
 * 
 * We can exclude any running transactions that have xmin> the xmin given;
 * their oldest snapshot must be newer than our xmin limit.
 * We can also exclude any transactions that have xmin = zero, since they
 * evidently have no live snapshot at all (and any one they might be in
 * process of taking is certainly newer than ours). Transactions in other
 * DBs can be ignored too, since they'll never even be able to see the
 * index being worked on.
 * 
 * We can also exclude autovacuum processes and processes running manual
 * lazy VACUUMs, because they won't be fazed by missing index entries
 * either. (Manual ANALYZEs, however, can't be excluded because they
 * might be within transactions that are going to do arbitrary operations
 * later.)
 * 
 * Also, GetCurrentVirtualXIDs never reports our own vxid, so we need not
 * check for that.
 * 
 * If a process goes idle-in-transaction with xmin zero, we do not need to
 * wait for it anymore, per the above argument. We do not have the 
 * infrastructure right now to stop waiting if that happens, but we can at
 * begin to wait. We do this by repeatedly rechecking the output of
 * GetCurrentVirtualXIDs. If, during any iteration, a particular vxid
 * doesn't show up in the output, we know we can forget about it.
 */
static void WaitForOlderSnapshots(TransactionId limitXmin)
{
    int n_old_snapshots;
    int i;
    VirtualTransactionId* old_snapshots;

    old_snapshots = GetCurrentVirtualXIDs(limitXmin, true, false, PROC_IS_AUTOVACUUM | PROC_IN_VACUUM, &n_old_snapshots);

    for (i = 0; i < n_old_snapshots; i++){
        if (!VirtualTransactionIdIsValid(old_snapshots[i]))
            continue;/* found uninteresting in previous cycle */
        
        if (i > 0) {
            /* see if anything's changed ... */
            VirtualTransactionId* newer_snapshots = NULL;
            int n_newer_snapshots;
            int j;
            int k;

            newer_snapshots = GetCurrentVirtualXIDs(
                limitXmin, true, false, PROC_IS_AUTOVACUUM | PROC_IN_VACUUM, &n_newer_snapshots);
            for (j = i; j < n_old_snapshots; j++){
                if (!VirtualTransactionIdIsValid(old_snapshots[j]))
                    continue; /* found uninteresting in prevous cycle */
                for (k = 0; k < n_newer_snapshots; k++){
                    if(VirtualTransactionIdEquals(old_snapshots[j], newer_snapshots[k]))
                        break;
                } 
                if (k >= n_newer_snapshots) /* not there anymore */
                    SetInvalidVirtualTransactionId(old_snapshots[j]);
            }
            pfree_ext(newer_snapshots);
        }

        if(VirtualTransactionIdIsValid(old_snapshots[i]))
            (void)VirtualXactLock(old_snapshots[i], true);
    }
}

inline bool get_rel_segment(Relation rel)
{
    if (rel == NULL || rel->rd_options == NULL) {
        return false;
    }

    StdRdOptions *opt = (StdRdOptions*)(rel->rd_options);
    return opt->segment;
}

static bool parseVisibleStateFromOptions(List* options)
{
    ListCell *cell = NULL;
    bool visible = true;
    /* Visible state may be indicated more than once, return the last one */
    foreach (cell, options) {
        void *pointer = lfirst(cell);
        if (IsA(pointer, String)) {
            Value *val = (Value *)pointer;
            if (strcmp(val->val.str, "visible") == 0) {
                visible = true;
            } else if (strcmp(val->val.str, "invisible") == 0) {
                visible = false;
            }
        }
    }
    return visible;
}

/*
 * DefineIndex
 *		Creates a new index.
 *
 *relationId: table oid for current index defination
 * 'stmt': IndexStmt describing the properties of the new index.
 * 'indexRelationId': normally InvalidOid, but during bootstrap can be
 *		nonzero to specify a preselected OID for the index.
 * 'is_alter_table': this is due to an ALTER rather than a CREATE operation.
 * 'check_rights': check for CREATE rights in the namespace.  (This should
 *		be true except when ALTER is deleting/recreating an index.)
 * 'skip_build': make the catalog entries but leave the index file empty;
 *		it will be filled later.
 * 'quiet': suppress the NOTICE chatter ordinarily provided for constraints.
 *
 * Returns the object address of the created index.
 */
ObjectAddress DefineIndex(Oid relationId, IndexStmt* stmt, Oid indexRelationId, bool is_alter_table, bool check_rights,
    bool skip_build, bool quiet, bool is_modify_primary)
{
    char* indexRelationName = NULL;
    char* accessMethodName = NULL;
    Oid* typeObjectId = NULL;
    Oid* collationObjectId = NULL;
    Oid* classObjectId = NULL;
    Oid accessMethodId = InvalidOid;
    Oid namespaceId = InvalidOid;
    Oid tablespaceId = InvalidOid;
    //Oid relfilenode = InvalidOid;
    bool dfsTablespace = false;
    List* indexColNames = NIL;
    List* allIndexParams = NIL;
    //List *filenodeList = NIL;
    Relation rel;
    HeapTuple tuple;
    Form_pg_am accessMethodForm;
    bool amcanorder = false;
    RegProcedure amoptions;
    Datum reloptions;
    int16* coloptions = NULL;
    IndexInfo* indexInfo = NULL;
    int numberOfAttributes = 0;
    int numberOfKeyAttributes;
    TransactionId limitXmin;
    VirtualTransactionId* old_lockholders = NULL;
    LockRelId heaprelid;
    LOCKTAG heaplocktag;
    LOCKMODE lockmode;
    Snapshot snapshot;
    int i = 0;
    List* partitionIndexdef = NIL;
    List* partitiontspList = NIL;
    char relPersistence;
    bool concurrent;
    bool has_dedup_opt = false;
    ObjectAddress address;
    StdRdOptions* index_relopts;
    int8 indexsplitMethod = INDEXSPLIT_NO_DEFAULT;
    int crossbucketopt = -1;
    List *subPartTspList = NULL;
    List *subPartitionIndexDef = NULL;
    List *subPartitionOidList = NULL;
    List *partitionOidList = NULL;
    Oid root_save_userid;
    int root_save_sec_context;
    int root_save_nestlevel;

    root_save_nestlevel = NewGUCNestLevel();

    /*
     * Force non-concurrent build on temporary relations, even if CONCURRENTLY
     * was requested.  Other backends can't access a temporary relation, so
     * there's no harm in grabbing a stronger lock, and a non-concurrent DROP
     * is more efficient.  Do this before any use of the concurrent option is
     * done.
     */
    relPersistence = get_rel_persistence(relationId);
    if (stmt->concurrent && !(relPersistence == RELPERSISTENCE_TEMP
                            || relPersistence == RELPERSISTENCE_GLOBAL_TEMP)) {
        concurrent = true;
    } else {
        concurrent = false;
    }

    /* Don't suppport gin/gist index on global temporary table */
    if (relPersistence == RELPERSISTENCE_GLOBAL_TEMP &&
       (strcmp(stmt->accessMethod, "gin") == 0 || strcmp(stmt->accessMethod, "gist") == 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("access method \"%s\" is not support for global temporary table index", stmt->accessMethod)));
    }

    /*
     * Open heap relation, acquire a suitable lock on it, remember its OID
     *
     * Only SELECT ... FOR UPDATE/SHARE are allowed while doing a standard
     * index build; but for concurrent builds we allow INSERT/UPDATE/DELETE
     * (but not VACUUM).
     *
     * NB: Caller is responsible for making sure that relationId refers
     * to the relation on which the index should be built; except in bootstrap
     * mode, this will typically require the caller to have already locked
     * the relation.  To avoid lock upgrade hazards, that lock should be at
     * least as strong as the one we take here.
     */
    lockmode = concurrent ? ShareUpdateExclusiveLock : ShareLock;
    rel = heap_open(relationId, lockmode);

    bool segment = get_rel_segment(rel);
    TableCreateSupport indexCreateSupport{(int)COMPRESS_TYPE_NONE, false, false, false, false, false, true, false};
    ListCell *cell = NULL;
    foreach (cell, stmt->options) {
        DefElem *defElem = (DefElem *)lfirst(cell);
        SetOneOfCompressOption(defElem, &indexCreateSupport);

        if (pg_strcasecmp(defElem->defname, "deduplication") == 0){
            has_dedup_opt = true;
        }
    }

    CheckCompressOption(&indexCreateSupport);
    /* do not suppport to create compressed index for temp table. */
    if ((indexCreateSupport.compressType != (int)COMPRESS_TYPE_NONE) &&
        (relPersistence == RELPERSISTENCE_TEMP || relPersistence == RELPERSISTENCE_GLOBAL_TEMP ||
         relPersistence == RELPERSISTENCE_UNLOGGED || segment)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("compressed index \"%s\" is not supported for temporary table, unlogged table and segment table,"
            " please use uncompressed one instead", stmt->idxname)));
    }

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations.  We
     * already arranged to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&root_save_userid, &root_save_sec_context);
    SetUserIdAndSecContext(rel->rd_rel->relowner,
        root_save_sec_context | SECURITY_RESTRICTED_OPERATION);
    /* Forbidden to create gin index on ustore table. */
    if (rel->rd_tam_ops == TableAmUstore) {
        if (strcmp(stmt->accessMethod, "btree") == 0) {
            elog(ERROR, "btree index is not supported for ustore, please use ubtree instead");
        }
        if (strcmp(stmt->accessMethod, "ubtree") != 0) {
            elog(ERROR, "%s index is not supported for ustore", (stmt->accessMethod));
        }
        if (has_dedup_opt) {
            elog(ERROR, "Index deduplication is not supported for ustore.");
        }
        if (stmt->deferrable == true) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("Ustore table does not support to set deferrable."),
                    errdetail("N/A"),
                    errcause("feature not supported"),
                    erraction("check constraints of columns")));
        }
    }

    if (strcmp(stmt->accessMethod, "ubtree") == 0 &&
        rel->rd_tam_ops != TableAmUstore) {
        elog(ERROR, "ubtree index is only supported for ustore");
    }

    relationId = RelationGetRelid(rel);
    namespaceId = RelationGetNamespace(rel);

    if (stmt->missing_ok && OidIsValid(get_relname_relid(stmt->idxname, namespaceId))) {
        ereport(NOTICE,
            (errcode(ERRCODE_DUPLICATE_TABLE),
                errmsg("relation \"%s\" already exists, skipping", stmt->idxname)));
        heap_close(rel, NoLock);
        return address;
    }

    if (stmt->schemaname != NULL) {
        if (namespaceId != get_namespace_oid(stmt->schemaname, false)) {
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("index and table must be in the same schema")));
        }
    }

    if (CheckWhetherForbiddenFunctionalIdx(relationId, namespaceId, stmt->indexParams)) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("not supported to create a functional index on this table.")));
    }

    if (rel->rd_isblockchain && CheckLedgerIndex(rel, (Node*)stmt->indexParams)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("index of ledger user talbe can not contain \"hash\" column.")));
    }

    SetPartionIndexType(stmt, rel, is_alter_table);

    if (stmt->isGlobal && DISABLE_MULTI_NODES_GPI) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Global partition index only support single node mode.")));
    }
    if (stmt->isGlobal && t_thrd.proc->workingVersionNum < SUPPORT_GPI_VERSION_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg(
                    "global partition index not support on work version num less than %u.", SUPPORT_GPI_VERSION_NUM)));
    }

    if (RELATION_HAS_BUCKET(rel)) {
        /* determine the crossbucket option */
        stmt->crossbucket = get_crossbucket_option(&stmt->options, stmt->isGlobal, stmt->accessMethod, &crossbucketopt);
        if (concurrent) {
            ereport(ERROR, (errmodule(MOD_INDEX), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot CREATE INDEX CONCURRENTLY on hash bucket table."),
                errdetail("CREATE INDEX CONCURRENTLY on hash bucket table is not supported on the current version."),
                errcause("CREATE INDEX CONCURRENTLY on hash bucket table on the current version."),
                erraction("Do not CREATE INDEX CONCURRENTLY on hash bucket table on the current version.")));
        }

    }

    if (stmt->crossbucket) {
        if (stmt->whereClause != NULL) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cross-bucket index does not support WHERE clause")));
        }

        if (pg_strcasecmp(stmt->accessMethod, DEFAULT_INDEX_TYPE) != 0 && (crossbucketopt > 0)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cross-bucket index only supports btree access method")));
        }

        if (concurrent) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot create CONCURRENTLY cross-bucket index")));
        }
    }

    /*
     * normal table does not support local partitioned index
     */
    if (stmt->isPartitioned) {
        if (RelationIsValuePartitioned(rel)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("dfs value partition table does not support local partitioned indexes ")));
        } else if (!RELATION_IS_PARTITIONED(rel)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("non-partitioned table does not support %s partitioned indexes ",
                        stmt->isGlobal ? "global" : "local")));
#ifdef ENABLE_MULTIPLE_NODES
        } else if (stmt->deferrable && stmt->unique) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Partition table does not support to set deferrable.")));
#endif
        } else if (stmt->isGlobal && stmt->whereClause != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Global partition index does not support WHERE clause.")));
        } else if (has_dedup_opt) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Index deduplication is not supported for partition table.")));
        }
    }

    /*
     * partitioned index not is not support concurrent index
     */
    if (stmt->isPartitioned && concurrent) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot create concurrent partitioned indexes ")));
    }

    CheckIndexParamsNumber(stmt);

    /* Add special index columns tableoid to global partition index. */
    if (stmt->isGlobal) {
        AddIndexColumnForGpi(stmt);
    }

    /* Add special index columns tablebucketid to crossbucket index. */
    if (stmt->crossbucket) {
        AddIndexColumnForCbi(stmt);
    }

    if (strcmp(stmt->accessMethod, "ubtree") != 0 && CheckIndexIncludingParams(stmt)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("create a index with include columns is only supported in ubtree")));
    }

    if (list_intersection(stmt->indexParams, stmt->indexIncludingParams) != NIL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("included columns must not intersect with key columns")));
    }

    /*
     * count key attributes in index
     */
    numberOfKeyAttributes = list_length(stmt->indexParams);

    /*
     * Calculate the new list of index columns including both key columns and
     * INCLUDE columns.  Later we can determine which of these are key columns,
     * and which are just part of the INCLUDE list by checking the list
     * position.  A list item in a position less than ii_NumIndexKeyAttrs is
     * part of the key columns, and anything equal to and over is part of the
     * INCLUDE columns.
     */
    allIndexParams = list_concat(list_copy(stmt->indexParams), list_copy(stmt->indexIncludingParams));
    /*
     * count attributes in index
     */
    numberOfAttributes = list_length(allIndexParams);
    if (numberOfAttributes <= 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("must specify at least one column")));
    }

    if (numberOfAttributes > INDEX_MAX_KEYS) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("cannot use more than %d columns in an index", INDEX_MAX_KEYS)));
    }

    indexRelationName = stmt->idxname;

    handleErrMsgForInfoCnstrnt(stmt, rel);

    /*
     * Don't try to CREATE INDEX on temp tables of other backends.
     */
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot create indexes on temporary tables of other sessions")));

    /*
     * Verify we (still) have CREATE rights in the rel's namespace.
     * (Presumably we did when the rel was created, but maybe not anymore.)
     * Skip check if caller doesn't want it.  Also skip check if
     * bootstrapping, since permissions machinery may not be working yet.
     */
    if (check_rights && !IsBootstrapProcessingMode()) {
        (void)CheckCreatePrivilegeInNamespace(namespaceId, root_save_userid, CREATE_ANY_INDEX);
    }

    /*
     * Select tablespace to use.  If not specified, use default tablespace
     * (which may in turn default to database's default).
     */
    if (stmt->tableSpace) {
        tablespaceId = get_tablespace_oid(stmt->tableSpace, false);
    } else {
        tablespaceId = GetDefaultTablespace(rel->rd_rel->relpersistence);
        /* note InvalidOid is OK in this case */
    }

    dfsTablespace = IsSpecifiedTblspc(tablespaceId, FILESYSTEM_HDFS);
    if (dfsTablespace) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("It is not supported to create index on DFS tablespace.")));
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        partitionOidList = relationGetPartitionOidList(rel);
    }
    if (RelationIsSubPartitioned(rel)) {
        subPartitionOidList = RelationGetSubPartitionOidListList(rel);
    }

    /* Check permissions except when using database's default */
    if (stmt->isPartitioned && !stmt->isGlobal) {
        /* LOCAL partition index check */
        ListCell* cell = NULL;

        if (PointerIsValid(stmt->partClause)) {
            if (RelationIsSubPartitioned(rel)) {
                partitionIndexdef = (List*)stmt->partClause;
                subPartitionIndexDef = ExtractSubPartitionIdf(stmt, subPartitionOidList, partitionIndexdef);
            } else {
                partitionIndexdef = (List*)stmt->partClause;

                /* index partition's number must no less than table partition's number */
                CheckPartitionIndexDef(stmt, partitionOidList);
            }
        } else {
            if (!RelationIsSubPartitioned(rel)) {
                /* construct the index list */
                for (i = 0; i < list_length(partitionOidList); i++) {
                    RangePartitionindexDefState* def = makeNode(RangePartitionindexDefState);
                    partitionIndexdef = lappend(partitionIndexdef, def);
                }
            } else {
                /* construct the index list */
                foreach (cell, subPartitionOidList) {
                    List *subPartOids = (List *)lfirst(cell);
                    List *partSubIndexDef = NULL;
                    for (i = 0; i < list_length(subPartOids); i++) {
                        RangePartitionindexDefState *def = makeNode(RangePartitionindexDefState);
                        partSubIndexDef = lappend(partSubIndexDef, def);
                    }

                    subPartitionIndexDef = lappend(subPartitionIndexDef, partSubIndexDef);
                }
            }
        }

        if (!RelationIsSubPartitioned(rel)) {
            foreach (cell, partitionIndexdef) {
                RangePartitionindexDefState* def = (RangePartitionindexDefState*)lfirst(cell);

                if (NULL != def->tablespace) {
                    /* use partition tablespace if user defines */
                    partitiontspList = lappend_oid(partitiontspList, get_tablespace_oid(def->tablespace, false));
                } else {
                    /* use partitioned table' tablespace if user doesnt define */
                    partitiontspList = lappend_oid(partitiontspList, tablespaceId);
                }
            }
        } else {
            foreach(cell, subPartitionIndexDef) {
                List *sub = (List *)lfirst(cell);
                ListCell *subcell;
                partitiontspList = NIL;
                foreach (subcell, sub) {
                    RangePartitionindexDefState* def = (RangePartitionindexDefState*)lfirst(subcell);
                    if (NULL != def->tablespace) {
                        /* use partition tablespace if user defines */
                        partitiontspList = lappend_oid(partitiontspList, get_tablespace_oid(def->tablespace, false));
                    } else {
                        /* use partitioned table' tablespace if user doesnt define */
                        partitiontspList = lappend_oid(partitiontspList, tablespaceId);
                    }
                }

                subPartTspList = lappend(subPartTspList, partitiontspList);
            }
        }
    }

    /*
     * partitioned index need check every index partition tablespace
     */
    if (!stmt->isPartitioned && OidIsValid(tablespaceId) && tablespaceId != u_sess->proc_cxt.MyDatabaseTableSpace) {
        AclResult aclresult;

        aclresult = pg_tablespace_aclcheck(tablespaceId, root_save_userid, ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(tablespaceId));
    } else if (stmt->isPartitioned) {
        /* check the partition index tablepsace acl */
        Oid tablespaceOid;
        AclResult aclresult;
        ListCell* tspcell = NULL;

        if (!stmt->isGlobal) {  // LOCAL partition index check
            foreach (tspcell, partitiontspList) {
                tablespaceOid = lfirst_oid(tspcell);
                if (OidIsValid(tablespaceOid) && tablespaceOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
                    aclresult = pg_tablespace_aclcheck(tablespaceOid, root_save_userid, ACL_CREATE);
                    if (aclresult != ACLCHECK_OK) {
                        aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(tablespaceOid));
                    }
                }
                /* In all cases disallow placing user relations in pg_global */
                if (tablespaceOid == GLOBALTABLESPACE_OID) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("only shared relations can be placed in pg_global tablespace")));
                }
            }
        } else {
            if (OidIsValid(tablespaceId) && tablespaceId != u_sess->proc_cxt.MyDatabaseTableSpace) {
                aclresult = pg_tablespace_aclcheck(tablespaceId, root_save_userid, ACL_CREATE);
                if (aclresult != ACLCHECK_OK) {
                    aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(tablespaceId));
                }
            }
        }

        /*
         * Check unique , if it is a unique/exclusion index,
         * index column must include the partition key.
         * For global partition index, we cancel this check.
         */
        if (stmt->unique && !stmt->isGlobal) {
            int2vector* partKey = GetPartitionKey(rel->partMap);
            CheckPartitionUniqueKey(rel, partKey, stmt, numberOfAttributes);
            if (RelationIsSubPartitioned(rel)) {
                List *partOidList = relationGetPartitionOidList(rel);
                Assert(list_length(partOidList) != 0);
                Partition part = partitionOpen(rel, linitial_oid(partOidList), NoLock);
                Relation partRel = partitionGetRelation(rel, part);
                int2vector* subPartKey = GetPartitionKey(partRel->partMap);
                CheckPartitionUniqueKey(partRel, subPartKey, stmt, numberOfAttributes);
                releaseDummyRelation(&partRel);
                partitionClose(rel, part, NoLock);
            }
        }
    }

    /*
     * Force shared indexes into the pg_global tablespace.	This is a bit of a
     * hack but seems simpler than marking them in the BKI commands.  On the
     * other hand, if it's not shared, don't allow it to be placed there.
     */
    if (rel->rd_rel->relisshared)
        tablespaceId = GLOBALTABLESPACE_OID;
    else if (tablespaceId == GLOBALTABLESPACE_OID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("only shared relations can be placed in pg_global tablespace")));

    /*
     * Choose the index column names.
     */
    indexColNames = ChooseIndexColumnNames(allIndexParams);

    /*
     * Select name for index if caller didn't specify.
     */
    indexRelationName = stmt->idxname;
    if (indexRelationName == NULL) {
        indexRelationName = ChooseIndexName(RelationGetRelationName(rel),
            namespaceId,
            indexColNames,
            stmt->excludeOpNames,
            stmt->primary,
            stmt->isconstraint);

        /* Build an not exists constraint name for informational. */
        buildConstraintNameForInfoCnstrnt(stmt, rel, &indexRelationName, namespaceId, indexColNames);
    }

    /*
     * look up the access method, verify it can handle the requested features
     */
    accessMethodName = stmt->accessMethod;
    if (RelationIsUstoreFormat(rel) &&
        (strcmp(accessMethodName, "gist") == 0 || strcmp(accessMethodName, "gin") == 0)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("access method \"%s\" is not supported in ustore", accessMethodName)));
    }
    tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
    if (!HeapTupleIsValid(tuple)) {
        /*
         * Hack to provide more-or-less-transparent updating of old RTREE
         * indexes to GiST: if RTREE is requested and not found, use GIST.
         */
        if (strcmp(accessMethodName, "rtree") == 0) {
            ereport(NOTICE, (errmsg("substituting access method \"gist\" for obsolete method \"rtree\"")));
            accessMethodName = "gist";
            tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
            if (!HeapTupleIsValid(tuple))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("access method \"%s\" does not exist", accessMethodName)));
        }
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("access method \"%s\" does not exist", accessMethodName)));
    }
    accessMethodId = HeapTupleGetOid(tuple);
    accessMethodForm = (Form_pg_am)GETSTRUCT(tuple);
    if (stmt->unique &&
#ifndef ENABLE_MULTIPLE_NODES
    !accessMethodForm->amcanunique)
#else
    (!accessMethodForm->amcanunique || accessMethodId == CBTREE_AM_OID))
#endif
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("access method \"%s\" does not support unique indexes", accessMethodName)));

    if (numberOfAttributes > 1 && !accessMethodForm->amcanmulticol)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("access method \"%s\" does not support multicolumn indexes", accessMethodName)));
    if (stmt->excludeOpNames && !OidIsValid(accessMethodForm->amgettuple))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("access method \"%s\" does not support exclusion constraints", accessMethodName)));

    amcanorder = accessMethodForm->amcanorder;
    amoptions = accessMethodForm->amoptions;

    ReleaseSysCache(tuple);

    /*
     * Validate predicate, if given
     */
    if (stmt->whereClause)
        CheckPredicate((Expr*)stmt->whereClause);

    if (RelationIsUstoreFormat(rel)) {
        DefElem* def = makeDefElem("storage_type", (Node*)makeString(TABLE_ACCESS_METHOD_USTORE));
        if (stmt->options == NULL) {
            stmt->options = list_make1(def);
        } else {
            ListCell* cell = NULL;
            bool optionsHasStorage = false;

            foreach (cell, stmt->options) {
                DefElem* defElem = (DefElem*)lfirst(cell);
                if (pg_strcasecmp(defElem->defname, "storage_type") == 0) {
                    optionsHasStorage = true;
                    break;
                }
            }

            if (!optionsHasStorage)
                stmt->options = lappend(stmt->options, def);
        }
    }

    if (indexCreateSupport.compressType || HasCompressOption(&indexCreateSupport)) {
        ListCell* cell = NULL;
        foreach (cell, stmt->options) {
            DefElem *defElem = (DefElem *)lfirst(cell);
            if (pg_strcasecmp(defElem->defname, "segment") == 0 && ReadBoolFromDefElem(defElem)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Can not use compress option in segment storage.")));
            }
        }
    }
    /*
     * Parse AM-specific options, convert to text array form, validate.
     */
    reloptions = transformRelOptions((Datum)0, stmt->options, NULL, NULL, false, false);

    index_relopts = (StdRdOptions *)index_reloptions(amoptions, reloptions, true);
    if (index_relopts != NULL && strcmp(accessMethodName, "btree") == 0) {
        /* check if the table supports to create crossbucket index */
        if (is_contain_crossbucket(stmt->options) && !RELATION_HAS_BUCKET(rel) && !skip_build) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Could not create cross-bucket index for non-hashbucket table.")));
        }
        /* Set the page split method */
        if (RelationIsIndexsplitMethodInsertpt(index_relopts)) {
            indexsplitMethod = INDEXSPLIT_NO_INSERTPT;
        }
        pfree_ext(index_relopts);
    }

    /*
     * Prepare arguments for index_create, primarily an IndexInfo structure.
     * Note that ii_Predicate must be in implicit-AND format.
     */
    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = numberOfAttributes;
    indexInfo->ii_NumIndexKeyAttrs = numberOfKeyAttributes;
    indexInfo->ii_Expressions = NIL; /* for now */
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = make_ands_implicit((Expr*)stmt->whereClause);
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = stmt->unique;
    /* In a concurrent build, mark it not-ready-for-inserts */
    indexInfo->ii_ReadyForInserts = !concurrent;
    indexInfo->ii_Concurrent = concurrent;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_PgClassAttrId = 0;
    indexInfo->ii_ParallelWorkers = 0;

    typeObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    collationObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    classObjectId = (Oid*)palloc(numberOfAttributes * sizeof(Oid));
    coloptions = (int16*)palloc(numberOfAttributes * sizeof(int16));
    ComputeIndexAttrs(indexInfo,
        typeObjectId,
        collationObjectId,
        classObjectId,
        coloptions,
        allIndexParams,
        stmt->excludeOpNames,
        relationId,
        accessMethodName,
        accessMethodId,
        amcanorder,
        stmt->isconstraint);

    if (stmt->isPartitioned) {
        if (PointerIsValid(indexInfo->ii_ExclusionOps)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Partitioned table does not support EXCLUDE index")));
        }
        if (stmt->isGlobal && PointerIsValid(indexInfo->ii_Expressions)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Global partition index does not support EXPRESSION index")));
        }
        if (!CheckGlobalIndexCompatible(relationId, stmt->isGlobal, indexInfo, accessMethodId)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Global and local partition index should not be on same column")));
        }
    }

    if (stmt->crossbucket) {
        if (PointerIsValid(indexInfo->ii_ExclusionOps)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cross-bucket index does not support EXCLUDE constraints")));
        }
        if (PointerIsValid(indexInfo->ii_Expressions)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cross-bucket index does not support EXPRESSION")));
        }
    }

#ifdef PGXC
    /* Check if index is safely shippable */
    if (IS_PGXC_COORDINATOR) {
        List* indexAttrs = NIL;

        /* Prepare call for shippability evaluation */
        for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
            /*
             * Expression attributes are set at 0, and do not make sense
             * when comparing them to distribution columns, so bypass.
             */
            if (indexInfo->ii_KeyAttrNumbers[i] > 0)
                indexAttrs = lappend_int(indexAttrs, indexInfo->ii_KeyAttrNumbers[i]);
        }

        /* Finalize check */
        if (!pgxc_check_index_shippability(GetRelationLocInfo(relationId),
                stmt->primary,
                stmt->unique,
                stmt->excludeOpNames != NULL,
                indexAttrs,
                indexInfo->ii_Expressions))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Cannot create index whose evaluation cannot be "
                           "enforced to remote nodes")));
    }
#endif

#ifndef ENABLE_MULTIPLE_NODES
    if (RelationIsCUFormat(rel) && (stmt->primary || stmt->unique)) {
        /* If it is a unique index, move data on delta to CU. */
        MoveDeltaDataToCU(rel);
    }
#endif

    /*
     * Extra checks when creating a PRIMARY KEY index.
     * If be informational constraint, we are not to set not null in pg_attribute.
     */
    if (stmt->primary && !stmt->internal_flag)
        index_check_primary_key(rel, indexInfo, is_alter_table, stmt, is_modify_primary);

    /*
     * Report index creation if appropriate (delay this till after most of the
     * error checks)
     */
    if (stmt->isconstraint && !quiet) {
        const char* constraint_type = NULL;

        if (stmt->primary)
            constraint_type = "PRIMARY KEY";
        else if (stmt->unique)
            constraint_type = "UNIQUE";
        else if (stmt->excludeOpNames != NIL)
            constraint_type = "EXCLUDE";
        else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("unknown constraint type")));
            constraint_type = NULL; /* keep compiler quiet */
        }

        /*
         * @hdfs
         * Make a speciallized NOTICE msg for infotmatioanl constraint.
         */
        if (stmt->internal_flag) {
            ereport(NOTICE,
                (errmsg("%s %s will create constraint \"%s\" for foreign table \"%s\"",
                    is_alter_table ? "ALTER FOREIGN TABLE / ADD" : "CREATE FOREIGN TABLE /",
                    constraint_type,
                    indexRelationName,
                    RelationGetRelationName(rel))));
        } else {
            ereport(NOTICE,
                (errmsg("%s %s will create implicit index \"%s\" for table \"%s\"",
                    is_alter_table ? "ALTER TABLE / ADD" : "CREATE TABLE /",
                    constraint_type,
                    indexRelationName,
                    RelationGetRelationName(rel))));
        }
    }

    /*
     * A valid stmt->oldNode implies that we already have a built form of the
     * index.  The caller should also decline any index build.
     *
     * A valid stmt->oldPSortOid implies that we already have a built form
     * of the psort index.
     */
    if ((OidIsValid(stmt->oldNode) && !(skip_build && !concurrent) &&
        !u_sess->attr.attr_sql.enable_cluster_resize) ||
        (OidIsValid(stmt->oldPSortOid) && !OidIsValid(stmt->oldNode))) {
        ereport(defence_errlevel(),
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("If we can not reuse the index, we can not skip the build of index and not build concurrent "
                       "index."),
                errhint("Please reindex the index.")));
    }

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, stmt->oldPSortOid, stmt->isPartitioned, stmt->isGlobal, stmt->crossbucket);

    if (stmt->internal_flag) {
#ifdef ENABLE_MOT
        if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            Oid relationId = RelationGetRelid(rel);
            ForeignTable *ftbl = GetForeignTable(relationId);
            ForeignServer *server = GetForeignServer(ftbl->serverid);
            if (isMOTTableFromSrvName(server->servername)) {
                bool idxNameChanged = false;
                indexRelationId = index_create(rel,
                    indexRelationName,
                    indexRelationId,
                    stmt->oldNode,
                    indexInfo,
                    indexColNames,
                    accessMethodId,
                    tablespaceId,
                    collationObjectId,
                    classObjectId,
                    coloptions,
                    reloptions,
                    stmt->primary,
                    stmt->isconstraint,
                    stmt->deferrable,
                    stmt->initdeferred,
                    g_instance.attr.attr_common.allowSystemTableMods,
                    true,
                    concurrent,
                    &extra);

                /* Roll back any GUC changes executed by index functions. */
                AtEOXact_GUC(false, root_save_nestlevel);

                /* Restore userid and security context */
                SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

                heap_close(rel, NoLock);

                if (stmt->idxname == NULL) {
                    stmt->idxname = indexRelationName;
                    idxNameChanged = true;
                }

                CreateForeignIndex(stmt, indexRelationId);

                if (idxNameChanged) {
                    stmt->idxname = NULL;
                }
                ObjectAddressSet(address, RelationRelationId, indexRelationId);
                return address;
            }
            /* Roll back any GUC changes executed by index functions. */
            AtEOXact_GUC(false, root_save_nestlevel);

            /* Restore userid and security context */
            SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

            ObjectAddressSet(address, RelationRelationId, indexRelationId);
            buildInformationalConstraint(stmt, indexRelationId, indexRelationName, rel, indexInfo, namespaceId);
            return address;
        }
#endif
        /* Roll back any GUC changes executed by index functions. */
        AtEOXact_GUC(false, root_save_nestlevel);

        /* Restore userid and security context */
        SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

        buildInformationalConstraint(stmt, indexRelationId, indexRelationName, rel, indexInfo, namespaceId);
        ObjectAddressSet(address, RelationRelationId, indexRelationId);
        return address;
    }

    /* workload client manager */
    if (IS_PGXC_COORDINATOR && ENABLE_WORKLOAD_CONTROL) {
        if (!stmt->skip_mem_check && !skip_build)
            EstIdxMemInfo(rel, stmt->relation, &indexInfo->ii_desc, indexInfo, accessMethodName);
        WLMInitQueryPlan((QueryDesc*)&indexInfo->ii_desc, false);
        dywlm_client_manager((QueryDesc*)&indexInfo->ii_desc, false);
        if (!stmt->skip_mem_check && !skip_build)
            AdjustIdxMemInfo(&stmt->memUsage, &indexInfo->ii_desc);
    } else if (IS_PGXC_DATANODE && stmt->memUsage.work_mem > 0) {
        indexInfo->ii_desc.query_mem[0] = stmt->memUsage.work_mem;
        indexInfo->ii_desc.query_mem[1] = stmt->memUsage.max_mem;
    }

    /*
     * Make the catalog entries for the index, including constraints. Then, if
     * not skip_build || concurrent, actually build the index.
     */
    indexRelationId = index_create(rel,
        indexRelationName,
        indexRelationId,
        stmt->oldNode,
        indexInfo,
        indexColNames,
        accessMethodId,
        tablespaceId,
        collationObjectId,
        classObjectId,
        coloptions,
        reloptions,
        stmt->primary,
        stmt->isconstraint,
        stmt->deferrable,
        stmt->initdeferred,
        (g_instance.attr.attr_common.allowSystemTableMods || u_sess->attr.attr_common.IsInplaceUpgrade),
        skip_build || concurrent,
        concurrent,
        &extra,
        false,
        indexsplitMethod,
        parseVisibleStateFromOptions(stmt->indexOptions));

    /*
     * Roll back any GUC changes executed by index functions, and keep
     * subsequent changes local to this command.  It's barely possible that
     * some index function changed a behavior-affecting GUC, e.g. xmloption,
     * that affects subsequent steps.
     */
    AtEOXact_GUC(false, root_save_nestlevel);
    root_save_nestlevel = NewGUCNestLevel();
    
    /* index options */
    CreateNonColumnComment(indexRelationId, stmt->indexOptions, RelationRelationId);
    
    /* Add any requested comment */
    if (stmt->idxcomment != NULL)
        CreateComments(indexRelationId, RelationRelationId, 0, stmt->idxcomment);

    ObjectAddressSet(address, RelationRelationId, indexRelationId);
    /* create the LOCAL index partition */
    if (stmt->isPartitioned && !stmt->isGlobal) {
        Relation partitionedIndex = index_open(indexRelationId, AccessExclusiveLock);

        if (rel->partMap->type == PART_TYPE_RANGE || 
            rel->partMap->type == PART_TYPE_INTERVAL ||
            rel->partMap->type == PART_TYPE_HASH ||
            rel->partMap->type == PART_TYPE_LIST) {
            MemoryContext partitionIndexMemContext = AllocSetContextCreate(CurrentMemoryContext,
                "partition index create memory context",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);
            MemoryContext oldMemContext = CurrentMemoryContext;

            ListCell* tspcell = NULL;
            ListCell* indexcell = NULL;
            ListCell* partitioncell = NULL;
            ListCell* partoidcell = NULL;
            Oid partitionid = InvalidOid;
            Oid partIndexFileNode = InvalidOid;
            PartIndexCreateExtraArgs partExtra;
            int count = 0;
            char* indexname = NULL;
            Partition partition = NULL;
            Oid partitiontspid = InvalidOid;
            RangePartitionindexDefState* indexdef = NULL;
            Oid toastid = InvalidOid;
            Relation pg_partition_rel = NULL;
            int indexnum = 0;
            indexInfo->ii_BrokenHotChain = false;
            partExtra.crossbucket = stmt->crossbucket;

            if (!RelationIsSubPartitioned(rel)) {
                if (!PointerIsValid(partitionIndexdef)) {
                    ereport(ERROR,
                                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                 errmsg("fail to get index info when create index partition")));
                }
            } else {
                if (!PointerIsValid(subPartitionIndexDef)) {
                    ereport(ERROR,
                                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                 errmsg("fail to get index info when create index partition")));
                }
            }
            pg_partition_rel = heap_open(PartitionRelationId, RowExclusiveLock);

            // switch to partition memory context
            oldMemContext = MemoryContextSwitchTo(partitionIndexMemContext);

            if (!RelationIsSubPartitioned(rel)) {
                forthree(tspcell, partitiontspList, indexcell, partitionIndexdef, partitioncell, partitionOidList)
                {
                    partitiontspid = lfirst_oid(tspcell);
                    indexdef = (RangePartitionindexDefState*)lfirst(indexcell);
                    partitionid = lfirst_oid(partitioncell);

                    /* reset the extra arguments data. */
                    partExtra.existingPSortOid = InvalidOid;

                    if (PointerIsValid(stmt->partIndexOldNodes)) {
                        Assert(stmt->partIndexOldPSortOid);
                        partIndexFileNode = list_nth_oid(stmt->partIndexOldNodes, count);
                        partExtra.existingPSortOid = list_nth_oid(stmt->partIndexOldPSortOid, count);
                        count++;
                    }
                    if (u_sess->attr.attr_sql.enable_cluster_resize && stmt->partIndexUsable &&
                        !stmt->partIndexUsable[indexnum++]) {
                        ereport(LOG, (errmsg("In redistribution, the %dth partition of source index is unusable, "
                            "the tmp table will skip.", indexnum)));
                        continue;
                    }

                    partition = partitionOpen(rel, partitionid, ShareLock);

                    if (PointerIsValid(indexdef->name)) {
                        Oid indexid = InvalidOid;

                        indexid = GetSysCacheOid3(PARTPARTOID, PointerGetDatum(indexdef->name),
                                                  CharGetDatum(PART_OBJ_TYPE_INDEX_PARTITION),
                                                  ObjectIdGetDatum(indexRelationId));
                        if (OidIsValid(indexid)) {
                            ereport(ERROR,
                                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                         errmsg("index partition with name \"%s\" already exists", indexdef->name)));
                        }
                    }

                    /* get index partition's name */
                    indexname =
                        (NULL != indexdef->name)
                        ? indexdef->name
                        : ChoosePartitionIndexName(
                                    PartitionGetPartitionName(partition), indexRelationId, indexColNames, false, false);

                    /* create index partition */
                    (void)partition_index_create(indexname,
                                partIndexFileNode,
                                partition,
                                partitiontspid,
                                partitionedIndex,
                                rel,
                                pg_partition_rel,
                                indexInfo,
                                indexColNames,
                                reloptions,
                                skip_build,
                                &partExtra);
                    partitionClose(rel, partition, NoLock);

                    // reset memory context
                    MemoryContextReset(partitionIndexMemContext);
                }
            } else {
                ListCell *subTspCell = NULL;
                ListCell *subIndexCell = NULL;
                ListCell *subPartCell = NULL;
                forfour(subTspCell, subPartTspList, subIndexCell, subPartitionIndexDef, subPartCell,
                         subPartitionOidList, partoidcell, partitionOidList)
                {
                    partitiontspList = (List *)lfirst(subTspCell);
                    partitionIndexdef = (List *)lfirst(subIndexCell);
                    List *subpartitionidlist = (List *)lfirst(subPartCell);
                    Oid partid = lfirst_oid(partoidcell);

                    Partition p = partitionOpen(rel, partid, ShareLock);
                    Relation partRel = partitionGetRelation(rel, p);

                    forthree(tspcell, partitiontspList, indexcell, partitionIndexdef, partitioncell, subpartitionidlist)
                    {
                        Oid subPartitionOid = lfirst_oid(partitioncell);
                        partitiontspid = lfirst_oid(tspcell);
                        indexdef = (RangePartitionindexDefState*)lfirst(indexcell);

                        /* reset the extra arguments data. */
                        partExtra.existingPSortOid = InvalidOid;

                        if (PointerIsValid(stmt->partIndexOldNodes)) {
                            Assert(stmt->partIndexOldPSortOid);
                            partIndexFileNode = list_nth_oid(stmt->partIndexOldNodes, count);
                            partExtra.existingPSortOid = list_nth_oid(stmt->partIndexOldPSortOid, count);
                            count++;
                        }

                        partition = partitionOpen(partRel, subPartitionOid, ShareLock);

                        if (PointerIsValid(indexdef->name)) {
                            Oid indexid = InvalidOid;

                            indexid = GetSysCacheOid3(PARTPARTOID, PointerGetDatum(indexdef->name),
                                                      CharGetDatum(PART_OBJ_TYPE_INDEX_PARTITION),
                                                      ObjectIdGetDatum(indexRelationId));
                            if (OidIsValid(indexid)) {
                                ereport(ERROR,
                                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                         errmsg("index subpartition with name \"%s\" already exists", indexdef->name)));
                            }
                        }

                        /* get index partition's name */
                        indexname = (NULL != indexdef->name)
                                        ? indexdef->name
                                        : ChoosePartitionIndexName(PartitionGetPartitionName(partition),
                                                                   indexRelationId, indexColNames, false, false);

                        /* create index partition */
                        (void)partition_index_create(indexname,
                                    partIndexFileNode,
                                    partition,
                                    partitiontspid,
                                    partitionedIndex,
                                    partRel,
                                    pg_partition_rel,
                                    indexInfo,
                                    indexColNames,
                                    reloptions,
                                    skip_build,
                                    &partExtra);
                        partitionClose(partRel, partition, NoLock);

                        // reset memory context
                        MemoryContextReset(partitionIndexMemContext);
                    }

                    releaseDummyRelation(&partRel);
                    partitionClose(rel, p, ShareLock);
                }
            }

            // delete memory context
            (void)MemoryContextSwitchTo(oldMemContext);
            MemoryContextDelete(partitionIndexMemContext);

            if (rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
                toastid = RelationGetRelid(partitionedIndex);
            }
            /* update heap pg_class rows */
            index_update_stats(rel, true, stmt->primary, toastid, InvalidOid, -1);
            /* update index pg_class rows */
            index_update_stats(partitionedIndex, false, false, InvalidOid, InvalidOid, -1);

            heap_close(pg_partition_rel, RowExclusiveLock);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("unsupport partitioned strategy")));
        }

        if (RELATION_IS_PARTITIONED(rel)) {
            releasePartitionOidList(&partitionOidList);
        }
        if (RelationIsSubPartitioned(rel)) {
            ReleaseSubPartitionOidList(&subPartitionOidList);
        }

        /* Roll back any GUC changes executed by index functions. */
        AtEOXact_GUC(false, root_save_nestlevel);

        /* Restore userid and security context */
        SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

        heap_close(partitionedIndex, NoLock);
        heap_close(rel, NoLock);
        ObjectAddressSet(address, RelationRelationId, indexRelationId);
        return address;
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        releasePartitionOidList(&partitionOidList);
    }
    if (RelationIsSubPartitioned(rel)) {
        ReleaseSubPartitionOidList(&subPartitionOidList);
    }

    /* Roll back any GUC changes executed by index functions. */
    AtEOXact_GUC(false, root_save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(root_save_userid, root_save_sec_context);

    if (!concurrent) {
        /* Close the heap and we're done, in the non-concurrent case */
        heap_close(rel, NoLock);
        return address;
    }

    // cstore relation doesn't support concurrent INDEX now.
	if (OidIsValid(rel->rd_rel->relcudescrelid)) {
	  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		  errmsg("column store table does not support concurrent INDEX yet"),
		  errdetail("The feature is not currently supported")));
	}

    /* save lockrelid and locktag for below, then close rel */
    heaprelid = rel->rd_lockInfo.lockRelId;
    SET_LOCKTAG_RELATION(heaplocktag, heaprelid.dbId, heaprelid.relId);
    heap_close(rel, NoLock);

    /*
     * For a concurrent build, it's important to make the catalog entries
     * visible to other transactions before we start to build the index. That
     * will prevent them from making incompatible HOT updates.	The new index
     * will be marked not indisready and not indisvalid, so that no one else
     * tries to either insert into it or use it for queries.
     *
     * We must commit our current transaction so that the index becomes
     * visible; then start another.  Note that all the data structures we just
     * built are lost in the commit.  The only data we keep past here are the
     * relation IDs.
     *
     * Before committing, get a session-level lock on the table, to ensure
     * that neither it nor the index can be dropped before we finish. This
     * cannot block, even if someone else is waiting for access, because we
     * already have the same lock within our transaction.
     *
     * Note: we don't currently bother with a session lock on the index,
     * because there are no operations that could change its state while we
     * hold lock on the parent table.  This might need to change later.
     */
    LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);

    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 2 of concurrent index build (see comments for validate_index()
     * for an overview of how this works)
     *
     * Now we must wait until no running transaction could have the table open
     * with the old list of indexes.  To do this, inquire which xacts
     * currently would conflict with ShareLock on the table -- ie, which ones
     * have a lock that permits writing the table.	Then wait for each of
     * these xacts to commit or abort.	Note we do not need to worry about
     * xacts that open the table for writing after this point; they will see
     * the new index when they open it.
     *
     * Note: the reason we use actual lock acquisition here, rather than just
     * checking the ProcArray and sleeping, is that deadlock is possible if
     * one of the transactions in question is blocked trying to acquire an
     * exclusive lock on our table.  The lock code will detect deadlock and
     * error out properly.
     *
     * Note: GetLockConflicts() never reports our own xid, hence we need not
     * check for that.	Also, prepared xacts are not reported, which is fine
     * since they certainly aren't going to do anything more.
     */
    old_lockholders = GetLockConflicts(&heaplocktag, ShareLock);

    while (VirtualTransactionIdIsValid(*old_lockholders)) {
        (void)VirtualXactLock(*old_lockholders, true);
        old_lockholders++;
    }

    /*
     * At this moment we are sure that there are no transactions with the
     * table open for write that don't have this new index in their list of
     * indexes.  We have waited out all the existing transactions and any new
     * transaction will have the new index in its list, but the index is still
     * marked as "not-ready-for-inserts".  The index is consulted while
     * deciding HOT-safety though.	This arrangement ensures that no new HOT
     * chains can be created where the new tuple and the old tuple in the
     * chain have different index keys.
     *
     * We now take a new snapshot, and build the index using all tuples that
     * are visible in this snapshot.  We can be sure that any HOT updates to
     * these tuples will be compatible with the index, since any updates made
     * by transactions that didn't know about the index are now committed or
     * rolled back.  Thus, each visible tuple is either the end of its
     * HOT-chain or the extension of the chain is HOT-safe for this index.
     */
    
    /* Set ActiveSnapshot since functions in the indexes may need it */
    PushActiveSnapshot(GetTransactionSnapshot());

    u_sess->attr.attr_sql.create_index_concurrently = true;
    /* Perform concurrent build of index */
    index_concurrently_build(relationId, indexRelationId, stmt->primary);

    /* we can do away with our snapshot */
    PopActiveSnapshot();

    /*
     * Commit this transaction to make the indisready update visible.
     */
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 3 of concurrent index build
     *
     * We once again wait until no transaction can have the table open with
     * the index marked as read-only for updates.
     */
    old_lockholders = GetLockConflicts(&heaplocktag, ShareLock);

    while (VirtualTransactionIdIsValid(*old_lockholders)) {
        (void)VirtualXactLock(*old_lockholders, true);
        old_lockholders++;
    }

    /*
     * Now take the "reference snapshot" that will be used by validate_index()
     * to filter candidate tuples.	Beware!  There might still be snapshots in
     * use that treat some transaction as in-progress that our reference
     * snapshot treats as committed.  If such a recently-committed transaction
     * deleted tuples in the table, we will not include them in the index; yet
     * those transactions which see the deleting one as still-in-progress will
     * expect such tuples to be there once we mark the index as valid.
     *
     * We solve this by waiting for all endangered transactions to exit before
     * we mark the index as valid.
     *
     * We also set ActiveSnapshot to this snap, since functions in indexes may
     * need a snapshot.
     */
    snapshot = RegisterSnapshot(GetTransactionSnapshot());
    PushActiveSnapshot(snapshot);

    /*
     * Scan the index and the heap, insert any missing index entries.
     */
    validate_index(relationId, indexRelationId, snapshot);

    /*
     * Drop the reference snapshot.  We must do this before waiting out other
     * snapshot holders, else we will deadlock against other processes also
     * doing CREATE INDEX CONCURRENTLY, which would see our snapshot as one
     * they must wait for.  But first, save the snapshot's xmin to use as
     * limitXmin for GetCurrentVirtualXIDs().
     */
    limitXmin = snapshot->xmin;

    PopActiveSnapshot();
    UnregisterSnapshot(snapshot);

    /*
     * The snapshot subsystem could still contain registered snapshots that
     * are holding back our process's advertised xmin; in particular, if
     * default_transaction_isolation = serializable, there is a transaction
     * snapshot that is still active.  The CatalogSnapshot is likewise a
     * hazard.  To ensure no deadlocks, we must commit and start yet another
     * transaction, and do our wait before any snapshot has been taken in it.
     */
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * The index is now valid in the sense that it contains all currently
     * interesting tuples.	But since it might not contain tuples deleted just
     * before the reference snap was taken, we have to wait out any
     * transactions that might have older snapshots. 
     */
    WaitForOlderSnapshots(limitXmin);

    if (IS_PGXC_COORDINATOR) {
        /*
        * Last thing to do is release the session-level lock on the parent table.
        */
        UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);

        return address;
    }

    /*
     * Index can now be marked valid -- update its pg_index entry
     */
    index_set_state_flags(indexRelationId, INDEX_CREATE_SET_VALID);

    /*
     * The pg_index update will cause backends (including this one) to update
     * relcache entries for the index itself, but we should also send a
     * relcache inval on the parent table to force replanning of cached plans.
     * Otherwise existing sessions might fail to use the new index where it
     * would be useful.  (Note that our earlier commits did not create reasons
     * to replan; so relcache flush on the index itself was sufficient.)
     */
    CacheInvalidateRelcacheByRelid(heaprelid.relId);

    /*
     * Last thing to do is release the session-level lock on the parent table.
     */
    UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);

    return address;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Input		:
 * Output	:
 * Return		:
 * Notes		:
 */
static bool columnIsExist(Relation rel, const Form_pg_attribute attTup, const List* indexParams)
{
    char* attname = NameStr(attTup->attname); /* get the column name */
    ListCell* cell = NULL;

    /* is the name exist in indexParams */
    foreach (cell, indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(cell);

        if (NULL == ielem->name) {
            Assert(NULL != ielem->expr);
            /* it is a expression */
            continue;
        }

        if (0 == strcmp(attname, ielem->name)) {
            /* check the collation */
            if (NULL == ielem->collation) {
                return true;
            } else {
                /* collation is same,return true ,else return false */
                HeapTuple tuple;
                Value* colValue = (Value*)linitial(ielem->collation);
                char* colName = colValue->val.str;
                Oid colId = CollationGetCollid(colName);
                Oid attColId = InvalidOid;

                if (0 == colId) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("collation \"%s\" does not exist", colName)));
                }

                /* find the column type 's collation */
                tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attTup->atttypid));
                if (!HeapTupleIsValid(tuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for type %u", attTup->atttypid)));

                attColId = ((Form_pg_type)GETSTRUCT(tuple))->typcollation;

                ReleaseSysCache(tuple);

                if (attColId == 0) {
                    /* type does not have a collation */
                    return true;
                } else if (attColId == colId) {
                    return true;
                }

                return false;
            }
        }
    }

    return false;
}

/*
 * CheckMutability
 *		Test whether given expression is mutable
 */
bool CheckMutability(Expr* expr)
{
    /*
     * First run the expression through the planner.  This has a couple of
     * important consequences.	First, function default arguments will get
     * inserted, which may affect volatility (consider "default now()").
     * Second, inline-able functions will get inlined, which may allow us to
     * conclude that the function is really less volatile than it's marked. As
     * an example, polymorphic functions must be marked with the most volatile
     * behavior that they have for any input type, but once we inline the
     * function we may be able to conclude that it's not so volatile for the
     * particular input type we're dealing with.
     *
     * We assume here that expression_planner() won't scribble on its input.
     */
    expr = expression_planner(expr);

    /* Now we can search for non-immutable functions */
    return contain_mutable_functions((Node*)expr);
}

/*
 * CheckPredicate
 *		Checks that the given partial-index predicate is valid.
 *
 * This used to also constrain the form of the predicate to forms that
 * indxpath.c could do something with.	However, that seems overly
 * restrictive.  One useful application of partial indexes is to apply
 * a UNIQUE constraint across a subset of a table, and in that scenario
 * any evaluatable predicate will work.  So accept any predicate here
 * (except ones requiring a plan), and let indxpath.c fend for itself.
 */
void CheckPredicate(Expr* predicate)
{
    /*
     * We don't currently support generation of an actual query plan for a
     * predicate, only simple scalar expressions; hence these restrictions.
     */
    if (contain_subplans((Node*)predicate))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in index predicate")));
    if (contain_agg_clause((Node*)predicate))
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate in index predicate")));

    /*
     * A predicate using mutable functions is probably wrong, for the same
     * reasons that we don't allow an index expression to use one.
     */
    if (CheckMutability(predicate))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("functions in index predicate must be marked IMMUTABLE")));
}

/*
 * Compute per-index-column information, including indexed column numbers
 * or index expressions, opclasses, and indoptions.
 */
void ComputeIndexAttrs(IndexInfo* indexInfo, Oid* typeOidP, Oid* collationOidP, Oid* classOidP,
    int16* colOptionP, List* attList, /* list of IndexElem's */
    List* exclusionOpNames, Oid relId, const char* accessMethodName, Oid accessMethodId, bool amcanorder, bool isconstraint)
{
    ListCell* nextExclOp = NULL;
    ListCell* lc = NULL;
    int attn;
    int nkeycols = indexInfo->ii_NumIndexKeyAttrs;

    /* Allocate space for exclusion operator info, if needed */
    if (exclusionOpNames != NULL) {
        Assert(list_length(exclusionOpNames) == nkeycols);
        indexInfo->ii_ExclusionOps = (Oid*)palloc(sizeof(Oid) * nkeycols);
        indexInfo->ii_ExclusionProcs = (Oid*)palloc(sizeof(Oid) * nkeycols);
        indexInfo->ii_ExclusionStrats = (uint16*)palloc(sizeof(uint16) * nkeycols);
        nextExclOp = list_head(exclusionOpNames);
    } else
        nextExclOp = NULL;

    /*
     * process attributeList
     */
    attn = 0;
    foreach (lc, attList) {
        IndexElem* attribute = (IndexElem*)lfirst(lc);
        Oid atttype;
        Oid attcollation;

        /*
         * Process the column-or-expression to be indexed.
         */
        if (attribute->name != NULL) {
            /* Simple index attribute */
            HeapTuple atttuple;
            Form_pg_attribute attform;

            Assert(attribute->expr == NULL);
            atttuple = SearchSysCacheAttName(relId, attribute->name);
            if (!HeapTupleIsValid(atttuple)) {
                /* difference in error message spellings is historical */
                if (isconstraint)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("column \"%s\" named in key does not exist", attribute->name)));
                else
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", attribute->name)));
            }
            attform = (Form_pg_attribute)GETSTRUCT(atttuple);
            indexInfo->ii_KeyAttrNumbers[attn] = attform->attnum;
            atttype = attform->atttypid;
            attcollation = attform->attcollation;
            ReleaseSysCache(atttuple);
        } else {
            /* Index expression */
            Node* expr = attribute->expr;

            Assert(expr != NULL);
            if (attn >= nkeycols) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("expressions are not supported in included columns")));
            }
            atttype = exprType(expr);
            attcollation = exprCollation(expr);

            /*
             * Strip any top-level COLLATE clause.	This ensures that we treat
             * "x COLLATE y" and "(x COLLATE y)" alike.
             */
            while (IsA(expr, CollateExpr))
                expr = (Node*)((CollateExpr*)expr)->arg;

            if (IsA(expr, Var) && ((Var*)expr)->varattno != InvalidAttrNumber) {
                /*
                 * User wrote "(column)" or "(column COLLATE something)".
                 * Treat it like simple attribute anyway.
                 */
                indexInfo->ii_KeyAttrNumbers[attn] = ((Var*)expr)->varattno;
            } else {
                indexInfo->ii_KeyAttrNumbers[attn] = 0; /* marks expression */
                indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions, expr);

                /*
                 * We don't currently support generation of an actual query
                 * plan for an index expression, only simple scalar
                 * expressions; hence these restrictions.
                 */
                if (contain_subplans(expr))
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in index expression")));
                if (contain_agg_clause(expr))
                    ereport(ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in index expression")));

                /*
                 * A expression using mutable functions is probably wrong,
                 * since if you aren't going to get the same result for the
                 * same data every time, it's not clear what the index entries
                 * mean at all.
                 */
                if (CheckMutability((Expr*)expr))
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("functions in index expression must be marked IMMUTABLE")));
            }
        }

        typeOidP[attn] = atttype;

       /*
        * Included columns have no collation, no opclass and no ordering options.
        */
       if (attn >= nkeycols) {
            if (attribute->collation) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("including column does not support a collation")));
            }
            if (attribute->opclass) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("including column does not support an operator class")));
            }
            if (attribute->ordering != SORTBY_DEFAULT) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("including column does not support ASC/DESC options")));
            }
            if (attribute->nulls_ordering != SORTBY_NULLS_DEFAULT) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("including column does not support NULLS FIRST/LAST options")));
            }
            classOidP[attn] = InvalidOid;
            colOptionP[attn] = 0;
            collationOidP[attn] = InvalidOid;
            attn++;
            continue;
        }

        /*
         * Apply collation override if any
         */
        if (attribute->collation)
            attcollation = get_collation_oid(attribute->collation, false);

        /*
         * Check we have a collation iff it's a collatable type.  The only
         * expected failures here are (1) COLLATE applied to a noncollatable
         * type, or (2) index expression had an unresolved collation.  But we
         * might as well code this to be a complete consistency check.
         */
        if (type_is_collatable(atttype)) {
            if (!OidIsValid(attcollation))
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for index expression"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
        } else {
            if (OidIsValid(attcollation))
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("collations are not supported by type %s", format_type_be(atttype))));
        }

        collationOidP[attn] = attcollation;

        /*
         * Identify the opclass to use.
         */
        classOidP[attn] = GetIndexOpClass(attribute->opclass, atttype, accessMethodName, accessMethodId);

        /*
         * Identify the exclusion operator, if any.
         */
        if (nextExclOp != NULL) {
            List* opname = (List*)lfirst(nextExclOp);
            Oid opid;
            Oid opfamily;
            int strat;

            /*
             * Find the operator --- it must accept the column datatype
             * without runtime coercion (but binary compatibility is OK)
             */
            opid = compatible_oper_opid(opname, atttype, atttype, false);

            /*
             * Only allow commutative operators to be used in exclusion
             * constraints. If X conflicts with Y, but Y does not conflict
             * with X, bad things will happen.
             */
            if (get_commutator(opid) != opid)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("operator %s is not commutative", format_operator(opid)),
                        errdetail("Only commutative operators can be used in exclusion constraints.")));

            /*
             * Operator must be a member of the right opfamily, too
             */
            opfamily = get_opclass_family(classOidP[attn]);
            strat = get_op_opfamily_strategy(opid, opfamily);
            if (strat == 0) {
                HeapTuple opftuple;
                Form_pg_opfamily opfform;

                /*
                 * attribute->opclass might not explicitly name the opfamily,
                 * so fetch the name of the selected opfamily for use in the
                 * error message.
                 */
                opftuple = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamily));
                if (!HeapTupleIsValid(opftuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for opfamily %u", opfamily)));
                opfform = (Form_pg_opfamily)GETSTRUCT(opftuple);

                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("operator %s is not a member of operator family \"%s\"",
                            format_operator(opid),
                            NameStr(opfform->opfname)),
                        errdetail(
                            "The exclusion operator must be related to the index operator class for the constraint.")));
            }

            indexInfo->ii_ExclusionOps[attn] = opid;
            indexInfo->ii_ExclusionProcs[attn] = get_opcode(opid);
            indexInfo->ii_ExclusionStrats[attn] = strat;
            nextExclOp = lnext(nextExclOp);
        }

        /*
         * Set up the per-column options (indoption field).  For now, this is
         * zero for any un-ordered index, while ordered indexes have DESC and
         * NULLS FIRST/LAST options.
         */
        colOptionP[attn] = 0;
        if (amcanorder) {
            /* default ordering is ASC */
            if (attribute->ordering == SORTBY_DESC)
                colOptionP[attn] = ((uint16)colOptionP[attn]) | INDOPTION_DESC;
            /* default null ordering is LAST for ASC, FIRST for DESC */
            if (attribute->nulls_ordering == SORTBY_NULLS_DEFAULT) {
                if (attribute->ordering == SORTBY_DESC)
                    colOptionP[attn] = ((uint16)colOptionP[attn]) | INDOPTION_NULLS_FIRST;
            } else if (attribute->nulls_ordering == SORTBY_NULLS_FIRST)
                colOptionP[attn] = ((uint16)colOptionP[attn]) | INDOPTION_NULLS_FIRST;
        } else {
            /* index AM does not support ordering */
            if (attribute->ordering != SORTBY_DEFAULT)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("access method \"%s\" does not support ASC/DESC options", accessMethodName)));
            if (attribute->nulls_ordering != SORTBY_NULLS_DEFAULT)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("access method \"%s\" does not support NULLS FIRST/LAST options", accessMethodName)));
        }

        attn++;
    }
}

/*
 * Resolve possibly-defaulted operator class specification
 */
Oid GetIndexOpClass(List* opclass, Oid attrType, const char* accessMethodName, Oid accessMethodId)
{
    char* schemaname = NULL;
    char* opcname = NULL;
    HeapTuple tuple;
    Oid opClassId, opInputType;

    /*
     * Release 7.0 removed network_ops, timespan_ops, and datetime_ops, so we
     * ignore those opclass names so the default *_ops is used.  This can be
     * removed in some later release.  bjm 2000/02/07
     *
     * Release 7.1 removes lztext_ops, so suppress that too for a while.  tgl
     * 2000/07/30
     *
     * Release 7.2 renames timestamp_ops to timestamptz_ops, so suppress that
     * too for awhile.	I'm starting to think we need a better approach. tgl
     * 2000/10/01
     *
     * Release 8.0 removes bigbox_ops (which was dead code for a long while
     * anyway).  tgl 2003/11/11
     */
    if (list_length(opclass) == 1) {
        char* claname = strVal(linitial(opclass));

        if (strcmp(claname, "network_ops") == 0 || strcmp(claname, "timespan_ops") == 0 ||
            strcmp(claname, "datetime_ops") == 0 || strcmp(claname, "lztext_ops") == 0 ||
            strcmp(claname, "timestamp_ops") == 0 || strcmp(claname, "bigbox_ops") == 0)
            opclass = NIL;
    }

    if (opclass == NIL) {
        /* no operator class specified, so find the default */
        opClassId = GetDefaultOpClass(attrType, accessMethodId);
        if (!OidIsValid(opClassId))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("data type %s has no default operator class for access method \"%s\"",
                        format_type_be(attrType),
                        accessMethodName),
                    errhint("You must specify an operator class for the index or define a default operator class for "
                            "the data type.")));
        return opClassId;
    }

    /*
     * Specific opclass name given, so look up the opclass.
     * deconstruct the name list
     */
    DeconstructQualifiedName(opclass, &schemaname, &opcname);

    if (schemaname != NULL) {
        /* Look in specific schema only */
        Oid namespaceId;

        namespaceId = LookupExplicitNamespace(schemaname);
        tuple = SearchSysCache3(
            CLAAMNAMENSP, ObjectIdGetDatum(accessMethodId), PointerGetDatum(opcname), ObjectIdGetDatum(namespaceId));
    } else {
        /* Unqualified opclass name, so search the search path */
        opClassId = OpclassnameGetOpcid(accessMethodId, opcname);
        if (!OidIsValid(opClassId))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg(
                        "operator class \"%s\" does not exist for access method \"%s\"", opcname, accessMethodName)));
        tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(opClassId));
    }

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("operator class \"%s\" does not exist for access method \"%s\"",
                    NameListToString(opclass),
                    accessMethodName)));

    /*
     * Verify that the index operator class accepts this datatype.	Note we
     * will accept binary compatibility.
     */
    opClassId = HeapTupleGetOid(tuple);
    opInputType = ((Form_pg_opclass)GETSTRUCT(tuple))->opcintype;

    if (!IsBinaryCoercible(attrType, opInputType))
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("operator class \"%s\" does not accept data type %s",
                    NameListToString(opclass),
                    format_type_be(attrType))));

    ReleaseSysCache(tuple);

    return opClassId;
}

/*
 * GetDefaultOpClass
 *
 * Given the OIDs of a datatype and an access method, find the default
 * operator class, if any.	Returns InvalidOid if there is none.
 */
Oid GetDefaultOpClass(Oid type_id, Oid am_id)
{
    Oid result = InvalidOid;
    int nexact = 0;
    int ncompatible = 0;
    int ncompatiblepreferred = 0;
    Relation rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple tup;
    TYPCATEGORY tcategory;

    /* If it's a domain, look at the base type instead */
    type_id = getBaseType(type_id);

    tcategory = TypeCategory(type_id);

    /*
     * We scan through all the opclasses available for the access method,
     * looking for one that is marked default and matches the target type
     * (either exactly or binary-compatibly, but prefer an exact match).
     *
     * We could find more than one binary-compatible match.  If just one is
     * for a preferred type, use that one; otherwise we fail, forcing the user
     * to specify which one he wants.  (The preferred-type special case is a
     * kluge for varchar: it's binary-compatible to both text and bpchar, so
     * we need a tiebreaker.)  If we find more than one exact match, then
     * someone put bogus entries in pg_opclass.
     */
    rel = heap_open(OperatorClassRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_opclass_opcmethod, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(am_id));

    scan = systable_beginscan(rel, OpclassAmNameNspIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_opclass opclass = (Form_pg_opclass)GETSTRUCT(tup);

        /* ignore altogether if not a default opclass */
        if (!opclass->opcdefault)
            continue;
        if (opclass->opcintype == type_id) {
            nexact++;
            result = HeapTupleGetOid(tup);
        } else if (nexact == 0 && IsBinaryCoercible(type_id, opclass->opcintype)) {
            if (IsPreferredType(tcategory, opclass->opcintype)) {
                ncompatiblepreferred++;
                result = HeapTupleGetOid(tup);
            } else if (ncompatiblepreferred == 0) {
                ncompatible++;
                result = HeapTupleGetOid(tup);
            }
        }
    }

    systable_endscan(scan);

    heap_close(rel, AccessShareLock);

    /* raise error if pg_opclass contains inconsistent data */
    if (nexact > 1)
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("there are multiple default operator classes for data type %s", format_type_be(type_id))));

    if (nexact == 1 || ncompatiblepreferred == 1 || (ncompatiblepreferred == 0 && ncompatible == 1))
        return result;

    return InvalidOid;
}

/*
 *	makeObjectName()
 *
 *	Create a name for an implicitly created index, sequence, constraint, etc.
 *
 *	The parameters are typically: the original table name, the original field
 *	name, and a "type" string (such as "seq" or "pkey").	The field name
 *	and/or type can be NULL if not relevant.
 *
 *	The result is a palloc'd string.
 *
 *	The basic result we want is "name1_name2_label", omitting "_name2" or
 *	"_label" when those parameters are NULL.  However, we must generate
 *	a name with less than NAMEDATALEN characters!  So, we truncate one or
 *	both names if necessary to make a short-enough string.	The label part
 *	is never truncated (so it had better be reasonably short).
 *
 *	The caller is responsible for checking uniqueness of the generated
 *	name and retrying as needed; retrying will be done by altering the
 *	"label" string (which is why we never truncate that part).
 */
char* makeObjectName(const char* name1, const char* name2, const char* label, bool reverseTruncate)
{
    char* name = NULL;
    int overhead = 0; /* chars needed for label and underscores */
    int availchars;   /* chars available for name(s) */
    int name1chars;   /* chars allocated to name1 */
    int name2chars;   /* chars allocated to name2 */
    int ndx;
    errno_t rc;

    name1chars = strlen(name1);
    if (name2 != NULL) {
        name2chars = strlen(name2);
        overhead++; /* allow for separating underscore */
    } else
        name2chars = 0;
    if (label != NULL)
        overhead += strlen(label) + 1;

    availchars = NAMEDATALEN - 1 - overhead;
    Assert(availchars > 0); /* else caller chose a bad label */

    /*
     * If we must truncate,  preferentially truncate the longer name. This
     * logic could be expressed without a loop, but it's simple and obvious as
     * a loop.
     */
    while (name1chars + name2chars > availchars) {
        if (!reverseTruncate) {
            if (name1chars > name2chars) {
                name1chars--;
            } else {
                name2chars--;
            }
        } else {
            if (name2chars > 0) {
                name2chars--;
            } else if (name1chars > 0) {
                name1chars--;
            }
        }
    }

    name1chars = pg_mbcliplen(name1, name1chars, name1chars);
    if (name2 != NULL)
        name2chars = pg_mbcliplen(name2, name2chars, name2chars);

    /* Now construct the string using the chosen lengths */
    Size name_len = name1chars + name2chars + overhead + 1;
    name = (char*)palloc0(name_len);
    rc = memcpy_s(name, name_len, name1, name1chars);
    securec_check(rc, "\0", "\0");
    ndx = name1chars;
    if (name2 != NULL) {
        name[ndx++] = '_';
        rc = memcpy_s(name + ndx, (name_len - ndx), name2, name2chars);
        securec_check(rc, "\0", "\0");
        ndx += name2chars;
    }

    /* Copy label and make sure the last character is '\0' */
    if (label && 0 != strlen(label)) {
        name[ndx++] = '_';
        rc = strcpy_s(name + ndx, (name_len - ndx), label);
        securec_check(rc, "\0", "\0");
    } else
        name[ndx] = '\0';

    return name;
}

/*
 * Select a nonconflicting name for a new relation.  This is ordinarily
 * used to choose index names (which is why it's here) but it can also
 * be used for sequences, or any autogenerated relation kind.
 *
 * name1, name2, and label are used the same way as for makeObjectName(),
 * except that the label can't be NULL; digits will be appended to the label
 * if needed to create a name that is unique within the specified namespace.
 *
 * labelLength is the length of label string without '\0'.
 *
 * Note: it is theoretically possible to get a collision anyway, if someone
 * else chooses the same name concurrently.  This is fairly unlikely to be
 * a problem in practice, especially if one is holding an exclusive lock on
 * the relation identified by name1.  However, if choosing multiple names
 * within a single command, you'd better create the new object and do
 * CommandCounterIncrement before choosing the next one!
 *
 * Returns a palloc'd string.
 */
char* ChooseRelationName(const char* name1, const char* name2, const char* label, size_t labelLength,
                         Oid namespaceid, bool reverseTruncate)
{
    int64 pass = 0;
    char* relname = NULL;
    char modlabel[NAMEDATALEN];
    errno_t rc = EOK;

    /* make sure modlabel is is zero-padded */
    rc = memset_s(modlabel, NAMEDATALEN, '\0', NAMEDATALEN);
    securec_check(rc, "", "");

    /* try the unmodified label first */
    if (label != NULL) {
        /* Name string should be truncated into 63 chars and 1 '\0' */
        rc = memcpy_s(modlabel, NAMEDATALEN - 1, label, labelLength);
        securec_check(rc, "\0", "\0");
    }

    for (;;) {
        relname = makeObjectName(name1, name2, modlabel, reverseTruncate);

        if (!OidIsValid(get_relname_relid(relname, namespaceid)))
            break;

        /* found a conflict, so try a new name component */
        pfree_ext(relname);
        if (label != NULL) {
            rc = snprintf_s(modlabel, sizeof(modlabel), sizeof(modlabel) - 1, "%s%ld", label, ++pass);
            securec_check_ss(rc, "", "");
        } else {
            rc = snprintf_s(modlabel, sizeof(modlabel), sizeof(modlabel) - 1, "%ld", ++pass);
            securec_check_ss(rc, "", "");
        }
    }

    return relname;
}

static char* ChoosePartitionName(
    const char* name1, const char* name2, const char* label, Oid partitionedrelid, char partType)
{
    int pass = 0;
    char* partname = NULL;
    char modlabel[NAMEDATALEN];
    errno_t rc = EOK;
    /* try the unmodified label first */
    rc = strncpy_s(modlabel, sizeof(modlabel), label, sizeof(modlabel) - 1);
    securec_check(rc, "", "");

    for (;;) {
        partname = makeObjectName(name1, name2, modlabel);

        if (!OidIsValid(GetSysCacheOid3(
                PARTPARTOID, PointerGetDatum(partname), CharGetDatum(partType), ObjectIdGetDatum(partitionedrelid)))) {
            break;
        }

        /* found a conflict, so try a new name component */
        pfree_ext(partname);
        rc = snprintf_s(modlabel, sizeof(modlabel), sizeof(modlabel) - 1, "%s%d", label, ++pass);
        securec_check_ss(rc, "\0", "\0");
    }

    return partname;
}

/*
 * Select the name to be used for an index.
 *
 * The argument list is pretty ad-hoc :-(
 */
static char* ChooseIndexName(const char* tabname, Oid namespaceId, const List* colnames, const List* exclusionOpNames,
    bool primary, bool isconstraint, bool psort)
{
    char* indexname = NULL;

    if (primary) {
        /* the primary key's name does not depend on the specific column(s) */
        indexname = ChooseRelationName(tabname, NULL, "pkey", strlen("pkey"), namespaceId);
    } else if (exclusionOpNames != NIL) {
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames), "excl", strlen("excl"), namespaceId);
    } else if (isconstraint) {
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames), "key", strlen("key"), namespaceId);
    } else if (psort) {
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames), "psort", strlen("psort"),
                                       namespaceId);
    } else {
        indexname = ChooseRelationName(tabname, ChooseIndexNameAddition(colnames), "idx", strlen("idx"), namespaceId);
    }

    return indexname;
}

char* ChoosePSortIndexName(const char* tabname, Oid namespaceId, List* colnames)
{
    return ChooseIndexName(tabname, namespaceId, colnames, NULL, false, false, true);
}

static char* ChoosePartitionIndexName(const char* partname, Oid partitionedindexId,
    List* colnames, /* List *exclusionOpNames, */
    bool primary, bool isconstraint, bool psort)
{
    char* indexname = NULL;

    if (primary) {
        /* the primary key's name does not depend on the specific column(s) */
        indexname = ChoosePartitionName(partname, NULL, "pkey", partitionedindexId, PART_OBJ_TYPE_INDEX_PARTITION);
    } else if (isconstraint) {
        indexname = ChoosePartitionName(
            partname, ChooseIndexNameAddition(colnames), "key", partitionedindexId, PART_OBJ_TYPE_INDEX_PARTITION);
    } else if (psort) {
        indexname = ChoosePartitionName(
            partname, ChooseIndexNameAddition(colnames), "psort", partitionedindexId, PART_OBJ_TYPE_INDEX_PARTITION);
    } else {
        indexname = ChoosePartitionName(
            partname, ChooseIndexNameAddition(colnames), "idx", partitionedindexId, PART_OBJ_TYPE_INDEX_PARTITION);
    }

    return indexname;
}
/*
 * Generate "name2" for a new index given the list of column names for it
 * (as produced by ChooseIndexColumnNames).  This will be passed to
 * ChooseRelationName along with the parent table name and a suitable label.
 *
 * We know that less than NAMEDATALEN characters will actually be used,
 * so we can truncate the result once we've generated that many.
 */
static char* ChooseIndexNameAddition(const List* colnames)
{
    char buf[NAMEDATALEN * 2];
    int buflen = 0;
    ListCell* lc = NULL;

    buf[0] = '\0';
    foreach (lc, colnames) {
        const char* name = (const char*)lfirst(lc);

        if (buflen > 0)
            buf[buflen++] = '_'; /* insert _ between names */

        /*
         * At this point we have buflen <= NAMEDATALEN.  name should be less
         * than NAMEDATALEN already, but use strlcpy for paranoia.
         */
        strlcpy(buf + buflen, name, NAMEDATALEN);
        buflen += strlen(buf + buflen);
        if (buflen >= NAMEDATALEN)
            break;
    }
    return pstrdup(buf);
}

/*
 * Select the actual names to be used for the columns of an index, given the
 * list of IndexElems for the columns.	This is mostly about ensuring the
 * names are unique so we don't get a conflicting-attribute-names error.
 *
 * Returns a List of plain strings (char *, not String nodes).
 */
List* ChooseIndexColumnNames(const List* indexElems)
{
    List* result = NIL;
    ListCell* lc = NULL;
    errno_t rc;

    foreach (lc, indexElems) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);
        const char* origname = NULL;
        const char* curname = NULL;
        int i = 0;
        char buf[NAMEDATALEN];

        /* Get the preliminary name from the IndexElem */
        if (ielem->indexcolname)
            origname = ielem->indexcolname; /* caller-specified name */
        else if (ielem->name)
            origname = ielem->name; /* simple column reference */
        else
            origname = "expr"; /* default name for expression */

        /* If it conflicts with any previous column, tweak it */
        curname = origname;
        for (i = 1;; i++) {
            ListCell* lc2 = NULL;
            char nbuf[32];
            int nlen;

            foreach (lc2, result) {
                if (strcmp(curname, (char*)lfirst(lc2)) == 0)
                    break;
            }
            if (lc2 == NULL)
                break; /* found nonconflicting name */

            rc = sprintf_s(nbuf, sizeof(nbuf), "%d", i);
            securec_check_ss(rc, "\0", "\0");

            /* Ensure generated names are shorter than NAMEDATALEN */
            nlen = pg_mbcliplen(origname, strlen(origname), NAMEDATALEN - 1 - strlen(nbuf));
            rc = memcpy_s(buf, nlen, origname, nlen);
            securec_check(rc, "\0", "\0");
            rc = strcpy_s(buf + nlen, NAMEDATALEN - 1 - nlen, nbuf);
            securec_check(rc, "\0", "\0");
            curname = buf;
        }

        /* And attach to the result list */
        result = lappend(result, pstrdup(curname));
    }
    return result;
}

/*
 * ReindexIndex
 *		Recreate a specific index.
 */
Oid ReindexIndex(RangeVar* indexRelation, const char* partition_name, AdaptMem* mem_info, bool concurrent)
{
    struct ReindexIndexCallbackState state;
    Oid indOid;
    Oid indPartOid = InvalidOid;
    Oid heapOid = InvalidOid;
    Oid heapPartOid = InvalidOid;
    LOCKMODE lockmode;
    Relation irel;

    /* lock level used here should match index lock reindex_index() */
    if (partition_name != NULL)
        lockmode = AccessShareLock;
    else
        lockmode = concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;

    state.concurrent = concurrent;
    state.locked_table_oid = heapOid;
    indOid = RangeVarGetRelidExtended(indexRelation,
        lockmode,
        false,
        false,
        partition_name != NULL,
        false,
        RangeVarCallbackForReindexIndex,
        (void*)&state);

    TrForbidAccessRbObject(RelationRelationId, indOid, indexRelation->relname);

    /*
     * Obtain the current persistence of the existing index.  We already hold
     * lock on the index.
     */
    irel = index_open(indOid, NoLock);
    index_close(irel, NoLock);

    if (partition_name != NULL)
        indPartOid = PartitionNameGetPartitionOid(indOid,
            partition_name,
            PART_OBJ_TYPE_INDEX_PARTITION,
            concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock,  // lock on index partition
            false,
            false,
            PartitionNameCallbackForIndexPartition,
            (void*)&heapPartOid,
            concurrent ? ShareUpdateExclusiveLock : ShareLock);  // lock on heap partition
    
    if (concurrent)
        ReindexRelationConcurrently(indOid, indPartOid, mem_info, false);
    else {
        reindex_index(indOid, indPartOid, false, mem_info, false);
        Oid relId = IndexGetRelation(indOid, false);
        if (RelationIsCUFormatByOid(relId) && irel->rd_index != NULL && irel->rd_index->indisunique) {
            /*
             * Unique index on CU owns a unique index on delta table, but delta index is not visble
             * to user. We reindex delta index manually.
             */
            ReindexDeltaIndex(indOid, indPartOid);
        }
    }
    return indOid;
}

void PartitionNameCallbackForIndexPartition(Oid partitionedRelationOid, const char* partitionName, Oid partId,
    Oid oldPartId, char partition_type, void* arg, LOCKMODE callbackobj_lockMode)
{
    char relkind;
    Oid* heapPartOid = (Oid*)arg;
    Oid heapOid = InvalidOid;

    heapOid = IndexGetRelation(partitionedRelationOid, false);
    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    if (partId != oldPartId && OidIsValid(oldPartId)) {
        /* lock level here should match reindex_index() heap lock */
        /* the lock on heap is held by calling RangeVarCallbackForReindexIndex() */
        UnlockPartition(heapOid, *heapPartOid, callbackobj_lockMode, PARTITION_LOCK);
        *heapPartOid = InvalidOid;
    }

    /* If the relation does not exist, there's nothing more to do. */
    if (!OidIsValid(partId) || !OidIsValid(partitionedRelationOid))
        return;

    /*
     * If the relation does exist, check whether it's an index.  But note that
     * the relation might have been dropped between the time we did the name
     * lookup and now.	In that case, there's nothing to do.
     */
    relkind = get_rel_relkind(partitionedRelationOid);
    if (!relkind) {
        return;
    }
    if (relkind != RELKIND_INDEX && relkind != RELKIND_GLOBAL_INDEX)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not an index", partitionName)));
    if (0 != memcmp(partitionName, getPartitionName(partId, false), strlen(partitionName)))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" does not mean oid \"%u\"", partitionName, partId)));
    if (partitionedRelationOid != partid_get_parentid(partId))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%u\" is not a child of \"%u\"", partId, partitionedRelationOid)));

    /* Check permissions */
    Oid tableOid = IndexGetRelation(partitionedRelationOid, false);
    AclResult aclresult = pg_class_aclcheck(tableOid, GetUserId(), ACL_INDEX);
    if (aclresult != ACLCHECK_OK && !pg_class_ownercheck(partitionedRelationOid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, partitionName);
    }

    /* Lock heap before index to avoid deadlock. */
    if (partId != oldPartId) {
        /*
         * Lock level here should match reindex_index() heap lock. If the OID
         * isn't valid, it means the index as concurrently dropped, which is
         * not a problem for us; just return normally.
         */
        *heapPartOid = indexPartGetHeapPart(partId, true);
        if (OidIsValid(*heapPartOid)) {
            LockPartition(heapOid, *heapPartOid, callbackobj_lockMode, PARTITION_LOCK);
        }
    }
}

/*
 * Check permissions for index.
 */
static void PermissionCheckForReindexIndex(const RangeVar* relation, Oid relId)
{
    if (pg_class_ownercheck(relId, GetUserId())) {
        return;
    }

    Oid tableoid = IndexGetRelation(relId, false);
    if (pg_class_aclcheck(tableoid, GetUserId(), ACL_INDEX) == ACLCHECK_OK) {
        return;
    }

    if (!IsSysSchema(GetNamespaceIdbyRelId(tableoid))) {
        if (HasSpecAnyPriv(GetUserId(), ALTER_ANY_INDEX, false)) {
            return;
        }
    }

    aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, relation->relname);
}

/*
 * Check permissions on table before acquiring relation lock; also lock
 * the heap before the RangeVarGetRelidExtended takes the index lock, to avoid
 * deadlocks.
 */
static void RangeVarCallbackForReindexIndex(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* arg)
{
    char relkind;
    struct ReindexIndexCallbackState* state = (struct ReindexIndexCallbackState*)arg;

    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    if (relId != oldRelId && OidIsValid(oldRelId)) {
        /* lock level here should match reindex_index() heap lock */
        if (target_is_partition) {
            UnlockRelationOid(state->locked_table_oid, AccessShareLock);
        } else if (state->concurrent) {
            UnlockRelationOid(state->locked_table_oid, ShareUpdateExclusiveLock);
        } else {
            UnlockRelationOid(state->locked_table_oid, ShareLock);
        }
        state->locked_table_oid = InvalidOid;
    }

    /* If the relation does not exist, there's nothing more to do. */
    if (!OidIsValid(relId))
        return;

    /*
     * If the relation does exist, check whether it's an index.  But note that
     * the relation might have been dropped between the time we did the name
     * lookup and now.	In that case, there's nothing to do.
     */
    relkind = get_rel_relkind(relId);
    if (!relkind) {
        return;
    }
    if (relkind != RELKIND_INDEX && relkind != RELKIND_GLOBAL_INDEX)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not an index", relation->relname)));

    if (target_is_partition && relkind == RELKIND_GLOBAL_INDEX) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot reindex global index with partition name")));
    }

    /* Check permissions */
    PermissionCheckForReindexIndex(relation, relId);

    /* Lock heap before index to avoid deadlock. */
    if (relId != oldRelId) {
        /*
         * Lock level here should match reindex_index() heap lock. If the OID
         * isn't valid, it means the index as concurrently dropped, which is
         * not a problem for us; just return normally.
         */
        state->locked_table_oid = IndexGetRelation(relId, true);
        if (OidIsValid(state->locked_table_oid)) {
            if (target_is_partition) {
                LockRelationOid(state->locked_table_oid, AccessShareLock);
            } else if (state->concurrent) {
                LockRelationOid(state->locked_table_oid, ShareUpdateExclusiveLock);
            } else {
                LockRelationOid(state->locked_table_oid, ShareLock);
            }
        }
    }
}

/*
 * ReindexTable
 *		Recreate all indexes of a table (and of its toast table, if any)
 */
Oid ReindexTable(RangeVar* relation, const char* partition_name, AdaptMem* mem_info, bool concurrent)
{
    Oid heapOid;
    bool result;

    if (partition_name != NULL) {
        Oid heapPartOid;
        /* The lock level used here should match reindexPartition(). */
        heapOid = RangeVarGetRelidExtended(
            relation, AccessShareLock, false, false, false, false, RangeVarCallbackOwnsTable, NULL);
        Relation rel = heap_open(heapOid, AccessShareLock);
        if (RelationIsSubPartitioned(rel)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            (errmsg("Un-support feature"),
                             errdetail("For subpartition table, REBUILD UNUSABLE LOCAL INDEXES is not yet supported."),
                             errcause("The function is not implemented."), erraction("Use other actions instead."))));
        }
        heap_close(rel, NoLock);
        TrForbidAccessRbObject(RelationRelationId, heapOid, relation->relname);

        heapPartOid = PartitionNameGetPartitionOid(
            heapOid, partition_name, PART_OBJ_TYPE_TABLE_PARTITION, concurrent ? ShareUpdateExclusiveLock : ShareLock, false, false, NULL, NULL, NoLock);
        if(concurrent)
            ReindexRelationConcurrently(heapOid, heapPartOid, mem_info);
        else
            reindexPartition(heapOid,
                heapPartOid,
                REINDEX_REL_PROCESS_TOAST | REINDEX_REL_SUPPRESS_INDEX_USE | REINDEX_REL_CHECK_CONSTRAINTS,
                REINDEX_ALL_INDEX);
    } else {
        /* The lock level used here should match ReindexRelation(). */
        heapOid =
            RangeVarGetRelidExtended(relation, concurrent ? ShareUpdateExclusiveLock : ShareLock, false, false, false, false, RangeVarCallbackOwnsTable, NULL);
        if (concurrent)
            result = ReindexRelationConcurrently(heapOid, InvalidOid, mem_info);
        else
            result = ReindexRelation(heapOid,
            REINDEX_REL_PROCESS_TOAST | REINDEX_REL_SUPPRESS_INDEX_USE | REINDEX_REL_CHECK_CONSTRAINTS,
            REINDEX_ALL_INDEX, NULL, mem_info);
        
        if (!result)
            ereport(NOTICE, (errmsg("table \"%s\" has no indexes", relation->relname)));
    }
    return heapOid;
}

/*
 * ReindexInternal
 *		Recreate all indexes of its cudesc table(and of its toast table, if any)
 * @ in relation: the column_table|hdfs_table  is used to execute the operation of 'reindex internal table name'.
 * @ in partition_name: the  partition_table is used to execute the operation of 'reindex internal table name partition
 *partition_name'.
 */
Oid ReindexInternal(RangeVar* relation, const char* partition_name)
{
    Oid heapOid;
    Relation rel;
    Relation partitionRel;
    Partition part;
    const int flags = REINDEX_REL_PROCESS_TOAST | REINDEX_REL_SUPPRESS_INDEX_USE | REINDEX_REL_CHECK_CONSTRAINTS;

    heapOid = RangeVarGetRelidExtended(
        relation, AccessShareLock, false, false, false, false, RangeVarCallbackOwnsTable, NULL);

    TrForbidAccessRbObject(RelationRelationId, heapOid, relation->relname);

    rel = heap_open(heapOid, AccessShareLock);

    /* 1. judge the cstore table and reindex its cudesc table.
     * 2. judge the hdfs table and reindex its cudesc table.
     * 3. others,such as row table and hdfs foreign table are to report error.
     */
    if (RelationIsCUFormat(rel)) {
        if (partition_name != NULL) {
            Oid PartOid;
            /* The lock level used here should match reindexPartition(). */
            PartOid = PartitionNameGetPartitionOid(heapOid,
                partition_name,
                PART_OBJ_TYPE_TABLE_PARTITION,
                AccessShareLock,
                false,
                false,
                NULL,
                NULL,
                NoLock);
            part = partitionOpen(rel, PartOid, AccessShareLock);
            partitionRel = partitionGetRelation(rel, part);
            Oid cudescOid = partitionRel->rd_rel->relcudescrelid;

            if (!ReindexRelation(cudescOid, flags, REINDEX_ALL_INDEX, NULL)) {
                ereport(ERROR,
                    (errcode(ERRCODE_PARTITION_ERROR),
                        errmsg("The operation of 'REINDEX INTERNAL TABLE %s PARTITION %s' failed. ",
                            relation->relname,
                            partition_name)));
            }
            partitionClose(rel, part, NoLock);
            releaseDummyRelation(&partitionRel);
        } else {
            if (RELATION_IS_PARTITIONED(rel)) {
                Oid partOid;
                ListCell* cell = NULL;
                List* partOidList = relationGetPartitionOidList(rel);
                foreach (cell, partOidList) {
                    partOid = lfirst_oid(cell);
                    part = partitionOpen(rel, partOid, AccessShareLock);
                    partitionRel = partitionGetRelation(rel, part);
                    Oid cudescOid = partitionRel->rd_rel->relcudescrelid;

                    if (!ReindexRelation(cudescOid, flags, REINDEX_ALL_INDEX, NULL)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_PARTITION_ERROR),
                                errmsg("The operation of 'REINDEX INTERNAL TABLE %s' on part \"%u\" failed. ",
                                    relation->relname,
                                    partOid)));
                    }
                    partitionClose(rel, part, NoLock);
                    releaseDummyRelation(&partitionRel);
                }

            } else {
                Oid cudescOid = rel->rd_rel->relcudescrelid;

                if (!ReindexRelation(cudescOid, flags, REINDEX_ALL_INDEX, NULL)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("The operation of 'REINDEX INTERNAL TABLE %s' failed. ", relation->relname)));
                }
            }
        }
    } else if (RelationIsPAXFormat(rel)) {
        Oid cudescOid = rel->rd_rel->relcudescrelid;
        if (!ReindexRelation(cudescOid, flags, REINDEX_ALL_INDEX, NULL)) {
            ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("The operation of 'REINDEX INTERNAL TABLE %s' failed. ", relation->relname)));
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("The table \"%s\" doesn't support the operation of 'REINDEX INTERNAL TABLE'. There is no Desc "
                       "table on it.",
                    relation->relname)));
    }

    heap_close(rel, NoLock);
    return heapOid;
}

/*
 * ReindexDatabase
 *		Recreate indexes of a database.
 *
 * To reduce the probability of deadlocks, each table is reindexed in a
 * separate transaction, so we can release the lock on it right away.
 * That means this must not be called within a user transaction block!
 */
Oid ReindexDatabase(const char* databaseName, bool do_system, bool do_user, AdaptMem* mem_info, bool concurrent)
{
    Relation relationRelation;
    TableScanDesc scan;
    HeapTuple tuple;
    MemoryContext private_context;
    MemoryContext old;
    List* relids = NIL;
    ListCell* l = NULL;

    AssertArg(databaseName);

    if (strcmp(databaseName, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId)) != 0)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can only reindex the currently open database")));

    if (!pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE, databaseName);

    /*
     * Create a memory context that will survive forced transaction commits we
     * do below.  Since it is a child of t_thrd.mem_cxt.portal_mem_cxt, it will go away
     * eventually even if we suffer an error; there's no need for special
     * abort cleanup logic.
     */
    private_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt,
        "ReindexDatabase",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * We always want to reindex pg_class first.  This ensures that if there
     * is any corruption in pg_class' indexes, they will be fixed before we
     * process any other tables.  This is critical because reindexing itself
     * will try to update pg_class. But reindex concurrent don't update pg_class.
     */
    if (do_system && !concurrent) {
        old = MemoryContextSwitchTo(private_context);
        relids = lappend_oid(relids, RelationRelationId);
        MemoryContextSwitchTo(old);
    }

    /*
     * Scan pg_class to build a list of the relations we need to reindex.
     *
     * We only consider plain relations here (toast rels will be processed
     * indirectly by ReindexRelation).
     */
    relationRelation = heap_open(RelationRelationId, AccessShareLock);
    scan = tableam_scan_begin(relationRelation, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class classtuple = (Form_pg_class)GETSTRUCT(tuple);

        if (classtuple->relkind != RELKIND_RELATION && classtuple->relkind != RELKIND_MATVIEW)
            continue;

        /* Skip temp tables of other backends; we can't reindex them at all */
        if (classtuple->relpersistence == RELPERSISTENCE_TEMP && !isTempNamespace(classtuple->relnamespace))
            continue;

        /* Check user/system classification, and optionally skip */
        if (IsSystemClass(classtuple)) {
            if (!do_system)
                continue;
        } else {
            if (!do_user)
                continue;
        }

        if (HeapTupleGetOid(tuple) == RelationRelationId)
            continue; /* got it already */

        /* Skip object in recycle bin. */
        if (TrIsRefRbObjectEx(RelationRelationId, HeapTupleGetOid(tuple), NameStr(classtuple->relname))) {
            continue;
        }
        /*
         * Skip system tables that index_create() would reject to index
         * concurrently, XXX We need the additional check for
         * FirstNormalObjectId to skip information_schema tables, because
         * IsCatalogClass() here does not cover information_schema, but the
         * check in index_create() will error on the TOAST tables of
         * information_schema tables.
         */
        if (concurrent &&
            (IsCatalogClass(HeapTupleGetOid(tuple), classtuple) || HeapTupleGetOid(tuple) < FirstNormalObjectId)) {
                ereport(WARNING,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("table \"%s.%s\" is catalog relation and concurrent reindex is not supported ",
                        get_namespace_name(get_rel_namespace(HeapTupleGetOid(tuple))),
                        get_rel_name(HeapTupleGetOid(tuple)))));         
                continue;
        }

        /* Temp relation doesn't support concurrent REINDEX now. */
        if (concurrent && (classtuple->relpersistence == RELPERSISTENCE_GLOBAL_TEMP || classtuple->relpersistence == RELPERSISTENCE_TEMP)) {
            ereport(WARNING,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("table \"%s.%s\" is TEMP relation and concurrent reindex is not supported ",
                    get_namespace_name(get_rel_namespace(HeapTupleGetOid(tuple))),
                    get_rel_name(HeapTupleGetOid(tuple)))));
            continue;
        }
        
        /*  cstore relation doesn't support concurrent REINDEX now. */
        if (concurrent && (RelationIsCUFormatByOid(HeapTupleGetOid(tuple)) || IsCStoreNamespace(classtuple->relnamespace))) {
            ereport(WARNING,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("table \"%s.%s\" is column store table and concurrent reindex is not supported ",
                    get_namespace_name(get_rel_namespace(HeapTupleGetOid(tuple))),
                    get_rel_name(HeapTupleGetOid(tuple)))));

            continue;
        }

        /* ustore relation doesn't support concurrent REINDEX now. */
        if (concurrent && RelationIsUStoreFormatByOid(HeapTupleGetOid(tuple))) {
            ereport(WARNING,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("table \"%s.%s\" is ustore table and concurrent reindex is not supported ",
                    get_namespace_name(get_rel_namespace(HeapTupleGetOid(tuple))),
                    get_rel_name(HeapTupleGetOid(tuple)))));
            continue;
        }

        old = MemoryContextSwitchTo(private_context);
        relids = lappend_oid(relids, HeapTupleGetOid(tuple));
        MemoryContextSwitchTo(old);
    }
    tableam_scan_end(scan);
    heap_close(relationRelation, AccessShareLock);

    /* Now reindex each rel in a separate transaction */
    PopActiveSnapshot();
    CommitTransactionCommand();
    foreach (l, relids) {
        Oid relid = lfirst_oid(l);
        bool result = false;

        StartTransactionCommand();
        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());
        PG_TRY();
        {
            if (concurrent)
                result = ReindexRelationConcurrently(relid, InvalidOid, mem_info, true);
            else
                result = ReindexRelation(relid, REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS, 
				REINDEX_ALL_INDEX, NULL, mem_info, true); 
            if (result)
                ereport(NOTICE,
                    (errmsg("table \"%s.%s\" was reindexed",
                        get_namespace_name(get_rel_namespace(relid)),
                        get_rel_name(relid))));
        }
        PG_CATCH();
        {
            if (geterrcode() == ERRCODE_RELATION_OPEN_ERROR && NULL == get_rel_name(relid)) {
                ereport(LOG,
                    (errmsg("Table with OID \"%u\" doesn't exit.", relid),
                        errhint("Please don't drop table when the operation of 'REINDEX DATABASE' is executed.")));

                FlushErrorState();
            } else {
                PG_RE_THROW();
            }
        }
        PG_END_TRY();

        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    StartTransactionCommand();

    MemoryContextDelete(private_context);
    return u_sess->proc_cxt.MyDatabaseId;
}

/*
 * check table relation supports concurrent reindex
 */
static void checkTableForReindexConcurrently(Relation heapRelation) {
    Oid heapId = heapRelation->rd_id;
    char relPersistence = get_rel_persistence(heapId);

    if (IsSharedRelation(heapId)) 
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not supported for share relation")));

    if (IsCatalogRelation(heapRelation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not supported for system catalog relations")));

    if (RelationIsCUFormatByOid(heapId) || IsCStoreNamespace(get_rel_namespace(heapId)))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not support for column store table")));

    if (relPersistence == RELPERSISTENCE_TEMP || relPersistence == RELPERSISTENCE_GLOBAL_TEMP)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not supported for TEMP table")));

    if (RelationIsUstoreFormat(heapRelation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not supported for ustore table")));

    if (RelationIsSubPartitioned(heapRelation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("concurrent reindex is not supported for subpartitioned table")));
}

/*
 * check index relation supports concurrent reindex
 * reindexIndex=false, this function is used in reindex table concurrently
 * reindexIndex=true, this function is used in reindex index concurrently
 * reindex table concurrently can't reindex invalid index
 * reindex index concurrently will make invalid index valid exclude toast index
 */
static bool checkIndexForReindexConcurrently(Relation indexRelation, bool reindexIndex)
{
    int errorType;
    if (reindexIndex)
        errorType = ERROR;
    else
        errorType = WARNING;

    if (!reindexIndex && !IndexIsValid(indexRelation->rd_index)) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot reindex concurrently invalid index \" %s.%s\", skipping",
                get_namespace_name(get_rel_namespace(indexRelation->rd_id)),
                get_rel_name(indexRelation->rd_id))));
        return false;
    }

    if (reindexIndex && IsToastNamespace(get_rel_namespace(indexRelation->rd_id)) && !IndexIsValid(indexRelation->rd_index))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("cannot reindex concurrently invalid index \"%s.%s\" on TOAST table",
                get_namespace_name(get_rel_namespace(indexRelation->rd_id)),
                get_rel_name(indexRelation->rd_id))));


    if (indexRelation->rd_index->indisexclusion) {
        ereport(errorType, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot reindex concurrently exclusion constraint index \" %s.%s.\"%s",
                get_namespace_name(get_rel_namespace(indexRelation->rd_id)),
                get_rel_name(indexRelation->rd_id),
                reindexIndex ? "" : ", skipping")));
        return false;
    }

    if (RelationIsGlobalIndex(indexRelation)) {
        ereport(errorType, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot reindex concurrently global partition index \" %s.%s\"%s",
                get_namespace_name(get_rel_namespace(indexRelation->rd_id)),
                get_rel_name(indexRelation->rd_id),
                reindexIndex ? "" : ", skipping")));
        return false;
    }

    return true;
}

/*
 * check index partition supports reindex in reindex table concurrently 
 */
static bool checkIndexPartitionForReindexConcurrently(Relation indexRelation, Oid indexPartitionOid) 
{
    bool isusable = true;

    if (!OidIsValid(indexPartitionOid)) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot reindex concurrently invalid partition index oid %u in index %u, skipping",
                        indexPartitionOid, indexRelation->rd_id)));
        return false;
    }

    Partition indexPartition = partitionOpen(indexRelation, indexPartitionOid, ShareUpdateExclusiveLock);

    if (!indexPartition->pd_part->indisusable) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot reindex concurrently inusable partition index \"%s\" in index \" %s\", skipping",
                getPartitionName(indexPartitionOid, false),
                get_rel_name(indexRelation->rd_id))));
        isusable = false;
    } 

    partitionClose(indexRelation, indexPartition, NoLock);

    return isusable;
}

/*
 * get toast oids from relation or partition
 */
static List* getToastOidsInReindexConcurrently(Relation heapRelation, Oid heapPartitionId)
{
    List* relToastOids = NIL;
    Oid heapId = heapRelation->rd_id;

    if (RelationIsPartitioned(heapRelation)) {
        if (OidIsValid(heapPartitionId)) {
            Oid toastOid = InvalidOid;
            HeapTuple partitionTup;
            Relation pg_partition;

            pg_partition = heap_open(PartitionRelationId, AccessShareLock);
            partitionTup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(heapPartitionId));

            if (!HeapTupleIsValid(partitionTup)) {
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for partition %u", heapPartitionId)));
            }

            toastOid = ((Form_pg_partition)GETSTRUCT(partitionTup))->reltoastrelid;
            if(OidIsValid(toastOid)) {
                relToastOids = lappend_oid(relToastOids, toastOid);
            }
            ReleaseSysCache(partitionTup);
            heap_close(pg_partition, AccessShareLock);
        } else {
            List* partTupleList = NIL;
            ListCell* partCell = NULL;

            partTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, heapId);

            foreach (partCell, partTupleList) {
                Oid toastOid = ((Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(partCell)))->reltoastrelid;

                if(OidIsValid(toastOid)) {
                    relToastOids = lappend_oid(relToastOids, toastOid);
                }
            }

            freePartList(partTupleList);
        }
    } else {
        if (OidIsValid(heapRelation->rd_rel->reltoastrelid)) {
            Oid toastOid = heapRelation->rd_rel->reltoastrelid;
            relToastOids = lappend_oid(relToastOids, toastOid);
        }
    }

    return relToastOids;
}

/*
 * perpare reindex table concurrently
 * In the case of a relation or partition. find all its indexes 
 * or all its partition indexes, include toast indexes
 */
static void prepareReindexTableConcurrently(Oid relationOid, Oid relationPartOid, List** rt_heapRelationIds,
    List** rt_heapPartitionIds, List** rt_indexIds, List** rt_indexPartIds, MemoryContext private_context)
{
    Relation heapRelation;
    List* heapRelationIds = NIL;
    List* heapPartitionIds = NIL;
    List* indexIds = NIL;
    List* indexPartIds = NIL;
    List* relToastOids = NIL;
    ListCell* lc;
    ListCell* lc2;
    MemoryContext oldcontext;

    /* open relation to get its indexes */
    heapRelation = heap_open(relationOid, ShareUpdateExclusiveLock);

    checkTableForReindexConcurrently(heapRelation);

    /* get add interval partition lock, unlock after transaction commit */
    if (RelationIsPartitioned(heapRelation) && heapRelation->partMap->type == PART_TYPE_INTERVAL)
        LockPartitionObject(heapRelation->rd_id, INTERVAL_PARTITION_LOCK_SDEQUENCE, PARTITION_EXCLUSIVE_LOCK);

    /* Save the list of relation OIDs in private context */
    oldcontext = MemoryContextSwitchTo(private_context);

    /* Track this Relation for session lock */
    heapRelationIds = lappend_oid(heapRelationIds, relationOid);

    if (RelationIsPartitioned(heapRelation)) {
        if (OidIsValid(relationPartOid))
            heapPartitionIds = lappend_oid(heapPartitionIds, relationPartOid);
        else
            heapPartitionIds = relationGetPartitionOidList(heapRelation);
    }

    MemoryContextSwitchTo(oldcontext);

    /* Add all the valid indexes of relation to list */
    foreach (lc, RelationGetIndexList(heapRelation)) {
        Oid cellOid = lfirst_oid(lc);
        Relation indexRelation = index_open(cellOid, ShareUpdateExclusiveLock);

        if (!checkIndexForReindexConcurrently(indexRelation, false))
            continue;
        
        if (RelationIsPartitioned(indexRelation)) {
            if (OidIsValid(relationPartOid)) {
                Oid indexPartitionOid = InvalidOid;

                indexPartitionOid = indexIdAndPartitionIdGetIndexPartitionId(cellOid, relationPartOid);

                if (checkIndexPartitionForReindexConcurrently(indexRelation, indexPartitionOid)) {
                    /* Save the list of partition OIDs in private context */
                    oldcontext = MemoryContextSwitchTo(private_context);

                    indexPartIds = lappend_oid(indexPartIds, indexPartitionOid);
                    
                    MemoryContextSwitchTo(oldcontext);
                }

            } else {
                List* indexPartOidList = NULL;
                ListCell* partCell = NULL;
                bool nowarning = true;

                indexPartOidList = indexGetPartitionOidList(indexRelation);
                foreach (partCell, indexPartOidList) {
                    Oid indexPartitionOid = lfirst_oid(partCell);

                    if (!checkIndexPartitionForReindexConcurrently(indexRelation, indexPartitionOid)) {
                        nowarning = false;
                        break;
                    }
                }

                if (!nowarning)
                    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot reindex concurrently index \" %s.%s\", skipping", 
                            get_namespace_name(get_rel_namespace(cellOid)),
                            get_rel_name(cellOid))));
                else {
                    /* Save the list of relation OIDs in private context */
                    oldcontext = MemoryContextSwitchTo(private_context);

                    indexIds = lappend_oid(indexIds, cellOid);

                    MemoryContextSwitchTo(oldcontext);
                }
            }
        } else {
            /* Save the list of relation OIDs in private context */
            oldcontext = MemoryContextSwitchTo(private_context);

            indexIds = lappend_oid(indexIds, cellOid);

            MemoryContextSwitchTo(oldcontext);
        }
        
        index_close(indexRelation, NoLock);
    }

    /* Also add the toast indexes */
    relToastOids = getToastOidsInReindexConcurrently(heapRelation,relationPartOid);

    foreach (lc, relToastOids) {
        Oid toastOid = lfirst_oid(lc);

        Relation toastRelation = heap_open(toastOid, ShareUpdateExclusiveLock);

        /* Save the list of relation OIDs in private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        /* Track the relation for session lock */
        heapRelationIds = lappend_oid(heapRelationIds, toastOid);

        MemoryContextSwitchTo(oldcontext);

        foreach(lc2, RelationGetIndexList(toastRelation)){
            Oid cellOid = lfirst_oid(lc2);
            Relation indexRelation = index_open(cellOid, ShareUpdateExclusiveLock);

            if (!IndexIsValid(indexRelation->rd_index))
                ereport(WARNING, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("cannot reindex concurrently invalid index \"%s.%s\", skipping",
                        get_namespace_name(get_rel_namespace(cellOid)),
                        get_rel_name(cellOid))));
            else {
                /* Save the list of relation OIDs in private context */
                oldcontext = MemoryContextSwitchTo(private_context);

                indexIds = lappend_oid(indexIds, cellOid);

                MemoryContextSwitchTo(oldcontext);
            }
            index_close(indexRelation, NoLock);
        }

        heap_close(toastRelation, NoLock);
    }

    heap_close(heapRelation, NoLock);

    *rt_heapRelationIds = heapRelationIds;
    *rt_heapPartitionIds = heapPartitionIds;
    *rt_indexIds = indexIds;
    *rt_indexPartIds = indexPartIds;
}

/*
 * prepare reindex index concurrently 
 * If partition Oid is valid, add its to list,
 * also, add an index Oid to list.
 */
static void prepareReindexIndexConcurrently(Oid relationOid, Oid relationPartOid, List** rt_heapRelationIds,
    List** rt_heapPartitionIds, List** rt_indexIds, List** rt_indexPartIds, MemoryContext private_context)
{
    Relation indexRelation = index_open(relationOid, ShareUpdateExclusiveLock);
    Oid heapId = IndexGetRelation(relationOid, false);
    Relation heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
    List* heapRelationIds = NIL;
    List* heapPartitionIds = NIL;
    List* indexIds = NIL;
    List* indexPartIds = NIL;
    MemoryContext oldcontext;

	checkTableForReindexConcurrently(heapRelation);

    /* Add all the valid indexes of relation to list */
    if (RelationIsPartitioned(heapRelation) && heapRelation->partMap->type == PART_TYPE_INTERVAL)
        LockPartitionObject(heapRelation->rd_id, INTERVAL_PARTITION_LOCK_SDEQUENCE, PARTITION_EXCLUSIVE_LOCK);

    checkIndexForReindexConcurrently(indexRelation, true);

    if (RelationIsPartitioned(indexRelation)) {
        if (OidIsValid(relationPartOid)) {
            Partition indexPartition = partitionOpen(indexRelation, relationPartOid, ShareUpdateExclusiveLock);

            Oid heapPartitionId = PartIndexGetPartition(relationPartOid, false);
            /* Save the list of relation OIDs in private context */
            oldcontext = MemoryContextSwitchTo(private_context);

            heapRelationIds = list_make1_oid(heapId);
            heapPartitionIds = list_make1_oid(heapPartitionId);
            indexPartIds = lappend_oid(indexPartIds, relationPartOid);

            MemoryContextSwitchTo(oldcontext);
        
            partitionClose(indexRelation, indexPartition, ShareUpdateExclusiveLock);
        } else {
            List* indexPartOidList = NULL;
            ListCell* partCell = NULL;
            bool hasinvalid = false;

            indexPartOidList = indexGetPartitionOidList(indexRelation);
            foreach (partCell, indexPartOidList) {
                Oid indexPartOid = lfirst_oid(partCell);

                if (!OidIsValid(indexPartOid)) {
                    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot reindex concurrently invalid partition index oid %u in index %u",
                            indexPartOid, relationOid)));
                    hasinvalid = true;
                    break;
                }
            }

            if (hasinvalid)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot reindex concurrently index \" %s.%s\"", 
                        get_namespace_name(get_rel_namespace(relationOid)),
                        get_rel_name(relationOid))));
            else {
                /* Save the list of relation OIDs in private context */
                oldcontext = MemoryContextSwitchTo(private_context);

                heapRelationIds = list_make1_oid(heapId);
                heapPartitionIds = relationGetPartitionOidList(heapRelation);
                indexIds = lappend_oid(indexIds, relationOid);

                MemoryContextSwitchTo(oldcontext);
            }
        }   
    } else {
        /* Save the list of relation OIDs in private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        heapRelationIds = list_make1_oid(heapId);
        indexIds = lappend_oid(indexIds, relationOid);

        MemoryContextSwitchTo(oldcontext);
    }

    index_close(indexRelation, NoLock);
    heap_close(heapRelation, NoLock);

    *rt_heapRelationIds = heapRelationIds;
    *rt_heapPartitionIds = heapPartitionIds;
    *rt_indexIds = indexIds;
    *rt_indexPartIds = indexPartIds;
}

/*
 * ReindexRelationConcurrently - process REINDEX CONCURRENTLY for given
 * relation OID
 * 
 * The relation can be either an index or a table. If it is a table, all its
 * valid indexes will be rebuilt, including its associated toast table
 * indexes. If it is an index, this index itself will be rebuit.
 * 
 * If it is a partition table, and partition OID is valid, all valid index
 * partitions in the partition will be rebuilt, partition associated toast 
 * table indexes will be rebuilt too. 
 * 
 * If is a partition table, and partition OID is invalid, all valid index 
 * partitions in the partitions of partition table will be rebuilt, partitions
 * associated toast table indexes will be rebuilt too. 
 * 
 * If it is an index, and partition OID is valid, only this index partition
 * will be rebuilt. if partition OID is invalid, all index partitions of index
 * will be rebuilt.
 * 
 * The locks taken on parent tables and involved indexes are kept until the
 * transaction is committed, at which point a session lock is taken on each 
 * relation. Both of these protect against concurrent schema changes.
 */
static bool ReindexRelationConcurrently(Oid relationOid, Oid relationPartOid, AdaptMem* memInfo, bool dbWide)
{
    List* heapRelationIds = NIL;
    List* heapPartitionIds = NIL;
    List* indexIds = NIL;
    List* newIndexIds = NIL;
    List* indexPartIds = NIL;
    List* newIndexPartIds = NIL;
    List* relationLocks = NIL;
    List* partitionLocks = NIL;
    List* lockTags = NIL;
    ListCell* lc; 
    ListCell* lc2;
    MemoryContext private_context;
    MemoryContext oldcontext;
    char relkind;
    VirtualTransactionId* old_lockholders = NULL;

    /*
     * Create a memory context that will survive forced transaction commits we
     * do below. Since it is a child of t_thrd.mem_cxt.portal_mem_cxt, it will go away
     * eventually even if we suffer an error: there's no need for special
     * abort cleanup logic.
     */
    private_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt, "ReindexConcurrent",
                    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    relkind = get_rel_relkind(relationOid);

    switch (relkind) {
        case RELKIND_RELATION :
        case RELKIND_MATVIEW :
        case RELKIND_TOASTVALUE :
        {
            prepareReindexTableConcurrently(relationOid, relationPartOid, &heapRelationIds, &heapPartitionIds, &indexIds, &indexPartIds, private_context);
            break;
        }
        case RELKIND_INDEX:
        {
            prepareReindexIndexConcurrently(relationOid, relationPartOid, &heapRelationIds, &heapPartitionIds, &indexIds, &indexPartIds, private_context);
            break;
        }

        case RELKIND_GLOBAL_INDEX:
        {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot reindex concurrently global partition index \" %s.%s\"",
                    get_namespace_name(get_rel_namespace(relationOid)),
                    get_rel_name(relationOid))));
        }

        default:
            /* return error if the typr of relation is not supported*/
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot reindex concurrently this type of relation")));
            break;
    }

    /* Definitely no indexes, so leave */
    if (indexIds == NIL && indexPartIds == NIL) {
        PopActiveSnapshot();
        return false;
    }

    Assert(heapRelationIds != NIL || heapPartitionIds != NIL);

    /*-----
     * Now we have all the indexes we want to process in indexIds.
     * 
     * The phases of reindex concurrently indexes now are:
     * 
     * 1. create new indexes in the catalog
     * 2. build new indexes
     * 3. let new indexes catch up with tuples inserted in the meantime
     * 4. swap index names
     * 5. mark old indexes as dead
     * 6. drop old indexes
     * 
     * The phases of reindex concurrently index partition now are:
     * 
     * 1. create new partitioned indexes and new index partitions in the catalog
     * 2. build new index partitions
     * 3. let new index partitions catch up with tuples inserted in the meantime
     * 4. swap index partition names and parentids, so that old partitioned index has new index partitions
     *    and new partitioned index has old index partitions.
     * 5. mark new partitioned indexes as dead because they have old index partitiones
     * 6. drop new partitioned indexes
     * 
     * We process each phase for all indexes before moving to the next phase,
     * for efficiency.
     */

    /*
     * Phase 1 of REINDEX CONCURRENTLY
     * Create a new index with the same properties as the old one, but it is
     * only registed in catalogs and will be built later. Then get session
     * locks on all involved tables. See analogous code in DefineIndex() for
     * more detailed comments.
     * If index is partitioned, create index partitions too.
     */

    foreach (lc, indexIds) {
        char* concurrentName;
        Oid indexId = lfirst_oid(lc);
        Oid newIndexId;
        Relation indexRel;
        Relation heapRel;
        Relation newIndexRel;
        LockRelId* lockrelid;
        LOCKTAG* locktag;

        indexRel = index_open(indexId, ShareUpdateExclusiveLock);
        heapRel = heap_open(indexRel->rd_index->indrelid, ShareUpdateExclusiveLock);

        /* Choose a temporary relation name for the new index */
        concurrentName = ChooseRelationName(get_rel_name(indexId), NULL, "ccnew", 5, 
                                            get_rel_namespace(indexRel->rd_index->indrelid),
                                            false);

        /* Create new index definition based on given index */
        newIndexId = index_concurrently_create_copy(heapRel, indexId, InvalidOid, concurrentName);

        /* Now open the relation of the new index, a lock is also needed on it */
        newIndexRel = index_open(newIndexId, ShareUpdateExclusiveLock);

        /* Save the list of OIDs and locks in Private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        newIndexIds = lappend_oid(newIndexIds, newIndexId);

        /*
         * Save lockrelid to protect each relation from drop then close
         * relations. The lockrelid on parent relation is not taken here to
         * avoid mutiple locks taken on the same relation, instead we rely on
         * parentRelationIds built earlier.
         */
        lockrelid = (LockRelId*)palloc(sizeof(LockRelId));
        *lockrelid  = indexRel->rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid);
        lockrelid = (LockRelId*)palloc(sizeof(LockRelId));
        *lockrelid  = newIndexRel->rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid);

        if (RelationIsPartitioned(heapRel)) {
            List* indexPartOidList = NULL;
            ListCell* partCell = NULL;

            indexPartOidList = indexGetPartitionOidList(indexRel);
            foreach (partCell, indexPartOidList) {
                Oid indexPartOid = lfirst_oid(partCell);
                Partition indexpartition = partitionOpen(indexRel, indexPartOid, ShareUpdateExclusiveLock); 


                locktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));
                SET_LOCKTAG_PARTITION(*locktag, indexpartition->pd_lockInfo.lockRelId.dbId,
                                    indexId, indexpartition->pd_lockInfo.lockRelId.relId);
                partitionLocks = lappend(partitionLocks, locktag);

                partitionClose(indexRel, indexpartition, NoLock);
            }

            indexPartOidList = indexGetPartitionOidList(newIndexRel);
            foreach (partCell, indexPartOidList) {
                Oid indexPartOid = lfirst_oid(partCell);
                Partition indexpartition = partitionOpen(newIndexRel, indexPartOid, ShareUpdateExclusiveLock); 


                locktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));
                SET_LOCKTAG_PARTITION(*locktag, indexpartition->pd_lockInfo.lockRelId.dbId,
                                    newIndexId, indexpartition->pd_lockInfo.lockRelId.relId);
                partitionLocks = lappend(partitionLocks, locktag);

                partitionClose(newIndexRel, indexpartition, NoLock);
            }
        }

        MemoryContextSwitchTo(oldcontext);

        index_close(indexRel, NoLock);
        index_close(newIndexRel, NoLock);
        heap_close(heapRel, NoLock);
    }

    foreach (lc, indexPartIds) {
        char* concurrentName;
        Oid indexPartId = lfirst_oid(lc);
        Oid newIndexPartId;
        Oid indexId;
        Relation indexRelation;
        Relation heapRelation;
        Partition indexPartition;
        Partition newIndexPartition;
        LockRelId* lockrelid;
        LOCKTAG* locktag;

        indexId = PartIdGetParentId(indexPartId, false);
        indexRelation = index_open(indexId, ShareUpdateExclusiveLock);
        heapRelation = heap_open(indexRelation->rd_index->indrelid, ShareUpdateExclusiveLock);
        indexPartition = partitionOpen(indexRelation, indexPartId, ShareUpdateExclusiveLock);

        /* Choose a temporary relation name for the new index */
        concurrentName = ChooseRelationName(get_rel_name(indexId), NULL, "ccnew", 5, 
                                            get_rel_namespace(indexRelation->rd_index->indrelid),
                                            false);

        /* Create new index definition based on given index */
        newIndexPartId = index_concurrently_create_copy(heapRelation, indexId, indexPartId, concurrentName);

        Oid newIndexId = PartIdGetParentId(newIndexPartId, false);
        Relation newIndexRelation = index_open(newIndexId, ShareUpdateExclusiveLock);

        /* Now open the relation of the new index, a lock is also needed on it */
        newIndexPartition = partitionOpen(newIndexRelation, newIndexPartId, ShareUpdateExclusiveLock);

        /* Save the list of OIDs and locks in Private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        newIndexPartIds = lappend_oid(newIndexPartIds, newIndexPartId);

        /*
         * Save lockrelid to protect each relation from drop then close
         * relations. The lockrelid on parent relation is not taken here to
         * avoid mutiple locks taken on the same relation, instead we rely on
         * parentRelationIds built earlier.
         */
        lockrelid = (LockRelId*)palloc(sizeof(LockRelId));
        *lockrelid  = indexRelation->rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid);
        lockrelid = (LockRelId*)palloc(sizeof(LockRelId));
        *lockrelid  = newIndexRelation->rd_lockInfo.lockRelId;
        relationLocks = lappend(relationLocks, lockrelid);

        locktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));
        SET_LOCKTAG_PARTITION(*locktag, indexPartition->pd_lockInfo.lockRelId.dbId,
                             indexId, indexPartition->pd_lockInfo.lockRelId.relId);
        partitionLocks = lappend(partitionLocks, locktag);
        locktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));
        SET_LOCKTAG_PARTITION(*locktag, newIndexPartition->pd_lockInfo.lockRelId.dbId,
                             newIndexId, newIndexPartition->pd_lockInfo.lockRelId.relId);
        partitionLocks = lappend(partitionLocks, locktag);

        MemoryContextSwitchTo(oldcontext);

        partitionClose(indexRelation, indexPartition, NoLock);
        partitionClose(newIndexRelation, newIndexPartition, NoLock);
        index_close(indexRelation, NoLock);
        index_close(newIndexRelation, NoLock);
        heap_close(heapRelation, NoLock);
    }

    /*
     * Save the heap lock for following visibility checks with other backends
     * might conflict with this session.
     */
    foreach (lc, heapRelationIds) {
        Relation heapRelation = heap_open(lfirst_oid(lc), ShareUpdateExclusiveLock);
        LockRelId* lockrelid;
        LOCKTAG* heaplocktag;

        /* Save the list of locks in private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        lockrelid = (LockRelId*) palloc(sizeof(LockRelId));
        *lockrelid = heapRelation->rd_lockInfo.lockRelId;

        /* Add lockrelid of heap relation to the list of locked relations */
        relationLocks = lappend(relationLocks, lockrelid);

        heaplocktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));

        /* Save the LOCKTAG for this parent relation for the wait phase */
        SET_LOCKTAG_RELATION(*heaplocktag, lockrelid->dbId, lockrelid->relId);
        lockTags = lappend(lockTags, heaplocktag);

        MemoryContextSwitchTo(oldcontext);

        /* Close heap relation */
        heap_close(heapRelation, NoLock);
    }

    /* 
     * Save the heap partition lock for following visibility checks wth other backends
     * might conflict with this session.
     */
    foreach (lc, heapPartitionIds) {
        Oid heapPartId = lfirst_oid(lc);
        Oid heapId = PartIdGetParentId(heapPartId, false);
        Relation heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
        Partition heapPartition = partitionOpen(heapRelation, heapPartId, ShareUpdateExclusiveLock);
        LockRelId lockrelid = heapPartition->pd_lockInfo.lockRelId;
        LOCKTAG* heapPartlocktag;

        /* Save the list of locks in private context */
        oldcontext = MemoryContextSwitchTo(private_context);

        /* Add lockrelid of heap partition to the list of locked partitions */
        heapPartlocktag = (LOCKTAG*) palloc(sizeof(LOCKTAG));

        /* Save the LOCKTAG for this parent partition for the wait phase */
        SET_LOCKTAG_PARTITION(*heapPartlocktag, heapPartition->pd_lockInfo.lockRelId.dbId,
                             heapId, heapPartition->pd_lockInfo.lockRelId.relId);
        partitionLocks = lappend(partitionLocks, heapPartlocktag);
        lockTags = lappend(lockTags, heapPartlocktag);

        MemoryContextSwitchTo(oldcontext);

        /* Close heap Partition */
        partitionClose(heapRelation, heapPartition, NoLock);
        heap_close(heapRelation, NoLock);

    }

    /* Get a session-level lock on each table */
    foreach (lc, relationLocks) {
        LockRelId* lockRel = (LockRelId*) lfirst(lc);

        LockRelationIdForSession(lockRel, ShareUpdateExclusiveLock);
    }

    /* Get a session-level lock on each partition */
    foreach(lc, partitionLocks) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);

        (void)LockAcquire(locktag, ShareUpdateExclusiveLock, true, false);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 2 of REINDEX CONCURRENTLY
     *
     * Build the new indexes in a separate transaction for each index to avoid
     * having open transactions for an unnecessary long time. But before
     * doing that, wait until no running transactions could have the table of
     * the index open with the old list of indexes. See "phase 2" in
     * DefineIndex() for more details. 
     * 
     * If index is partitioned, build the new index partitions.
     */
    foreach (lc, lockTags) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);
        old_lockholders = GetLockConflicts(locktag, ShareLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            (void)VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }
    }
    CommitTransactionCommand();

    forboth (lc, indexIds, lc2, newIndexIds) {
        Relation indexRel;
        Oid oldIndexId = lfirst_oid(lc);
        Oid newIndexId = lfirst_oid(lc2);
        Oid heapId;
        bool isPrimary;
        bool isPartition;

        /* Start new transaction for this index's concurrent build */
        StartTransactionCommand();

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        /* Set ActiveSnapshot since functions in the indexes may need it */
        PushActiveSnapshot(GetTransactionSnapshot());

        /*
         * Index relation has been closed by previous commit, so reopen it to
         * get its information.
         */
        indexRel = index_open(oldIndexId, ShareUpdateExclusiveLock);
        heapId = indexRel->rd_index->indrelid;
        isPrimary = indexRel->rd_rel->relhaspkey;
        isPartition = RelationIsPartitioned(indexRel);
        index_close(indexRel, NoLock);

        if (!isPartition)
            /* Perform concurrent build of new index */
            index_concurrently_build(heapId, newIndexId, isPrimary, memInfo, dbWide);
        else {
            /* Perform concurrent build of new index partition */
            ListCell* partCell;

            /* avoid build autoadd interval index partitions */
            foreach (partCell, heapPartitionIds) {
                Oid heapPartId = lfirst_oid(partCell);
                Oid newIndexPartId = indexIdAndPartitionIdGetIndexPartitionId(newIndexId, heapPartId);

                index_concurrently_part_build(heapId, heapPartId, newIndexId, newIndexPartId, memInfo, dbWide);
            }

            index_set_state_flags(newIndexId, INDEX_CREATE_SET_READY);
        }

        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    foreach (lc, newIndexPartIds) {
        Oid newIndexPartId = lfirst_oid(lc);
        Oid indexId;
        Oid heapId;
        Oid heapPartId;
        List* heapPartIds;
        ListCell* partCell;

        /* Start new transaction for this index partition's concurrent build */
        StartTransactionCommand();

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        /* Set ActiveSnapshot since functions in the partindexes may need it */
        PushActiveSnapshot(GetTransactionSnapshot());

        indexId = PartIdGetParentId(newIndexPartId, false);
        heapPartId = PartIndexGetPartition(newIndexPartId, false);
        heapId = PartIdGetParentId(heapPartId, false);

        /* Perform concurrent build of new index part */
        index_concurrently_part_build(heapId, heapPartId, indexId, newIndexPartId, memInfo, dbWide);

        heapPartIds = getPartitionObjectIdList(heapId, PART_OBJ_TYPE_TABLE_PARTITION);
        
        /* Invalidate the partcache for other partition, so that after this commit
         * all sessions will refresh any cached plans that might reference the
         * partitioned index.
         * 
         * The partcache which is related to heapPartId has been invalidate in index_concurrently_part_build();
         */
        foreach(partCell, heapPartIds) {
            Oid otherHeapPartId = lfirst_oid(partCell);

            if (otherHeapPartId != heapPartId)
                CacheInvalidatePartcacheByPartid(otherHeapPartId);
        }

        index_set_state_flags(indexId, INDEX_CREATE_SET_READY);

        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    StartTransactionCommand();

    /*
     * Phase 3 of REINDEX CONCURRENTLY
     * 
     * During this phase the new indexes catch up with any new tuples that
     * were created during the previous phase. See "Phase 3" in DefineIndex()
     * for more details;
     */
    foreach (lc, lockTags) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);
        old_lockholders = GetLockConflicts(locktag, ShareLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            (void)VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }
    }
    CommitTransactionCommand();

    TransactionId limitXmin = InvalidTransactionId;

    foreach (lc, newIndexIds) {
        Oid newIndexId = lfirst_oid(lc);
        Oid heapId;
        Relation indexRelation;
        Snapshot snapshot;

        StartTransactionCommand();

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        heapId = IndexGetRelation(newIndexId, false);
        indexRelation = index_open(newIndexId, ShareUpdateExclusiveLock);

        /*
         * Take the "reference snapshot" that will be used by validate_index()
         * to filter candidate tuples.
         */
        snapshot = RegisterSnapshot(GetTransactionSnapshot());
        PushActiveSnapshot(snapshot);

        if (!RelationIsPartitioned(indexRelation))
            validate_index(heapId, newIndexId, snapshot);
        else {
            ListCell* partCell = NULL;

            /* avoid validate autoadd interval index partition */
            foreach(partCell, heapPartitionIds) {
                Oid heapPartId = lfirst_oid(partCell);
                Oid indexPartId = indexIdAndPartitionIdGetIndexPartitionId(newIndexId, heapPartId);

                validate_index(heapPartId, indexPartId, snapshot, true);
            }
        }

        /*
         * We can now do away with our active snapshot, we still need to save
         * the first snapshot xmin limit to wait for older snapshot in two loop.
         */
        if (TransactionIdPrecedes(limitXmin, snapshot->xmin))
            limitXmin = snapshot->xmin;

        PopActiveSnapshot();
        UnregisterSnapshot(snapshot);

        index_close(indexRelation, NoLock);

        /*
         * To ensure no deadlocks, we must commit and start yet another
         * transaction, and do our wait before any snapshot has been taken in
         * it.
         */
        CommitTransactionCommand();
    }
    
    foreach (lc, newIndexPartIds) {
        Oid newIndexPartId = lfirst_oid(lc);
        Oid heapPartId;
        Snapshot snapshot;

        StartTransactionCommand();

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        heapPartId = PartIndexGetPartition(newIndexPartId, false);

        /*
         * Take the "reference snapshot" that will be used by validate_index()
         * to filter candidate tuples.
         */
        snapshot = RegisterSnapshot(GetTransactionSnapshot());
        PushActiveSnapshot(snapshot);

        validate_index(heapPartId, newIndexPartId, snapshot, true);

        /*
         * We can now do away with our active snapshot, we still need to save
         * the first snapshot xmin limit to wait for older snapshot in two loop.
         */
        if (TransactionIdPrecedes(limitXmin, snapshot->xmin))
            limitXmin = snapshot->xmin;

        PopActiveSnapshot();
        UnregisterSnapshot(snapshot);

        /*
         * To ensure no deadlocks, we must commit and start yet another
         * transaction, and do our wait before any snapshot has been taken in
         * it.
         */
        CommitTransactionCommand();
    }

    StartTransactionCommand();

    /*
     * The index is now valid in the sense that it contains all currently
     * interesting tuples. But since it might not contain tuples deleted just
     * before the reference snap was taken, we have to wait out any
     * transactions that might have older snapshots.
     */
    WaitForOlderSnapshots(limitXmin);

    CommitTransactionCommand();

    /*
     * Phase 4 of REINDEX CONCURRENTLY
     * 
     * Now that the new indexes have been validated, swap each new index with
     * its corresponding old index.
     * 
     * If index is not partitioned,
     * we mark the new indexes as valid and the old indexes as not valid at
     * the same time to make sure we only get constraint violations from the
     * indexes with the correct names.
     * 
     * If index is partitioned,
     * we mark the old index partitions are unuse and swap name and parentid
     * between the new index partition and the old one 
     */
    StartTransactionCommand();

    forboth (lc, indexIds, lc2, newIndexIds) {
        char* oldName;
        Oid oldIndexId = lfirst_oid(lc);
        Oid newIndexId = lfirst_oid(lc2);
        Oid heapId;
        Relation heapRelation;

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        heapId = IndexGetRelation(oldIndexId, false);

        heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
        if (!RelationIsPartitioned(heapRelation)) {
            /* Choose a relation name for old index */
            oldName = ChooseRelationName(get_rel_name(oldIndexId), NULL, "ccold", 5,
                                        get_rel_namespace(heapId), false);

            /*
             * Swap old index with the new one. This also marks the new one as
             * valid and the old one as not valid.
             */
            index_concurrently_swap(newIndexId, oldIndexId, oldName);

            /*
             * Invalidate the relcache for the table, so that after this commit
             * all sessions will refresh any cached plans that might reference the
             * index.
             */
            CacheInvalidateRelcacheByRelid(heapId);

            /*
             * CCI here so that subsequent iterations see the oldName in the
             * catalog and can choose a nonconflicting name for their oldName.
             * Otherwise, this could lead to confilcts if a table has two indexes
             * whose names are equal for the first NAMEDATALEN-minus-a-few
             * characters.
             */
            CommandCounterIncrement();
        } else {
            ListCell* partCell;
            
            /* avoid swap autoadd interval index partitions */
            foreach (partCell, heapPartitionIds) {
                Oid heapPartId = lfirst_oid(partCell);
                Oid oldIndexPartId = indexIdAndPartitionIdGetIndexPartitionId(oldIndexId, heapPartId);
                Oid newIndexPartId = indexIdAndPartitionIdGetIndexPartitionId(newIndexId, heapPartId);
                
                /* Choose a partition name for old index part */
                oldName = ChoosePartitionName(getPartitionName(oldIndexPartId, false),
                                            NULL, "ccold", oldIndexId,
                                            PART_OBJ_TYPE_INDEX_PARTITION);
                /*
                 * Swap old index part with the new one. This also marks the old one 
                 * as not usable.
                 */
                index_concurrently_part_swap(newIndexPartId, oldIndexPartId, oldName);

                /*
                 * Invalidate the partcache for the partition, so that after this commit
                 * all sessions will refresh any cached plans that might reference the
                 * index.
                 */
                CacheInvalidatePartcacheByPartid(heapPartId);

                /*
                 * CCI here so that subsequent iterations see the oldName in the
                 * catalog and can choose a nonconflicting name for their oldName.
                 * Otherwise, this could lead to confilcts if a table has two indexes
                 * whose names are equal for the first NAMEDATALEN-minus-a-few
                 * characters.
                 */
                CommandCounterIncrement();
            }

            // call the internal function, if index is unusable, set it usable
            ATExecSetIndexUsableState(IndexRelationId, oldIndexId, true);
            CacheInvalidateRelcacheByRelid(heapId);

            /* 
             * swap index id for drop new partitioned index, because this new partitioned 
             * index has old index partitions.
             */
            lc->data.oid_value = newIndexId;
            lc2->data.oid_value = oldIndexId;
        }
        heap_close(heapRelation, NoLock);
    }

    forboth (lc, indexPartIds, lc2, newIndexPartIds) {
        char* oldName;
        Oid oldIndexPartId = lfirst_oid(lc);
        Oid newIndexPartId = lfirst_oid(lc2);
        Oid oldIndexId = InvalidOid;
        Oid heapPartId = InvalidOid;

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();

        oldIndexId = PartIdGetParentId(oldIndexPartId, false);
        heapPartId = PartIndexGetPartition(oldIndexPartId, false);

        /* Choose a partition name for old index part */
        oldName = ChoosePartitionName(getPartitionName(oldIndexPartId, false),
                                    NULL, "ccold", oldIndexId,
                                    PART_OBJ_TYPE_INDEX_PARTITION);
        /*
         * Swap old index part with the new one. This also marks the old one
         * as not usable.
         */
        index_concurrently_part_swap(newIndexPartId, oldIndexPartId, oldName);

        /*
         * Invalidate the partcache for the table, so that after this commit
         * all sessions will refresh any cached plans that might reference the
         * index.
         */
        CacheInvalidatePartcacheByPartid(heapPartId);

        /*
         * CCI here so that subsequent iterations see the oldName in the
         * catalog and can choose a nonconflicting name for their oldName.
         * Otherwise, this could lead to confilcts if a table has two indexes
         * whose names are equal for the first NAMEDATALEN-minus-a-few
         * characters.
         */
        CommandCounterIncrement();
    }
    
    /* Commit this transaction and make index swaps visible */
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 5 of REINDEXX CONCURRENTLY 
     * 
     * Mark the old indexes or new partitioned indexes as dead, because 
     * new partitioned indexes have old index partitions. First we must
     * wait until no running transaction could be using the index 
     * for a query. See also index_drop() for more details.
     */
    foreach (lc, lockTags) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);
        old_lockholders = GetLockConflicts(locktag, ShareLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            (void)VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }
    }

    foreach (lc, indexIds) {
        Oid oldIndexId = lfirst_oid(lc);
        Oid heapId;

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();
        heapId = IndexGetRelation(oldIndexId, false);
        index_concurrently_set_dead(heapId, oldIndexId);
    }

    foreach (lc, indexPartIds) {
        Oid oldIndexPartId = lfirst_oid(lc);
        Oid indexId = PartIdGetParentId(oldIndexPartId, false);
        Oid heapId;

        /*
         * Check for user-requested abort.  This is inside a transaction so as
         * xact.c does not issue a useless WARNING, and ensures that
         * session-level locks are cleaned up on abort.
         */
        CHECK_FOR_INTERRUPTS();
        heapId = IndexGetRelation(indexId, false);
        index_concurrently_set_dead(heapId, indexId);
    }

    /* Commit this transaction to make the updates visible. */
    CommitTransactionCommand();
    StartTransactionCommand();

    /*
     * Phase 6 of REINDEX CONCURRENTLY 
     * 
     * Drop the old indexes and new partitioned indexes which have old index partitions.
     */
    foreach (lc, lockTags) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);
        old_lockholders = GetLockConflicts(locktag, ShareLock);

        while (VirtualTransactionIdIsValid(*old_lockholders)) {
            (void)VirtualXactLock(*old_lockholders, true);
            old_lockholders++;
        }
    }

    PushActiveSnapshot(GetTransactionSnapshot());

    {
        foreach (lc, heapRelationIds) {
            Oid heapRelationId = lfirst_oid(lc);
            Relation heapRelation = heap_open(heapRelationId, AccessShareLock);

            /* get add interval partition lock, unlock after transaction commit */
            if (RelationIsPartitioned(heapRelation) && heapRelation->partMap->type == PART_TYPE_INTERVAL)
                LockPartitionObject(heapRelation->rd_id, INTERVAL_PARTITION_LOCK_SDEQUENCE, PARTITION_EXCLUSIVE_LOCK);
            
            heap_close(heapRelation, AccessShareLock);
        }

        ObjectAddresses* objects = new_object_addresses();

        foreach (lc, indexIds) {
            Oid oldIndexId = lfirst_oid(lc);
            ObjectAddress* object =(ObjectAddress*) palloc(sizeof(ObjectAddress));

            object->classId = RelationRelationId;
            object->objectId = oldIndexId;
            object->objectSubId = 0;

            add_exact_object_address(object, objects);
        }

        foreach (lc, indexPartIds) {
            Oid oldIndexPartId = lfirst_oid(lc);
            Oid indexId = PartIdGetParentId(oldIndexPartId, false);
            ObjectAddress* object =(ObjectAddress*) palloc(sizeof(ObjectAddress));

            object->classId = RelationRelationId;
            object->objectId = indexId;
            object->objectSubId = 0;

            add_exact_object_address(object, objects);
        }

        /*
         * Use PERFORM_DELETION_CONCURRENT_LOCK co that index_drop() uses the
         * right lock level.
         */
        performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_CONCURRENTLY_LOCK | PERFORM_DELETION_INTERNAL);
    }

    PopActiveSnapshot();
    CommitTransactionCommand();

    /*
     * Finally, release the session-level lock on the table and partition.
     */
    foreach (lc, partitionLocks) {
        LOCKTAG* locktag = (LOCKTAG*) lfirst(lc);

        (void)LockRelease(locktag, ShareUpdateExclusiveLock, true);
    }
    
    foreach (lc, relationLocks) {
        LockRelId* lockRel = (LockRelId*) lfirst(lc);

        UnlockRelationIdForSession(lockRel, ShareUpdateExclusiveLock);
    }

    /* Start a new transaction to finish process properly */
    StartTransactionCommand();

    MemoryContextDelete(private_context);
    
    return true;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void addIndexForPartition(Relation partitionedRelation, Oid partOid)
{
    IndexInfo* indexInfo = NULL;
    Relation indexRel = NULL;
    List* indexElemList = NIL;
    List* indexColNames = NIL;
    List* indelist = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partitionDelta = NULL;
    char* indexPartitionName = NULL;
    Oid indexRelOid = InvalidOid;
    Oid indexHeapOid = InvalidOid;
    char buf[NAMEDATALEN] = {0};
    HeapTuple indexTuple;
    Form_pg_index indexForm;
    HeapTuple attTuple;
    Form_pg_attribute attForm;
    int2 indexColumns = -1;
    int2 indexColumn = -1;
    int2vector* idxKeys = NULL;
    Datum idxKeysDatum;
    bool isnull = false;
    int i;
    Relation pg_partition_rel;

    if (!PointerIsValid(partitionedRelation) || !OidIsValid(partOid)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("Invalid relation to create index partition")));
    }

    /* get oid list of indexRel */
    indelist = RelationGetSpecificKindIndexList(partitionedRelation, false);
    if (!PointerIsValid(indelist)) {
        return;
    }

    partition = partitionOpen(partitionedRelation, partOid, ShareUpdateExclusiveLock);
    pg_partition_rel = heap_open(PartitionRelationId, RowExclusiveLock);

    foreach (cell, indelist) {
        IndexElem* indexelem = NULL;
        indexColNames = NIL;
        indexElemList = NIL;
        Oid partIndexOid = InvalidOid;
        indexRelOid = lfirst_oid(cell);
        indexRel = relation_open(indexRelOid, AccessShareLock);
        indexInfo = BuildIndexInfo(indexRel);

        indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexRel->rd_id));
        if (!HeapTupleIsValid(indexTuple)) {
            partitionClose(partitionedRelation, partition, NoLock);
            ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("fail to get index info for index %u", indexRel->rd_id)));
        }

        indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
        indexHeapOid = indexForm->indrelid;
        indexColumns = indexForm->indnatts;

        idxKeysDatum = SysCacheGetAttr(INDEXRELID, indexTuple, Anum_pg_index_indkey, &isnull);
        Assert(!isnull);

        idxKeys = (int2vector*)DatumGetPointer(idxKeysDatum);

        Assert(idxKeys->dim1 == indexColumns);

        for (i = 0; i < indexColumns; i++) {
            indexColumn = idxKeys->values[i];

            if (indexColumn == 0) {
                indexElemList = lappend(indexElemList, makeNode(IndexElem));
                continue;
            }
            attTuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(indexHeapOid), Int32GetDatum(indexColumn));
            if (!HeapTupleIsValid(attTuple)) {
                partitionClose(partitionedRelation, partition, NoLock);

                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                        errmsg("unable to find attribute %d for relation %u.", (int)indexColumn, indexHeapOid)));
            }

            attForm = (Form_pg_attribute)GETSTRUCT(attTuple);

            errno_t rc = EOK;
            rc = strncpy_s(buf, sizeof(buf), NameStr(attForm->attname), sizeof(buf) - 1);
            securec_check(rc, "", "");

            indexelem = makeNode(IndexElem);
            indexelem->name = pstrdup(buf);
            indexElemList = lappend(indexElemList, indexelem);

            heap_freetuple(attTuple);
        }

        indexColNames = ChooseIndexColumnNames(indexElemList);

        /* choose a default name */
        indexPartitionName =
            ChoosePartitionIndexName(PartitionGetPartitionName(partition), indexRelOid, indexColNames, false, false);

        // inherit the relation-options of existing partitioned index relation.
        // keep the same reloptions with all the exsiting partitions.
        //
        Datum indexRelOptions = (Datum)0;
        HeapTuple indexTupleInPgclass = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(indexRelOid));
        if (indexTupleInPgclass != NULL) {
            indexRelOptions = SysCacheGetAttr(RELOID, indexTupleInPgclass, Anum_pg_class_reloptions, &isnull);
            if (isnull) {
                indexRelOptions = (Datum)0;
            }
        } else {
            partitionClose(partitionedRelation, partition, NoLock);

            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache %d lookup failed for relation %u", RELOID, indexRelOid)));
        }

        PartIndexCreateExtraArgs partExtraArg;
        partExtraArg.existingPSortOid = InvalidOid;
        partExtraArg.crossbucket = RelationIsCrossBucketIndex(indexRel);

        partIndexOid = partition_index_create(indexPartitionName,
            InvalidOid,
            partition,
            indexRel->rd_rel->reltablespace,
            indexRel,
            partitionedRelation,
            pg_partition_rel,
            indexInfo,
            indexColNames,
            indexRelOptions,
            false,
            &partExtraArg);

#ifndef ENABLE_MULTIPLE_NODES
        if (RelationIsCUFormat(partitionedRelation) && indexForm->indisunique) {
            if (!PointerIsValid(partitionDelta)) {
                partitionDelta = heap_open(partition->pd_part->reldeltarelid, ShareUpdateExclusiveLock);
            }
            char partDeltaIdxName[NAMEDATALEN] = {0};
            error_t ret = snprintf_s(partDeltaIdxName, sizeof(partDeltaIdxName),
                sizeof(partDeltaIdxName) - 1, "pg_delta_part_index_%u", partIndexOid);
            securec_check_ss_c(ret, "\0", "\0");

            (void)CreateDeltaUniqueIndex(partitionDelta, partDeltaIdxName, indexInfo, indexElemList,
                indexColNames, indexForm->indisprimary);
        }
#endif

        relation_close(indexRel, NoLock);
        heap_freetuple(indexTuple);
        heap_freetuple(indexTupleInPgclass);
    }

    heap_close(pg_partition_rel, RowExclusiveLock);
    partitionClose(partitionedRelation, partition, NoLock);

    if (PointerIsValid(partitionDelta)) {
        heap_close(partitionDelta, NoLock);
    }
}

void dropIndexForPartition(Oid partOid)
{
    List* partIndexlist = NULL;
    HeapTuple partIndexTuple = NULL;
    Form_pg_partition partForm = NULL;
    Oid partIndexOid = InvalidOid;
    Relation indexRel = NULL;
    ListCell* cell = NULL;

    if (!OidIsValid(partOid)) {
        return;
    }

    /* first get the list of index partition on targeting table partition */
    partIndexlist = searchPartitionIndexesByblid(partOid);
    if (!PointerIsValid(partIndexlist)) {
        return;
    }

    /* iterate the index partition list */
    foreach (cell, partIndexlist) {
        partIndexTuple = (HeapTuple)lfirst(cell);
        if (HeapTupleIsValid(partIndexTuple)) {
            partForm = (Form_pg_partition)GETSTRUCT(partIndexTuple);

            if (!PointerIsValid(partForm)) {
                continue;
            }

            /* get partitioned index's oid, and index partition's oid*/
            partIndexOid = HeapTupleGetOid(partIndexTuple);
            indexRel = relation_open(partForm->parentid, AccessShareLock);
            heapDropPartitionIndex(indexRel, partIndexOid);
            relation_close(indexRel, NoLock);
        }
    }

    freePartList(partIndexlist);
}

/*
 * Brief        : See whether relation has an existing a informational primary key.
 * Description  : See whether relation has an existing a informational primary key.
 * Input        : conrel, a relation struct object.
 * Output       : None.
 * Return Value : return true if relation has existing a informational primary key.
 *                else, return false.
 * Notes        : None.
 */
static bool relationHasInformationalPrimaryKey(const Relation rel)
{
    bool found = false;
    HeapTuple htup;
    ScanKeyData skey[1];
    SysScanDesc conscan;
    Relation conrel;
    ScanKeyInit(
        &skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
    conrel = heap_open(ConstraintRelationId, ShareUpdateExclusiveLock);
    conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, skey);
    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(htup);

        if ('p' == con->contype) {
            /* Found it. */
            found = true;
            break;
        }
    }

    systable_endscan(conscan);
    heap_close(conrel, ShareUpdateExclusiveLock);

    return found;
}

/*
 * Handle the error message accroding to informational constaint.
 */
static void handleErrMsgForInfoCnstrnt(const IndexStmt* stmt, const Relation rel)
{
    if (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_MATVIEW) {
        Oid relationId = RelationGetRelid(rel);
        /*
         * @hdfs
         * Because of using informational constraint on HDFS foreign talble,
         * HDFS foreign table need take this "create index" branch.
         */
        if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE || rel->rd_rel->relkind == RELKIND_STREAM) {
            ForeignTable* ftbl = NULL;
            ForeignServer* fsvr = NULL;
            ftbl = GetForeignTable(relationId);

            Assert(ftbl != NULL);
            fsvr = GetForeignServer(ftbl->serverid);
            Assert(fsvr != NULL);

            /*
             * @hdfs
             * When add or create a informational constraint for the HDFS foreign table,
             * this branch will be covered.
             */
#ifdef ENABLE_MOT
            if ((!isMOTFromTblOid(RelationGetRelid(rel)) &&
                    !CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(RelationGetRelid(rel))) || !stmt->internal_flag) {
#else
            if (!CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(RelationGetRelid(rel)) || !stmt->internal_flag) {
#endif
                /*
                 * Custom error message for FOREIGN TABLE since the term is close
                 * to a regular table and can confuse the user.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("cannot create index on foreign table \"%s\"", RelationGetRelationName(rel))));
            }
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a table", RelationGetRelationName(rel))));
        }
    }
}

/*
 * Build constraint name for informational constaint.
 */
static void buildConstraintNameForInfoCnstrnt(
    const IndexStmt* stmt, Relation rel, char** indexRelationName, Oid namespaceId, const List* indexColNames)
{
    /*
     * @hdfs
     * If the IndexStmt bulit for informational consraint with constraint name, must check
     * whether has same constraint name in pg_constraint. If found the same name, must rebuild
     * other name for the constraint.
     */
    if (stmt->idxname == NULL && stmt->internal_flag) {
        if (FindExistingConstraint(*indexRelationName, rel)) {
            if (stmt->primary) {
                *indexRelationName = ChooseConstraintName(RelationGetRelationName(rel), NULL, "pkey", namespaceId, NIL);
            } else if (stmt->isconstraint) {
                *indexRelationName = ChooseConstraintName(
                    RelationGetRelationName(rel), ChooseIndexNameAddition(indexColNames), "key", namespaceId, NIL);
            }
        }
    }
}

/*
 * Build the informational constraint.
 */
static Oid buildInformationalConstraint(
    IndexStmt* stmt, Oid indexRelationId, const char* indexRelationName, Relation rel, IndexInfo* indexInfo, Oid namespaceId)
{
    /*
     * @hdfs
     * Now, we have to deal with the soft constraint. Do not bulid index for it.
     * So, Only update the pg_constraint for soft constraint.
     * If the stmt->internal_flag is true, it means that stmt relate with the HDFS
     * foreign table.
     */
    char constraintType;

    if (stmt->primary && relationHasInformationalPrimaryKey(rel)) {
        heap_close(rel, NoLock);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg(
                    "Multiple primary keys for foreign table \"%s\" are not allowed.", RelationGetRelationName(rel))));
    }
    /* Check constraint name indexRelationName in pg_constraint */
    if (stmt->idxname != NULL && FindExistingConstraint(indexRelationName, rel)) {
        heap_close(rel, NoLock);
        ereport(
            ERROR, (errcode(ERRCODE_UNIQUE_VIOLATION), errmsg("Constraint \"%s\" already exists.", indexRelationName)));
    }

    /* currently, the soft constraint only support primary key and unique type */
    if (stmt->primary) {
        constraintType = CONSTRAINT_PRIMARY;
    } else {
        constraintType = CONSTRAINT_UNIQUE;
    }

    /*
     * Construct a pg_constraint entry.
     */
    (void)CreateConstraintEntry(indexRelationName, /* constraintName */
        namespaceId,
        constraintType,
        stmt->deferrable,
        stmt->initdeferred,
        true,
        RelationGetRelid(rel),
        indexInfo->ii_KeyAttrNumbers,
        indexInfo->ii_NumIndexKeyAttrs,
        indexInfo->ii_NumIndexAttrs,
        InvalidOid,      /* no domain */
        indexRelationId, /* InvalidOid */
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
        true,                   /* islocal */
        0,                      /* inhcount */
        true,                   /* noinherit */
        stmt->inforConstraint); /* informational constraint */

    heap_close(rel, NoLock);
    return InvalidOid;
}

/*
 * Index constraint: Local partition index could not be on same column with global partition index
 * This function check all exist index on table of 'relOid', compare index attr column with new index of 'indexInfo',
 * return true indicate new index is compatible with all existing index, otherwise, return false.
 */
static bool CheckGlobalIndexCompatible(Oid relOid, bool isGlobal, const IndexInfo* indexInfo, Oid currMethodOid)
{
    ScanKeyData skey[1];
    SysScanDesc sysScan;
    HeapTuple tarTuple;
    Relation indexRelation;
    bool ret = true;
    errno_t rc;
    bool isNull = false;
    char currIdxKind = isGlobal ? RELKIND_GLOBAL_INDEX : RELKIND_INDEX;
    int currSize = sizeof(AttrNumber) * indexInfo->ii_NumIndexKeyAttrs;
    int currKeyNum = indexInfo->ii_NumIndexKeyAttrs;

    ScanKeyInit(&skey[0], Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relOid));
    indexRelation = heap_open(IndexRelationId, AccessShareLock);
    sysScan = systable_beginscan(indexRelation, IndexIndrelidIndexId, true, NULL, 1, skey);

    AttrNumber* currAttrsArray = (AttrNumber*)palloc0(currSize);
    rc = memcpy_s(currAttrsArray, currSize, indexInfo->ii_KeyAttrNumbers, currSize);
    securec_check(rc, "\0", "\0");
    qsort(currAttrsArray, currKeyNum, sizeof(AttrNumber), AttrComparator);

    while (HeapTupleIsValid(tarTuple = systable_getnext(sysScan))) {
        Form_pg_index indexTuple = (Form_pg_index)GETSTRUCT(tarTuple);
        char tarIdxKind = get_rel_relkind(indexTuple->indexrelid);
        /* only check index of different type(local and global) */
        if (currIdxKind != tarIdxKind) {
            if (!CheckIndexMethodConsistency(tarTuple, indexRelation, currMethodOid)) {
                continue;
            }

            heap_getattr(tarTuple, Anum_pg_index_indexprs, RelationGetDescr(indexRelation), &isNull);
            /*
             * check expressions: This condition looks confused, here we judge whether tow index has expressions,
             * like XOR operation. if two indexes both have expression or not, we continue to
             * check next condition, otherwise, two index could be treated as compatible.
             */
            if ((indexInfo->ii_Expressions != NIL) != (!isNull)) {
                continue;
            }
            if (!CheckSimpleAttrsConsistency(tarTuple, currAttrsArray, currKeyNum)) {
                ret = false;
                break;
            }
        }
    }
    systable_endscan(sysScan);
    heap_close(indexRelation, AccessShareLock);
    pfree(currAttrsArray);
    return ret;
}

/*
 * check consistency of two index, we use first attrs opclass as key to search index method
 */
static bool CheckIndexMethodConsistency(HeapTuple indexTuple, Relation indexRelation, Oid currMethodOid)
{
    bool isNull = false;
    bool ret = true;
    oidvector* opClass = (oidvector*)DatumGetPointer(
        heap_getattr(indexTuple, Anum_pg_index_indclass, RelationGetDescr(indexRelation), &isNull));
    if (opClass == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Operator class search failed!")));
    }
    Oid opClassOid = opClass->values[0];
    HeapTuple opClassTuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(opClassOid));
    if (!HeapTupleIsValid(opClassTuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Operator class does not exist for index compatible check.")));
    }
    Oid tarMethodOid = ((Form_pg_opclass)GETSTRUCT(opClassTuple))->opcmethod;
    if (tarMethodOid != currMethodOid) {
        ret = false;
    }
    ReleaseSysCache(opClassTuple);
    return ret;
}

/*
 * check column consistency: if run here, then compare two indexes' simple index col,
 * if index col array is totally same, it means not compatible situation.
 */
static bool CheckSimpleAttrsConsistency(HeapTuple tarTuple, const int16* currAttrsArray, int currKeyNum)
{
    Form_pg_index indexTuple = (Form_pg_index)GETSTRUCT(tarTuple);
    int tarKeyNum = GetIndexKeyAttsByTuple(NULL, tarTuple);
    bool ret = true;
    AttrNumber* indkeyValues = NULL;
    int i;
    if (tarKeyNum == currKeyNum) {
        size_t indkeyValuesSize = sizeof(AttrNumber) * currKeyNum;
        indkeyValues = (AttrNumber*)palloc0(indkeyValuesSize);
        errno_t rc = memcpy_s(indkeyValues, indkeyValuesSize, indexTuple->indkey.values, indkeyValuesSize);
        securec_check(rc, "\0", "\0");

        qsort(indkeyValues, currKeyNum, sizeof(AttrNumber), AttrComparator);
        for (i = 0; i < currKeyNum; i++) {
            if (indkeyValues[i] != currAttrsArray[i]) {
                break;
            }
        }
        if (i == currKeyNum) {  // attrs of two index is totally same, which indicates not compatible.
            ret = false;
        }
    }
    pfree_ext(indkeyValues);
    return ret;
}

static int AttrComparator(const void* a, const void* b)
{
    return *(AttrNumber*)a - *(AttrNumber*)b;
}

/* Set the internal index column partoid for global partition index */
static void AddIndexColumnForGpi(IndexStmt* stmt)
{
    ListCell* cell = NULL;
    foreach (cell, stmt->indexIncludingParams) {
        IndexElem* param = (IndexElem*)lfirst(cell);
        if (strcmp(param->name, "tableoid") == 0) {
            return;
        }
    }
    IndexElem* iparam = makeNode(IndexElem);
    iparam->name = pstrdup("tableoid");
    iparam->expr = NULL;
    iparam->indexcolname = NULL;
    iparam->collation = NIL;
    iparam->opclass = NIL;
    stmt->indexIncludingParams = lappend(stmt->indexIncludingParams, iparam);
}

/* Set the internal index column bucketid for crossbucket index */
static void AddIndexColumnForCbi(IndexStmt* stmt)
{
    ListCell* cell = NULL;
    foreach (cell, stmt->indexIncludingParams) {
        IndexElem* param = (IndexElem*)lfirst(cell);
        if (strcmp(param->name, "tablebucketid") == 0) {
            return;
        }
    }
    IndexElem* iparam = makeNode(IndexElem);
    iparam->name = pstrdup("tablebucketid");
    iparam->expr = NULL;
    iparam->indexcolname = NULL;
    iparam->collation = NIL;
    iparam->opclass = NIL;
    stmt->indexIncludingParams = lappend(stmt->indexIncludingParams, iparam);
}
static void CheckIndexParamsNumber(IndexStmt* stmt) {
    if (list_length(stmt->indexParams) <= 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("must specify at least one column")));
    }
    if (list_length(stmt->indexParams) > INDEX_MAX_KEYS) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS), errmsg("cannot use more than %d columns in an index", INDEX_MAX_KEYS)));
    }
    if (stmt->isGlobal && stmt->crossbucket && (list_length(stmt->indexParams) > GLOBAL_CROSSBUCKET_INDEX_MAX_KEYS)) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("cannot use more than %d columns in a global cross-bucket index", 
                    GLOBAL_CROSSBUCKET_INDEX_MAX_KEYS)));
    } else if (stmt->isGlobal && (list_length(stmt->indexParams) > INDEX_MAX_KEYS - 1)) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("cannot use more than %d columns in an global partition index", INDEX_MAX_KEYS - 1)));
    } else if (stmt->crossbucket && (list_length(stmt->indexParams) > INDEX_MAX_KEYS - 1)) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg("cannot use more than %d columns in a cross-bucket index", INDEX_MAX_KEYS - 1)));
    }
}

static bool CheckIdxParamsOwnPartKey(Relation rel, const List* indexParams)
{
    if (!PartExprKeyIsNull(rel, NULL)) {
        return false;
    }
    int2vector* partKey = ((RangePartitionMap*)rel->partMap)->partitionKey;
    for (int i = 0; i < partKey->dim1; i++) {
        int2 attNum = partKey->values[i];
        Form_pg_attribute attTup = &rel->rd_att->attrs[attNum - 1];
        if (!columnIsExist(rel, attTup, indexParams)) {
            return false;
        }
    }
    return true;
}


static bool
CheckWhetherForbiddenFunctionalIdx(Oid relationId, Oid namespaceId, List* indexParams)
{
    ListCell* lc = NULL;
    bool isFunctionalIdx = false;

    foreach (lc, indexParams) {
        IndexElem* elem = (IndexElem*)lfirst(lc);
        if (PointerIsValid(elem) && PointerIsValid(elem->expr) 
            && nodeTag(elem->expr) == T_FuncExpr) {
            isFunctionalIdx = true;
            break;
        }
    }
    /* 
     * If the index is not a functional index, the function will return false directly.
     * */
    if (likely((!isFunctionalIdx))) {
        return false;
    }

    /* Currently, there is only one element in the forbidden list. 
     * Hence we can determine it using the following method briefly.
     * */
    if (unlikely(namespaceId == PG_DB4AI_NAMESPACE || namespaceId == PG_SNAPSHOT_NAMESPACE)) {
        return true;
    }

    return false;
}


#ifdef ENABLE_MULTIPLE_NODES
/*
 * @Description : Mark index indisvalid.
 * @in         	: schemaname, idxname
 * @out         : None
 */
Datum gs_mark_indisvalid(PG_FUNCTION_ARGS)
{
    if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_PGXC_DATANODE) {
        ereport(ERROR, (errmodule(MOD_FUNCTION), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Unsupported function for users."),
            errdetail("This is an inner function used for CIC."),
            errcause("This function is not supported for users to execute directly."),
            erraction("Please do not execute this function.")));
    } else {
        char* schname = PG_GETARG_CSTRING(0);
        char* idxname = PG_GETARG_CSTRING(1);
        if (idxname == NULL || strlen(idxname) == 0) {
            ereport(ERROR, (errmodule(MOD_INDEX), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid input index name."),
                errdetail("The input index name is null."),
                errcause("Input empty or less parameters."),
                erraction("Please input the correct index name.")));
            PG_RETURN_VOID();
        }
        mark_indisvalid_all_cns(schname, idxname);
    }
    PG_RETURN_VOID();
}

/* Mark the given index indisvalid, used for create index concurrently */
void mark_indisvalid_local(char* schname, char* idxname)
{
    Oid idx_oid = InvalidOid;
    if (schname == NULL || strlen(schname) == 0) {
        idx_oid = RangeVarGetRelid(makeRangeVar(NULL, idxname, -1), NoLock, false);
    } else {
        idx_oid = RangeVarGetRelid(makeRangeVar(schname, idxname, -1), NoLock, false);
    }

    if (!OidIsValid(idx_oid)) {
        ereport(ERROR, (errmodule(MOD_INDEX), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The given schema or index name cannot find."),
            errdetail("Cannot find valid oid from the given index name."),
            errcause("Input error schema or index name."),
            erraction("Check the input schema and index name.")));
    }

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && GetTopTransactionIdIfAny() != InvalidTransactionId) {
        CommitTransactionCommand();
        StartTransactionCommand();
    }

    Relation rel = heap_open(IndexGetRelation(idx_oid, false), ShareUpdateExclusiveLock);
    LockRelId heaprelid = rel->rd_lockInfo.lockRelId;
    heap_close(rel, NoLock);

    LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
    index_set_state_flags(idx_oid, INDEX_CREATE_SET_VALID);
    /*
     * The pg_index update will cause backends (including this one) to update
     * relcache entries for the index itself, but we should also send a
     * relcache inval on the parent table to force replanning of cached plans.
     * Otherwise existing sessions might fail to use the new index where it
     * would be useful.  (Note that our earlier commits did not create reasons
     * to replan; so relcache flush on the index itself was sufficient.)
     */
    CacheInvalidateRelcacheByRelid(heaprelid.relId);

    UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
}

void mark_indisvalid_all_cns(char* schname, char* idxname)
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        ParallelFunctionState* state = NULL;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "select pg_catalog.gs_mark_indisvalid(");
        if (schname == NULL || strlen(schname) == 0) {
            appendStringInfo(&buf, "'', %s)", quote_literal_cstr(idxname));
        } else {
            appendStringInfo(&buf, "%s, %s)", quote_literal_cstr(schname), quote_literal_cstr(idxname));
        }

        state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_COORDS, true);
        FreeParallelFunctionState(state);
        pfree_ext(buf.data);
    }

    mark_indisvalid_local(schname, idxname);
}

#endif
