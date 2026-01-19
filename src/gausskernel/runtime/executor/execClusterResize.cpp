/* -------------------------------------------------------------------------
 *
 * execClusterResize.cpp
 *	  MPPDB ClusterResizing relevant routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execClusterResize.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pgxc_group.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "optimizer/clauses.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

#include "storage/lmgr.h"
#include "postgres_ext.h"

/*
 * ---------------------------------------------------------------------------------
 *						*Local functions/variables declaration fields*
 * ---------------------------------------------------------------------------------
 */
/* delete delta table definition */
#define Natts_pg_delete_delta 3

#define Anum_pg_delete_delta_xcnodeid_and_dntableoid 1
#define Anum_pg_delete_delta_tablebucketid_and_ctid 2

#define RANGE_SCAN_IN_REDIS "tidge+tid+pg_get_redis_rel_start_ctid+tidle+tid+pg_get_redis_rel_end_ctid+"

static Node* eval_dnstable_func_mutator(
    Relation rel, Node* node, StringInfo qual_str, RangeScanInRedis *rangeScanInRedis, bool isRoot);

static inline bool redis_tupleid_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs);

static inline bool redis_blocknum_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs);

static inline bool redis_offset_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs);

#define REDIS_TUPLEID_RETRIVE_FUNCSIG(rettype, argstype, nargs)                                     \
    (((nargs) == 1 && (rettype) == TIDOID && (argstype)[0] == TEXTOID) ||                           \
    ((nargs) == 4 && (rettype) == TIDOID && (argstype)[0] == TEXTOID && (argstype)[1] == NAMEOID && \
    (argstype)[2] == INT4OID && (argstype)[3] == INT4OID))

static inline bool redis_tupleid_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs)

{
    if (pg_strcasecmp(funcname, "pg_get_redis_rel_start_ctid") == 0 &&
        REDIS_TUPLEID_RETRIVE_FUNCSIG(rettype, argstype, nargs)) {
        return true;
    }

    if (pg_strcasecmp(funcname, "pg_get_redis_rel_end_ctid") == 0 &&
        REDIS_TUPLEID_RETRIVE_FUNCSIG(rettype, argstype, nargs)) {
        return true;
    }

    return false;
}

static inline bool redis_offset_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs)

{
    if (pg_strcasecmp(funcname, "pg_tupleid_get_offset") == 0 &&
        (nargs == 1 && rettype == INT4OID && argstype[0] == TIDOID)) {
        return true;
    }

    return false;
}

static inline bool redis_blocknum_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs)

{
    if (pg_strcasecmp(funcname, "pg_tupleid_get_blocknum") == 0 &&
        (nargs == 1 && rettype == INT8OID && argstype[0] == TIDOID)) {
        return true;
    }

    return false;
}

static inline bool redis_ctid_retrive_function(const char* funcname, Oid rettype, const Oid* argstype, int nargs)

{
    if (pg_strcasecmp(funcname, "pg_tupleid_get_ctid_to_bigint") == 0 &&
        (nargs == 1 && rettype == INT8OID && argstype[0] == TIDOID)) {
        return true;
    }

    return false;
}


/*
 * - Brief: Record the given tuple's tupleid into pg_delete_delta table
 * - Parameter:
 *      @rel: target relation of UPDATE/DELETE operation
 *      @tupleid: tupleid that needs record
 * - Return:
 *      no return value
 */
void RecordDeletedTuple(Oid relid, int2 bucketid, const ItemPointer tupleid, const Relation deldelta_rel)
{
    Datum values[Natts_pg_delete_delta];
    bool nulls[Natts_pg_delete_delta];
    HeapTuple tup = NULL;

    Assert(deldelta_rel);
    /* In redistribution, table delete_delta has 3 or 2 column. */
    Assert(RelationGetDescr(deldelta_rel)->natts <= 3);

    /* Iterate through attributes initializing nulls and values */
    for (int i = 0; i < Natts_pg_delete_delta; i++) {
        nulls[i] = false;
        values[i] = (Datum)0;
    }
    values[Anum_pg_delete_delta_xcnodeid_and_dntableoid - 1] = 
        UInt64GetDatum(((uint64)u_sess->pgxc_cxt.PGXCNodeIdentifier << 32) | relid);
    values[Anum_pg_delete_delta_tablebucketid_and_ctid - 1] =
        UInt64GetDatum(((uint64)ItemPointerGetBlockNumber(tupleid) << 16) | ItemPointerGetOffsetNumber(tupleid));
    if (BUCKET_NODE_IS_VALID(bucketid)) {
        values[Anum_pg_delete_delta_tablebucketid_and_ctid - 1] |= ((uint64)bucketid << 48);
    }
    /* Record delta */
    tup = heap_form_tuple(RelationGetDescr(deldelta_rel), values, nulls);
    (void)simple_heap_insert(deldelta_rel, tup);

    tableam_tops_free_tuple(tup);
}

/*
 * - Brief: Determine if the relation is under cluster resizing operation
 * - Parameter:
 *      @rel: relation that needs to check
 * - Return:
 *      @TRUE:  relation is under cluster resizing
 *      @FALSE: relation is not under cluster resizing
 */
bool RelationInClusterResizing(const Relation rel)
{
    Assert(rel != NULL);

    /* Check relation's append_mode status */
    if (!IsInitdb && RelationInRedistribute(rel))
        return true;

    return false;
}

/*
 * - Brief: Determine if the relation is under cluster resizing read only operation
 * - Parameter:
 *      @rel: relation that needs to check
 * - Return:
 *      @TRUE:  relation is under cluster resizing read only
 *      @FALSE: relation is not under cluster resizing read only
 */
bool RelationInClusterResizingReadOnly(const Relation rel)
{
    Assert(rel != NULL);

    /* Check relation's append_mode status */
    if (!IsInitdb && RelationInRedistributeReadOnly(rel))
        return true;

    return false;
}

/*
 * - Brief: Determine if the relation is under cluster resizing read only operation
 * - Parameter:
 *      @rel: relation that needs to check
 * - Return:
 *      @TRUE:  relation is under cluster resizing endcatchup(write error)
 *      @FALSE: relation is not under cluster resizing endcatchup(write error)
 */
bool RelationInClusterResizingEndCatchup(const Relation rel)
{
    Assert(rel != NULL);

    /* Check relation's append_mode status */
    if (!IsInitdb && RelationInRedistributeEndCatchup(rel))
        return true;

    return false;
}

/*
 * @Description: check whether relation is in redistribution though range variable.
 * @in range_var: range variable which stored relation info.
 * @return: true for in redistribution.
 */
bool CheckRangeVarInRedistribution(const RangeVar* range_var)
{
    Relation relation;
    Oid relid;
    bool in_redis = false;

    relid = RangeVarGetRelid(range_var, AccessShareLock, true);

    if (OidIsValid(relid)) {
        relation = relation_open(relid, NoLock);
        /* If the relation is index, we should check the related table is resizing or not. */
        if (RelationIsIndex(relation)) {
            Oid heapOid = IndexGetRelation(relid, false);
            Relation heapRelation = relation_open(heapOid, AccessShareLock);
            in_redis = RelationInClusterResizing(heapRelation);
            relation_close(heapRelation, AccessShareLock);
        } else {
            in_redis = RelationInClusterResizing(relation);
        }

        relation_close(relation, NoLock);
        UnlockRelationOid(relid, AccessShareLock);
    }
    return in_redis;
}

/*
 * - Brief: Determine if the table name is delete_delta table.
 * - Parameter:
 *      @relname: name of target table
 * - Return:
 *      @TRUE: the table is delete_delta table
 *      @FALSE: the table is not delete_delta table
 */
bool RelationIsDeleteDeltaTable(char* delete_delta_name)
{
    Oid relid;
    uint64 val;
    char* endptr = NULL;
    HeapTuple tuple;

    if (IsInitdb) {
        return false;
    }

    if (strncmp(delete_delta_name, "pg_delete_delta_", 16) != 0) {
        return false;
    }

    val = strtoull(delete_delta_name + 16, &endptr, 0);

    if ((errno == ERANGE) || (errno != 0 && val == 0)) {
        return false;
    }

    if (endptr == delete_delta_name + 16 || *endptr != '\0') {
        return false;
    }
    relid = (Oid)val;

    if (!OidIsValid(relid)) {
        return false;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        elog(WARNING, "Table %u related to %s does not exists.", relid, delete_delta_name);
        return false;
    }
    ReleaseSysCache(tuple);
    return true;
}

/*
 * - Brief: Determine if the Progress is under cluster resizing status
 * - Return:
 *      @TRUE:  Progress is under cluster resizing
 *      @FALSE: Progress is not under cluster resizing
 */
bool ClusterResizingInProgress()
{
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;
    bool result = false;

    pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    if (!pgxc_group_rel) {
        ereport(PANIC, (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("can not open pgxc_group")));
    }

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pgxc_group_in_redistribution, RelationGetDescr(pgxc_group_rel), &isNull);

        if ('y' == DatumGetChar(datum)) {
            result = true;
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    return result;
}

/*
 * - Brief: get the name of delete_delta table
 * - Parameter:
 *      @relname: name of target table
 *      @delta_delta_name: output value for delete_delta table name
 *      @isMultiCatchup: multi catchup delta or not
 * - Return:
 *      no return value
 */
static inline void RelationGetDeleteDeltaTableName(Relation rel, char* delete_delta_name, bool isMultiCatchup)
{
    int rc = 0;

    /* Check if output parameter it not palloc()-ed from caller side */
    if (delete_delta_name == NULL || rel == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid parameter in function '%s'", __FUNCTION__)));
    }

    /*
     * Look up Relation's reloptions to get table's cnoid to
     * form the name of delete_delta table
     */
    if (!IsInitdb) {
        if (RelationInClusterResizing(rel) && !RelationInClusterResizingReadOnly(rel)) {
            if (isMultiCatchup) {
                rc = snprintf_s(delete_delta_name,
                    NAMEDATALEN,
                    NAMEDATALEN - 1,
                    REDIS_MULTI_CATCHUP_DELETE_DELTA_TABLE_PREFIX "%u",
                    RelationGetRelCnOid(rel));
            } else {
                rc = snprintf_s(delete_delta_name,
                                NAMEDATALEN,
                                NAMEDATALEN - 1,
                                REDIS_DELETE_DELTA_TABLE_PREFIX "%u",
                                RelationGetRelCnOid(rel));
            }
            securec_check_ss(rc, "\0", "\0");
        } else {
            elog(LOG, "rel %s doesn't exist in redistributing", RelationGetRelationName(rel));
            rc = snprintf_s(delete_delta_name, 
                            NAMEDATALEN, 
                            NAMEDATALEN - 1, 
                            REDIS_DELETE_DELTA_TABLE_PREFIX "%s", 
                            RelationGetRelationName(rel));
            securec_check_ss(rc, "\0", "\0");
        }
    }
    return;
}

/*
 * - Brief: get and open delete_delta rel
 * - Parameter:
 *      @rel: target relation of UPDATE/DELETE/TRUNCATE operation
 *      @lockmode: lock mode
 *      @isMultiCatchup: multi catchup delta or not
 * - Return:
 *      delete_delta rel
 */
Relation GetAndOpenDeleteDeltaRel(const Relation rel, LOCKMODE lockmode, bool isMultiCatchup)
{
    Relation deldelta_rel;
    Oid deldelta_relid;
    char delete_delta_tablename[NAMEDATALEN];
    Oid data_redis_namespace;
    errno_t errorno;

    errorno = memset_s(delete_delta_tablename, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check_c(errorno, "\0", "\0");

    RelationGetDeleteDeltaTableName(rel, (char*)delete_delta_tablename, isMultiCatchup);
    data_redis_namespace = get_namespace_oid("data_redis", false);

    /* We are going to fetch the delete delta relation under data_redis schema. */
    deldelta_relid = get_relname_relid(delete_delta_tablename, data_redis_namespace);
    if (!OidIsValid(deldelta_relid)) {
        /*
         * If multi catchup delta table is not there, just return NULL. We should not
         * report error, because it is a valid case. Multi catchup delta table is
         * dropped in each catchup iteration.
         */
        if (isMultiCatchup) {
            return NULL;
        }

        /*
         * To support Update or Delete during extension, we need to add 2 more columns.
         * more columns. Limited by MaxHeapAttributeNumber, if the table already contains too many columns,
         * we don't allow update or delete anymore, but insert statement can still proceed.
         */
        if (((rel->rd_att->natts > (MaxHeapAttributeNumber - (Natts_pg_delete_delta - 1))) &&
                !RELATION_IS_PARTITIONED(rel)) ||
            ((rel->rd_att->natts > (MaxHeapAttributeNumber - Natts_pg_delete_delta)) && RELATION_IS_PARTITIONED(rel))) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("do not support update or delete on table %s, when do cluster resizing on it.",
                        RelationGetRelationName(rel)),
                    errdetail("Can not support online extension, if the table contains too many columns")));
        }
        /* ERROR case, should never come here */
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("delete delta table %s is not found when do cluster resizing table \"%s\"",
                    delete_delta_tablename,
                    RelationGetRelationName(rel))));
    }
    deldelta_rel = relation_open(deldelta_relid, lockmode);
    elog(DEBUG1,
        "Delete_delta table %s for relation %s being under cluster resizing is valid.",
        delete_delta_tablename,
        RelationGetRelationName(rel));

    return deldelta_rel;
}

/*
 * - Brief: Check the stmtment during online expansion, block unsupported ddl in cluster resizing.
 * - Parameter:
 *      @rel: parsetree of DDL
 * - Return:
 *      no return value
 */
void BlockUnsupportedDDL(const Node* parsetree)
{
    if (IsInitdb) {
        return;
    }

    Relation rel = NULL;
    Oid relid = InvalidOid;
    List* relidlist = NULL;
    ListCell* lc = NULL;
    LOCKMODE lockmode_getrelid = AccessShareLock;
    LOCKMODE lockmode_openrel = AccessShareLock;

    /*
     * Check for shared-cache-inval messages before trying to access the
     * relation.  This is needed to cover the case where the name
     * identifies a rel that has been dropped and recreated since the
     * start of our transaction: if we don't flush the old syscache entry,
     * then we'll latch onto that entry and suffer an error later.
     */
    AcceptInvalidationMessages();

    switch (nodeTag(parsetree)) {
        case T_CreatedbStmt:
        case T_AlterDatabaseStmt:
        case T_AlterDatabaseSetStmt:
        case T_CreateTableSpaceStmt:
        case T_DropTableSpaceStmt:
        case T_AlterTableSpaceOptionsStmt:
        case T_CreateGroupStmt: {
            if (ClusterResizingInProgress()) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion", CreateCommandTag((Node*)parsetree))));
            }
            return;
        } break;

        case T_DropdbStmt: {
            if (ClusterResizingInProgress() && !u_sess->attr.attr_common.xc_maintenance_mode) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion", CreateCommandTag((Node*)parsetree))));
            } else if (ClusterResizingInProgress()) {
                ereport(WARNING, (errmsg("Drop database during online expansion in maintenance mode!")));
            }
            return;
        } break;

        /* Block CURSOR for while table in cluster resizing */
        case T_PlannedStmt: {
            PlannedStmt* stmt = (PlannedStmt*)parsetree;
            relidlist = stmt->relationOids;
        } break;

        /* Block RENAME while table in cluster resizing */
        case T_RenameStmt: {
            RenameStmt* stmt = (RenameStmt*)parsetree;

            switch (stmt->renameType) {
                case OBJECT_SCHEMA: {
                    Oid nsOid = get_namespace_oid(stmt->subname, true);
                    TRANSFER_DISABLE_DDL(nsOid);
                    break;
                }
                case OBJECT_TABLE: {
                    if (stmt->relation != NULL) {
                        Oid relOid = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                        if (OidIsValid(relOid)) {
                            Oid nsOid = GetNamespaceIdbyRelId(relOid);
                            UnlockRelationOid(relOid, AccessShareLock);
                            TRANSFER_DISABLE_DDL(nsOid);
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            if (stmt->relation && CheckRangeVarInRedistribution(stmt->relation))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion on '%s'",
                            CreateCommandTag((Node*)parsetree),
                            stmt->relation->relname)));
        } break;

        /* Block ALTER set schema while table in cluster resizing */
        case T_AlterObjectSchemaStmt: {
            AlterObjectSchemaStmt* stmt = (AlterObjectSchemaStmt*)parsetree;

            /* disable alter table set schema when transfer */
            if (stmt->relation != NULL) {
                Oid relOid = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                if (OidIsValid(relOid)) {
                    Oid nsOid = GetNamespaceIdbyRelId(relOid);
                    UnlockRelationOid(relOid, AccessShareLock);
                    TRANSFER_DISABLE_DDL(nsOid);

                    if (stmt->newschema) {
                        nsOid = get_namespace_oid(stmt->newschema, true);
                        TRANSFER_DISABLE_DDL(nsOid);
                    }
                }
            }

            if (stmt->relation && CheckRangeVarInRedistribution(stmt->relation))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion on '%s'",
                            CreateCommandTag((Node*)parsetree),
                            stmt->relation->relname)));
        } break;

        /* Block CREATE index while table in cluster resizing(for row table only) */
        case T_IndexStmt: {
            IndexStmt* stmt = (IndexStmt*)parsetree;
            if (stmt->relation) {
                relid = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                if (OidIsValid(relid)) {
                    Relation relation = relation_open(relid, NoLock);
                    bool inRedis = RelationIsRowFormat(relation) && RelationInClusterResizing(relation);

                    relation_close(relation, NoLock);
                    UnlockRelationOid(relid, AccessShareLock);

                    if (inRedis) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupport '%s' command during online expansion on '%s'",
                                    CreateCommandTag((Node*)parsetree),
                                    stmt->relation->relname)));
                    }
                }
            }
        } break;

        /* Block REINDEX while table in cluster resizing(for row table only) */
        case T_ReindexStmt: {
            ReindexStmt* stmt = (ReindexStmt*)parsetree;
            if (stmt->relation) {
                relid = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                if (OidIsValid(relid)) {
                    /* release index lock before lock table to avoid deadlock */
                    UnlockRelationOid(relid, AccessShareLock);

                    Relation relation = relation_open(relid, NoLock);
                    bool inRedis = false;

                    if (RelationIsRelation(relation)) {
                        inRedis = RelationIsRowFormat(relation) && RelationInClusterResizing(relation);
                    } else if (RelationIsIndex(relation)) {
                        Oid heapOid = IndexGetRelation(relid, false);
                        Relation heapRelation = relation_open(heapOid, AccessShareLock);
                        inRedis = RelationIsRowFormat(heapRelation) && RelationInClusterResizing(heapRelation);
                        relation_close(heapRelation, AccessShareLock);
                    }
                    relation_close(relation, NoLock);
                    if (inRedis) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupport '%s' command during online expansion on '%s'",
                                    CreateCommandTag((Node*)parsetree),
                                    stmt->relation->relname)));
                    }
                }
            }
        } break;

        /* Block ALTER-Table while table in cluster resizing */
        case T_AlterTableStmt: {
            AlterTableStmt* stmt = (AlterTableStmt*)parsetree;
            AlterTableCmd* cmd = NULL;
            foreach (lc, stmt->cmds) {
                cmd = (AlterTableCmd*)lfirst(lc);
                switch (cmd->subtype) {
                    case AT_TruncatePartition: {
                        /*
                         * We do not allow truncate partition when the target is in read only
                         * mode during online expansion time.
                         */
                        if (stmt->relation) {
                            relid = RangeVarGetRelid(stmt->relation, lockmode_getrelid, true);
                            if (OidIsValid(relid)) {
                                /* disable alter table truncate partition during transfer */
                                if (CheckRangeVarInRedistribution(stmt->relation)) {
                                    Oid nsOid = GetNamespaceIdbyRelId(relid);
                                    TRANSFER_DISABLE_DDL(nsOid);
                                }

                                rel = relation_open(relid, NoLock);
                                if (RelationInClusterResizingReadOnly(rel)) {
                                    ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg("Unsupport '%s' command with '%s' option during online expansion on "
                                                   "'%s' because the object is in read only mode.",
                                                CreateCommandTag((Node*)parsetree),
                                                CreateAlterTableCommandTag(cmd->subtype),
                                                RelationGetRelationName(rel))));
                                }
                                relation_close(rel, NoLock);

                                if (u_sess->attr.attr_sql.enable_parallel_ddl)
                                    UnlockRelationOid(relid, lockmode_getrelid);
                            }
                        }
                    } break;
                    case AT_AddNodeList:
                    case AT_DeleteNodeList: {
#ifndef ENABLE_MULTIPLE_NODES
                        ereport(ERROR,
                            (errmodule(MOD_FUNCTION),
                            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported feature"),
                            errdetail("The capability is not supported for openGauss."),
                            errcause("%s is not supported for openGauss",
                                cmd->subtype == AT_DeleteNodeList ? "DeleteNode" : "AddNode"),
                            erraction("NA")));
#endif
                    } break;
                    case AT_UpdateSliceLike: {
#ifndef ENABLE_MULTIPLE_NODES
                        ereport(ERROR,
                            (errmodule(MOD_FUNCTION),
                            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported feature"),
                            errdetail("The capability is not supported for openGauss."),
                            errcause("UpdateSliceLike is not supported for openGauss"),
                            erraction("NA")));
#endif
                        if (!ClusterResizingInProgress() && IS_PGXC_COORDINATOR) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Cannot alter table update slice like when it is not "
                                           "under redistribution")));
                        }
                    } break;
                    case AT_ResetRelOptions:
                    case AT_SetRelOptions:
                        if (cmd->def) {
                            List* options = (List*)cmd->def;
                            ListCell* opt = NULL;
                            DefElem* def = NULL;
                            foreach (opt, options) {
                                def = (DefElem*)lfirst(opt);

                                if (pg_strcasecmp(def->defname, "append_mode") == 0) {
                                    break;
                                }
                            }

                            /* If rel option contain append_mode, then not check. */
                            if (opt != NULL) {
                                break;
                            }
                        }
                        /* fall through */
                    default: {
                        if (stmt->relation && !u_sess->attr.attr_sql.enable_cluster_resize &&
                            CheckRangeVarInRedistribution(stmt->relation))
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Unsupport '%s' command with '%s' option during online expansion on '%s'",
                                        CreateCommandTag((Node*)parsetree),
                                        CreateAlterTableCommandTag(cmd->subtype),
                                        stmt->relation->relname)));
                    } break;
                }
            }
            return;
        } break;

        /* Block CREATE-RULE statements while target table in cluster resizing */
        case T_RuleStmt: {
            RuleStmt* stmt = (RuleStmt*)parsetree;
            if (stmt->relation) {
                relid = RangeVarGetRelid(stmt->relation, lockmode_getrelid, true);
                relidlist = list_make1_oid(relid);
            }
        } break;

        /* Block CREATE SEQUENCE set schema while owner table in cluster resizing */
        case T_CreateSeqStmt: {
            CreateSeqStmt* stmt = (CreateSeqStmt*)parsetree;
            List* owned_by = NULL;
            DefElem* defel = NULL;
            foreach (lc, stmt->options) {
                defel = (DefElem*)lfirst(lc);
                if (pg_strcasecmp(defel->defname, "owned_by") == 0 && nodeTag(defel->arg) == T_List) {
                    owned_by = (List*)defel->arg;
                }
            }

            if (owned_by != NULL) {
                int owned_len = list_length(owned_by);
                if (owned_len != 1) {
                    List* relname = list_truncate(list_copy(owned_by), owned_len - 1);
                    RangeVar* r = makeRangeVarFromNameList(relname);
                    if (r && CheckRangeVarInRedistribution(r))
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupport '%s' command during online expansion on '%s'",
                                    CreateCommandTag((Node*)parsetree),
                                    r->relname)));
                }
            }
        } break;

        /* Block ALTER SEQUENCE while owner table in cluster resizing */
        case T_AlterSeqStmt: {
            AlterSeqStmt* stmt = (AlterSeqStmt*)parsetree;
            List* owned_by = NIL;
            DefElem* defel = NULL;
            foreach (lc, stmt->options) {
                defel = (DefElem*)lfirst(lc);
                if (pg_strcasecmp(defel->defname, "owned_by") == 0 && nodeTag(defel->arg) == T_List) {
                    owned_by = (List*)defel->arg;
                }
            }

            if (owned_by != NULL) {
                int owned_len = list_length(owned_by);
                if (owned_len != 1) {
                    List* relname = list_truncate(list_copy(owned_by), owned_len - 1);
                    RangeVar* r = makeRangeVarFromNameList(relname);
                    if (r && CheckRangeVarInRedistribution(r))
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupport '%s' command during online expansion on '%s'",
                                    CreateCommandTag((Node*)parsetree),
                                    r->relname)));
                }
            }
        } break;

        /* Block CLUSTER while table in cluster resizing */
        case T_ClusterStmt: {
            ClusterStmt* stmt = (ClusterStmt*)parsetree;
            if (stmt->relation && CheckRangeVarInRedistribution(stmt->relation))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion on '%s'",
                            CreateCommandTag((Node*)parsetree),
                            stmt->relation->relname)));
        } break;

        /* Block VACUUM FULL while table in cluster resizing */
        case T_VacuumStmt: {
            VacuumStmt* stmt = (VacuumStmt*)parsetree;
            if ((stmt->options & VACOPT_VACUUM) || (stmt->options & VACOPT_MERGE)) {
                if (stmt->relation) {
                    relid = RangeVarGetRelid(stmt->relation, lockmode_getrelid, true);
                    relidlist = list_make1_oid(relid);
                }
            }
            if (stmt->options & VACOPT_FULL) {
                if (OidIsValid(relid)) {
                    rel = relation_open(relid, lockmode_openrel);
                    if (RelationInClusterResizing(rel)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unsupport 'VACUUM FULL' command during online expansion on '%s'",
                                    RelationGetRelationName(rel))));
                    }
                    relation_close(rel, lockmode_openrel);
                }
            }
        } break;

        /* Block truncate DDL when the target table is read only in cluster resizing */
        case T_TruncateStmt: {
            ListCell* cell = NULL;
            TruncateStmt* stmt = (TruncateStmt*)parsetree;

            foreach (cell, stmt->relations) {
                RangeVar* rv = (RangeVar*)lfirst(cell);

                if (CheckRangeVarInRedistribution(rv)) {
                    Oid relOid = RangeVarGetRelid(rv, lockmode_getrelid, true);
                    if (OidIsValid(relOid)) {
                        Oid nsOid = GetNamespaceIdbyRelId(relOid);
                        UnlockRelationOid(relOid, lockmode_getrelid);
                        TRANSFER_DISABLE_DDL(nsOid);
                    }
                }

                rel = heap_openrv(rv, lockmode_openrel);

                if (RelationInClusterResizingReadOnly(rel)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupport '%s' command during online expansion on '%s' because the object is in "
                                   "read only mode.",
                                CreateCommandTag((Node*)parsetree),
                                RelationGetRelationName(rel))));
                }
                relation_close(rel, lockmode_openrel);
            }
        } break;

        case T_DropStmt: {
            DropStmt* stmt = (DropStmt*)parsetree;
            switch (stmt->removeType) {
                case OBJECT_TABLE: {
                    /* disable drop table when transfer */
                    ListCell* cell = NULL;
                    foreach (cell, stmt->objects) {
                        RangeVar* rel = makeRangeVarFromNameList((List*)lfirst(cell));
                        Oid relOid = RangeVarGetRelid(rel, AccessShareLock, true);
                        if (OidIsValid(relOid)) {
                            Oid nsOid = GetNamespaceIdbyRelId(relOid);
                            UnlockRelationOid(relOid, AccessShareLock);
                            TRANSFER_DISABLE_DDL(nsOid);
                        }
                    }
                    break;
                }
                case OBJECT_SCHEMA: {
                    /* disable drop schema when transfer */
                    ListCell* cell = NULL;
                    foreach (cell, stmt->objects) {
                        List* objname = (List*)lfirst(cell);
                        char* name = NameListToString(objname);
                        Oid nsOid = get_namespace_oid(name, true);
                        TRANSFER_DISABLE_DDL(nsOid);
                    }
                    break;
                }
                default:
                    break;
           }
        } break;

        case T_CreateStmt: {
            /* disable create table when transfer */
            CreateStmt* stmt = (CreateStmt*)parsetree;
            if (stmt->relation != NULL) {
                Oid nsOid = RangeVarGetCreationNamespace(stmt->relation);
                TRANSFER_DISABLE_DDL(nsOid);
            }
        } break;

        default:
            break;
    }

    foreach (lc, relidlist) {
        relid = lfirst_oid(lc);
        if (OidIsValid(relid) && get_rel_name(relid) != NULL) {
            rel = relation_open(relid, lockmode_openrel);
            if (RelationInClusterResizing(rel)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupport '%s' command during online expansion on '%s'",
                            CreateCommandTag((Node*)parsetree),
                            RelationGetRelationName(rel))));
            }
            relation_close(rel, lockmode_openrel);
        }
    }
}

/*
 * - Brief: For online expanions, the shippable function is evaluated here, the module
 * 			will be invoked in optimizer when do FQS evaluation, we have to define function
 *			as STABLE
 * - Parameter:
 *      @funcid: oid of user defined function which is createed/dropped in scope of gs_redis
 * - Return:
 *      @true: shippable
 *      @false: unshippable
 */
bool redis_func_shippable(Oid funcid)
{
    const char* func_name = get_func_name(funcid);
    Oid* argstype = NULL;
    int nargs;
    Oid rettype = InvalidOid;
    bool result = false;

    if (func_name == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("function with OID %u does not exist", funcid)));
    }

    /* Fetch function signatures */
    rettype = get_func_signature(funcid, &argstype, &nargs);

    if (redis_tupleid_retrive_function(func_name, rettype, argstype, nargs)) {
        /* tupleid retrive functions is shippable to datanodes */
        result = true;
    } else if (redis_offset_retrive_function(func_name, rettype, argstype, nargs)) {
        result = true;
    } else if (redis_blocknum_retrive_function(func_name, rettype, argstype, nargs)) {
        result = true;
    } else if (redis_ctid_retrive_function(func_name, rettype, argstype, nargs)) {
        result = true;
    }

    /* pfree */
    if (argstype != NULL) {
        pfree_ext(argstype);
        argstype = NULL;
    }

    return result;
}

/*
 * - Brief: determine if given funcid reflects a dn-stable function
 * - Parameter:
 *      @funcid: function oid that to evaluate
 * - Return:
 *      @result: true:dnstable false: not-dnstable function
 */
bool redis_func_dnstable(Oid funcid)
{
    const char* func_name = get_func_name(funcid);
    Oid* argstype = NULL;
    int nargs;
    Oid rettype = InvalidOid;
    bool result = false;

    if (func_name == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function with OID %u does not exist when checking function dnstable", funcid)));
    }

    /* Fetch function signatures */
    rettype = get_func_signature(funcid, &argstype, &nargs);

    if (redis_tupleid_retrive_function(func_name, rettype, argstype, nargs)) {
        /* tupleid retrive functions is dnstable */
        result = true;
    }

    return result;
}

/*
 * - Brief: evaluate ctid functions into a const value to avoid per-scanning
 *      tuple invokation in seqscan.
 * - Parameter:
 *		@rel: the rel being redistributing
 *      @original_quals: the original quals possible contains ctid_funcs
 *      @isRangeScanInRedis: if is a redis range scan
 * - Return:
 *      @new_quals: quals which func call be replaced by a const
 */
List* eval_ctid_funcs(Relation rel, List* original_quals, RangeScanInRedis *rangeScanInRedis)
{

    StringInfo qual_str = makeStringInfo();
    /*
     * we have to make a copy of the original quals, since the eval_dnstable_func_mutator
     * will modify the it. the original qual will be needed again and again in later
     * to be re-eval in partition table scans.
     */
    List* new_quals = (List*)copyObject((const void*)(original_quals));

    rangeScanInRedis->isRangeScanInRedis = false;
    rangeScanInRedis->sliceTotal = 0;
    rangeScanInRedis->sliceIndex = 0;
    (void)eval_dnstable_func_mutator(rel, (Node*)new_quals, qual_str, rangeScanInRedis, true);

    pfree_ext(qual_str->data);
    pfree_ext(qual_str);

    return new_quals;
}

static int32 get_expr_const_val(Node *val){
    if (IsA(val, Const) && !((Const*)val)->constisnull && ((Const*)val)->consttype == INT4OID) {
        return DatumGetInt32(((Const*)val)->constvalue);
    } else {
        return 0;
    }
}

/*
 * - Brief: working house for eval_dnstable_func() to evaluate dn stable function into a const
 *		value to avoid per-scanning tuple invocation in seqscan
 * - Parameter:
 *		@rel: the rel being redistributing
 *      @node: expression node
 *      @qual_str: predicate pattern
 *      @isRangeScanInRedis: output to indicate if the predicate pattern is range scan in redis
 *      @isRoot: we want to compare the predicate pattern only once at root level
 * - Return:
 *      @result: expression tree with dn stable function const-evaluated
 */
static Node* eval_dnstable_func_mutator(
    Relation rel, Node* node, StringInfo qual_str, RangeScanInRedis *rangeScanInRedis, bool isRoot)
{
    if (node == NULL)
        return NULL;

    if (IS_PGXC_COORDINATOR)
        return node;

    switch (nodeTag(node)) {
        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;

            /* flatten dn stable function into const value */
            if (redis_func_dnstable(expr->funcid)) {
                Node* new_const = NULL;
                char* funcname = get_func_name(expr->funcid);
                if (funcname == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                            errmsg("operation expression function with OID %u does not exist.", expr->funcid)));
                }

                bool is_func_get_start_ctid = pg_strcasecmp(funcname, "pg_get_redis_rel_start_ctid") == 0;
                bool is_func_get_end_ctid = pg_strcasecmp(funcname, "pg_get_redis_rel_end_ctid") == 0;

                if (is_func_get_start_ctid || is_func_get_end_ctid){
                    int32 numSlices = get_expr_const_val((Node*)list_nth(expr->args, 2));
                    int32 idxSlices = get_expr_const_val((Node*)list_nth(expr->args, 3));
                    new_const = eval_redis_func_direct(rel, is_func_get_start_ctid, numSlices, idxSlices);
                    rangeScanInRedis->sliceIndex = idxSlices;
                    rangeScanInRedis->sliceTotal = numSlices;
                } else {
                    new_const = eval_const_expressions(NULL, node);
                }
                appendStringInfoString(qual_str, get_func_name(expr->funcid));
                appendStringInfoString(qual_str, "+");
                return new_const;
            }

            break;
        }
        case T_List: {
            List* l = (List*)node;
            for (int i = 0; i < list_length(l); i++) {
                Node* expr = (Node*)list_nth(l, i);
                Node* new_expr = eval_dnstable_func_mutator(rel, expr, qual_str, rangeScanInRedis, false);

                /*
                 * If a FuncExpr node is evalated into a T_Const value, we are hitting
                 * the point so replace it in qual list.
                 */
                if (expr && IsA(expr, FuncExpr) && new_expr && IsA(new_expr, Const)) {
                    l = list_delete_ptr(l, expr);
                    l = lappend(l, new_expr);
                }
            }

            /*
             * If the predicate at root is something like "where ctid between pg_get_redis_rel_start_ctid('xx')
             * and pg_get_redis_rel_end_ctid('xx')" on DN, we will pushdown the predicate at scan node.
             */
            if (isRoot && pg_strcasecmp(qual_str->data, RANGE_SCAN_IN_REDIS) == 0) {
                rangeScanInRedis->isRangeScanInRedis = true;
            }

            break;
        }
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            char* funcname = get_func_name(opexpr->opfuncid);

            if (funcname == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("operation expression function with OID %u does not exist.", opexpr->opfuncid)));
            }
            appendStringInfoString(qual_str, funcname);
            appendStringInfoString(qual_str, "+");
            eval_dnstable_func_mutator(rel, (Node*)opexpr->args, qual_str, rangeScanInRedis, false);

            break;
        }
        case T_Var: {
            Var* var = (Var*)node;
            /* we only expect tid column in the predicate */
            if (var->vartype == TIDOID) {
                appendStringInfoString(qual_str, "tid");
                appendStringInfoString(qual_str, "+");
            }
            break;
        }
        default: {
            appendStringInfoString(qual_str, nodeTagToString(nodeTag(node)));
            appendStringInfoString(qual_str, "+");
            break;
        }
    }

    return NULL;
}

/*
 * - Brief: get and open new_table rel
 * - Parameter:
 *      @rel: target relation of TRUNCATE operation
 * - Return:
 *      new_table rel
 */
Relation GetAndOpenNewTableRel(const Relation rel, LOCKMODE lockmode)
{
    Relation newtable_rel = NULL;
    Oid newtable_relid = InvalidOid;
    Oid data_redis_namespace;
    char new_tablename[NAMEDATALEN];
    errno_t errorno = EOK;

    errorno = memset_s(new_tablename, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check_c(errorno, "\0", "\0");

    RelationGetNewTableName(rel, (char*)new_tablename);
    data_redis_namespace = get_namespace_oid("data_redis", false);
    newtable_relid = get_relname_relid(new_tablename, data_redis_namespace);
    if (!OidIsValid(newtable_relid)) {
        /* ERROR case, should never come here */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("new table %s is not found when do cluster resizing table \"%s\"",
                    new_tablename,
                    RelationGetRelationName(rel))));
    }
    newtable_rel = relation_open(newtable_relid, lockmode);
    elog(LOG,
        "New temp table %s for relation %s under cluster resizing is valid.",
        new_tablename,
        RelationGetRelationName(rel));

    return newtable_rel;
}

/*
 * - Brief: get the name of new table
 * - Parameter:
 *      @relname: name of target table
 *      @newtable_name: output value for new table name
 * - Return:
 *      no return value
 */
void RelationGetNewTableName(Relation rel, char* newtable_name)
{
    int rc = 0;

    /* Check if output parameter it not palloc()-ed from caller side */
    if (newtable_name == NULL || rel == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid parameter in function '%s' when getting the name of new table", __FUNCTION__)));
    }

    /*
     * Look up relaion's reloptions to get table's cnoid to
     * form the name of new table
     */
    if (!IsInitdb) {
        Oid rel_cn_oid = RelationGetRelCnOid(rel);
        if (OidIsValid(rel_cn_oid)) {
            rc = snprintf_s(newtable_name, NAMEDATALEN, NAMEDATALEN - 1, "data_redis_tmp_%u", rel_cn_oid);
        } else {
            elog(LOG, "rel %s doesn't exist in redistributing", RelationGetRelationName(rel));
            rc = snprintf_s(
                newtable_name, NAMEDATALEN, NAMEDATALEN - 1, "data_redis_tmp_%s", RelationGetRelationName(rel));
        }
        /* check the return value of security function */
        securec_check_ss(rc, "\0", "\0");
    }
    return;
}

/*
 * - Brief: Determine if the relation is under cluster resizing write error mode
 * - Parameter:
 *      @rel: relation that needs to check
 * - Return:
 *      @TRUE:  relation is under cluster resizing write error mode
 *      @FALSE: relation is not under cluster resizing write error mode
 */
bool RelationInClusterResizingWriteErrorMode(const Relation rel)
{
    return RelationInClusterResizingReadOnly(rel) ||
        (RelationInClusterResizingEndCatchup(rel) && !pg_try_advisory_lock_for_redis(rel));
}

