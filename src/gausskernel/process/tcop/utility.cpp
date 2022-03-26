/* -------------------------------------------------------------------------
 *
 * utility.cpp
 *	  Contains functions which control the execution of the openGauss utility
 *	  commands.  At one time acted as an interface between the Lisp and C
 *	  systems.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    src/gausskernel/process/tcop/utility.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/print.h"

#include "access/cstore_delta.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_synonym.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_streaming_fn.h"
#include "catalog/toasting.h"
#include "catalog/cstore_ctlg.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/gs_global_config.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/gs_matview.h"
#include "commands/alter.h"
#include "commands/async.h"
#include "commands/cluster.h"
#include "commands/comment.h"
#include "commands/collationcmds.h"
#include "commands/conversioncmds.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/discard.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/matview.h"
#include "commands/lockcmds.h"
#include "commands/portalcmds.h"
#include "commands/prepare.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sec_rls_cmds.h"
#include "commands/sequence.h"
#include "commands/subscriptioncmds.h"
#include "commands/shutdown.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/directory.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "commands/verify.h"
#include "commands/view.h"
#include "executor/node/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#include "instruments/instr_unique_sql.h"
#include "gaussdb_version.h"
#include "gs_policy/gs_policy_masking.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "nodes/makefuncs.h"
#include "parser/parse_utilcmd.h"
#include "parser/analyze.h"
#include "parser/parse_func.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteRemove.h"
#include "storage/smgr/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/tcap.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/acl.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "access/heapam.h"
#include "utils/syscache.h"
#include "gs_policy/gs_policy_audit.h"
#include "gs_policy/policy_common.h"
#include "client_logic/client_logic.h"
#include "db4ai/create_model.h"
#include "gs_ledger/userchain.h"
#include "gs_ledger/ledger_utils.h"
#include "libpq/libpq.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "pgxc/pgFdwRemote.h"
#include "pgxc/globalStatistic.h"
#include "tsdb/common/ts_tablecmds.h"
#include "tsdb/storage/delta_merge.h"
#include "tsdb/utils/ts_redis.h"
#include "tsdb/optimizer/policy.h"
#include "tsdb/time_bucket.h"
#include "streaming/streaming_catalog.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#ifdef PGXC
#include "pgxc/barrier.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/poolutils.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"
#include "pgxc/groupmgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "catalog/pgxc_node.h"
#include "workload/workload.h"
#include "streaming/init.h"
#include "replication/archive_walreceiver.h"

static RemoteQueryExecType ExecUtilityFindNodes(ObjectType object_type, Oid rel_id, bool* is_temp);
static RemoteQueryExecType exec_utility_find_nodes_relkind(Oid rel_id, bool* is_temp);
static RemoteQueryExecType get_nodes_4_comment_utility(CommentStmt* stmt, bool* is_temp, ExecNodes** exec_nodes);
static RemoteQueryExecType get_nodes_4_rules_utility(RangeVar* relation, bool* is_temp);
static void drop_stmt_pre_treatment(
    DropStmt* stmt, const char* query_string, bool sent_to_remote, bool* is_temp, RemoteQueryExecType* exec_type);
static void ExecUtilityWithMessage(const char* query_string, bool sent_to_remote, bool is_temp);

static void exec_utility_with_message_parallel_ddl_mode(
    const char* query_string, bool sent_to_remote, bool is_temp, const char* first_exec_node, RemoteQueryExecType exec_type);
static bool is_stmt_allowed_in_locked_mode(Node* parse_tree, const char* query_string);

static ExecNodes* assign_utility_stmt_exec_nodes(Node* parse_tree);

#endif

static void TransformLoadDataToCopy(LoadStmt* stmt);


static void add_remote_query_4_alter_stmt(bool is_first_node, AlterTableStmt* atstmt, const char* query_string, List** stmts,
    char** drop_seq_string, ExecNodes** exec_nodes);

bool IsVariableinBlackList(const char* name);

/* @hdfs DoVacuumMppTable process MPP local table for command Vacuum/Analyze */
void DoVacuumMppTable(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote);

/* Hook for plugins to get control in ProcessUtility() */
THR_LOCAL ProcessUtility_hook_type ProcessUtility_hook = NULL;
#ifdef ENABLE_MULTIPLE_NODES
static void analyze_tmptbl_debug_cn(Oid rel_id, Oid main_relid, VacuumStmt* stmt, bool iscommit);
#endif
/* @hdfs Get sheduling message for analyze command */
extern List* CNSchedulingForAnalyze(
    unsigned int* totalFilesNum, unsigned int* num_of_dns, Oid foreign_table_id, bool isglbstats);

extern void begin_delta_merge(VacuumStmt* stmt);
#ifdef ENABLE_MULTIPLE_NODES
static void set_dndistinct_coors(VacuumStmt* stmt, int attnum);
#endif
static void attatch_global_info(char** query_string_with_info, VacuumStmt* stmt, const char* query_string, bool has_var,
    AnalyzeMode e_analyze_mode, Oid rel_id, char* foreign_tbl_schedul_message = NULL);
static char* get_hybrid_message(ForeignTableDesc* table_desc, VacuumStmt* stmt, char* foreign_tbl_schedul_message);
static bool need_full_dn_execution(const char* group_name);

extern void check_log_ft_definition(CreateForeignTableStmt* stmt);
extern void ts_check_feature_disable();

/* only check the case where the vacuum list just contains one temp object */
static bool IsAllTempObjectsInVacuumStmt(Node* parsetree);
static int64 getCopySequenceMaxval(const char *nspname, const char *relname, const char *colname);
static int64 getCopySequenceCountval(const char *nspname, const char *relname);

/* the hash value of extension script */
#define POSTGIS_VERSION_NUM 2

/* the size of page, unit is kB */
#define PAGE_SIZE 8
#define HALF_AMOUNT 0.5
#define BIG_MEM_RATIO 1.2
#define SMALL_MEM_RATIO 1.1

static const int LOADER_COL_BUF_CNT = 5;

Oid GetNamespaceIdbyRelId(const Oid relid)
{
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("relation with OID %u does not exist", relid)));
    }

    Oid namespaceOid = ((Form_pg_class)GETSTRUCT(tuple))->relnamespace;

    ReleaseSysCache(tuple);
    return namespaceOid;
}

bool IsSchemaInDistribution(const Oid namespaceOid)
{
    bool isNull = false; 
    bool result = false;
    if (!OidIsValid(namespaceOid)) {
        return false;
    }

    HeapTuple tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_SCHEMA),
            errmsg("schema with OID %u does not exist", namespaceOid)));
    }

    Datum in_redistribution = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_in_redistribution, &isNull);
    if (!isNull) {
        result = (DatumGetChar(in_redistribution) == 'y');
    }
    ReleaseSysCache(tuple);
    return result;
}

static bool foundPgstatPartititonOperations(AlterTableType subtype)
{
    return subtype == AT_TruncatePartition || subtype == AT_ExchangePartition || subtype == AT_DropPartition ||
        subtype == AT_DropSubPartition;
}

/* ----------------------------------------------------------------
 * report_utility_time
 *
 * send the finish time of copy and exchange/truncate/drop partition operations to pgstat collector.
 * ----------------------------------------------------------------
 */
static void report_utility_time(void* parse_tree)
{
    ListCell* lc = NULL;
    RangeVar* relation = NULL;

    if (u_sess->attr.attr_sql.enable_save_datachanged_timestamp == false)
        return;

    if (!IS_SINGLE_NODE && !IS_PGXC_COORDINATOR)
        return;

    if (nodeTag(parse_tree) != T_CopyStmt && nodeTag(parse_tree) != T_AlterTableStmt)
        return;

    if (nodeTag(parse_tree) == T_CopyStmt) {
        CopyStmt* cs = (CopyStmt*)parse_tree;
        if (cs->is_from == false) {
            ereport(DEBUG1, (errmsg("\"copy to\" found")));
            return;
        }
        relation = cs->relation;
    }

    if (nodeTag(parse_tree) == T_AlterTableStmt) {
        AlterTableStmt* ats = (AlterTableStmt*)parse_tree;

        bool found = false;
        AlterTableCmd* cmd = NULL;
        foreach (lc, ats->cmds) {
            cmd = (AlterTableCmd*)lfirst(lc);
            found = foundPgstatPartititonOperations(cmd->subtype);
        }

        if (found == false) {
            ereport(DEBUG1, (errmsg("truncate/exchange/drop partition not found")));
            return;
        }

        relation = ats->relation;
    }

    if (relation == NULL) {
        ereport(DEBUG1, (errmsg("relation is NULL in CopyStmt/AlterTableStmt")));
        return;
    }

    MemoryContext current_ctx = CurrentMemoryContext;

    Relation rel = NULL;

    PG_TRY();
    {
        rel = heap_openrv(relation, AccessShareLock);

        Oid rid = rel->rd_id;

        if (rel->rd_rel->relkind == RELKIND_RELATION && rid >= FirstNormalObjectId) {
            if (rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT ||
                rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED) {
                pgstat_report_data_changed(rid, STATFLG_RELATION, rel->rd_rel->relisshared);
            }
        }

        heap_close(rel, AccessShareLock);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(current_ctx);

        ErrorData* edata = CopyErrorData();

        ereport(DEBUG1, (errmsg("Failed to send data changed time, cause: %s", edata->message)));

        FlushErrorState();

        FreeErrorData(edata);

        if (rel != NULL)
            heap_close(rel, AccessShareLock);
    }
    PG_END_TRY();
}

/*
 * CommandIsReadOnly: is an executable query read-only?
 *
 * This is a much stricter test than we apply for XactReadOnly mode;
 * the query must be *in truth* read-only, because the caller wishes
 * not to do CommandCounterIncrement for it.
 *
 * Note: currently no need to support Query nodes here
 */
bool CommandIsReadOnly(Node* parse_tree)
{
    if (IsA(parse_tree, PlannedStmt)) {
        PlannedStmt* stmt = (PlannedStmt*)parse_tree;

        switch (stmt->commandType) {
            case CMD_SELECT:
                if (stmt->rowMarks != NIL)
                    return false; /* SELECT FOR [KEY] UPDATE/SHARE */
                else if (stmt->hasModifyingCTE)
                    return false; /* data-modifying CTE */
                else
                    return true;
            case CMD_UPDATE:
            case CMD_INSERT:
            case CMD_DELETE:
            case CMD_MERGE:
                return false;
            default:
                elog(WARNING, "unrecognized commandType: %d", (int)stmt->commandType);
                break;
        }
    }
    /* For now, treat all utility commands as read/write */
    return false;
}

/*
 * check_xact_readonly: is a utility command read-only?
 *
 * Here we use the loose rules of XactReadOnly mode: no permanent effects
 * on the database are allowed.
 */
static void check_xact_readonly(Node* parse_tree)
{
    if (!u_sess->attr.attr_common.XactReadOnly)
        return;

    /*
     *Note:Disk space usage reach the threshold causing database open read-only.
     *In this case,if xc_maintenance_mode=on T_DropStmt and T_TruncateStmt is allowed
     *otherwise,just allow read-only stmt
     */
    if (u_sess->attr.attr_common.xc_maintenance_mode) {
        switch (nodeTag(parse_tree)) {
            case T_DropStmt:
            case T_TruncateStmt:
                return;
            default:
                break;
        }
    }

    /*
     * Note: Commands that need to do more complicated checking are handled
     * elsewhere, in particular COPY and plannable statements do their own
     * checking.  However they should all call PreventCommandIfReadOnly to
     * actually throw the error.
     */
    switch (nodeTag(parse_tree)) {
        case T_AlterDatabaseStmt:
        case T_AlterDatabaseSetStmt:
        case T_AlterDomainStmt:
        case T_AlterFunctionStmt:
        case T_AlterRoleSetStmt:
        case T_AlterObjectSchemaStmt:
        case T_AlterOwnerStmt:
        case T_AlterSchemaStmt:
        case T_AlterSeqStmt:
        case T_AlterTableStmt:
        case T_RenameStmt:
        case T_CommentStmt:
        case T_DefineStmt:
        case T_CreateCastStmt:
        case T_CreateConversionStmt:
        case T_CreatedbStmt:
        case T_CreateDomainStmt:
        case T_CreateFunctionStmt:
        case T_CreateRoleStmt:
        case T_IndexStmt:
        case T_CreatePLangStmt:
        case T_CreateOpClassStmt:
        case T_CreateOpFamilyStmt:
        case T_AlterOpFamilyStmt:
        case T_RuleStmt:
        case T_CreateSchemaStmt:
        case T_CreateSeqStmt:
        case T_CreateStmt:
        case T_CreateTableAsStmt:
        case T_RefreshMatViewStmt:
        case T_CreateTableSpaceStmt:
        case T_CreateTrigStmt:
        case T_CompositeTypeStmt:
        case T_CreateEnumStmt:
        case T_CreateRangeStmt:
        case T_AlterEnumStmt:
        case T_ViewStmt:
        case T_DropStmt:
        case T_DropdbStmt:
        case T_DropTableSpaceStmt:
        case T_DropRoleStmt:
        case T_GrantStmt:
        case T_GrantRoleStmt:
        case T_GrantDbStmt:
        case T_AlterDefaultPrivilegesStmt:
        case T_TruncateStmt:
        case T_DropOwnedStmt:
        case T_ReassignOwnedStmt:
        case T_AlterTSDictionaryStmt:
        case T_AlterTSConfigurationStmt:
        case T_CreateExtensionStmt:
        case T_AlterExtensionStmt:
        case T_AlterExtensionContentsStmt:
        case T_CreateFdwStmt:
        case T_AlterFdwStmt:
        case T_CreateForeignServerStmt:
        case T_AlterForeignServerStmt:
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
        case T_AlterTableSpaceOptionsStmt:
        case T_CreateForeignTableStmt:
        case T_SecLabelStmt:
        case T_CreateResourcePoolStmt:
        case T_AlterResourcePoolStmt:
        case T_DropResourcePoolStmt:
        case T_AlterGlobalConfigStmt:
        case T_DropGlobalConfigStmt:
        case T_CreatePolicyLabelStmt:
        case T_AlterPolicyLabelStmt:
        case T_DropPolicyLabelStmt:
        case T_CreateAuditPolicyStmt:
        case T_AlterAuditPolicyStmt:
        case T_DropAuditPolicyStmt:
        case T_CreateWeakPasswordDictionaryStmt:
        case T_DropWeakPasswordDictionaryStmt:        
        case T_CreateMaskingPolicyStmt:
        case T_AlterMaskingPolicyStmt:
        case T_DropMaskingPolicyStmt:
        case T_ClusterStmt:
        case T_ReindexStmt:
        case T_CreateDataSourceStmt:
        case T_AlterDataSourceStmt:
        case T_CreateDirectoryStmt:
        case T_DropDirectoryStmt:
        case T_CreateRlsPolicyStmt:
        case T_AlterRlsPolicyStmt:
        case T_CreateSynonymStmt:
        case T_DropSynonymStmt:
        case T_CreateClientLogicGlobal:
        case T_CreateClientLogicColumn:
        case T_CreatePackageStmt:
        case T_CreatePackageBodyStmt:
        case T_CreatePublicationStmt:
		case T_AlterPublicationStmt:
		case T_CreateSubscriptionStmt:
		case T_AlterSubscriptionStmt:
		case T_DropSubscriptionStmt:
            PreventCommandIfReadOnly(CreateCommandTag(parse_tree));
            break;
        case T_VacuumStmt: {
            VacuumStmt* stmt = (VacuumStmt*)parse_tree;
            /* on verify mode, do nothing */
            if (!(stmt->options & VACOPT_VERIFY)) {
                PreventCommandIfReadOnly(CreateCommandTag(parse_tree));
            }
            break;
        }
        case T_AlterRoleStmt: {
            AlterRoleStmt* stmt = (AlterRoleStmt*)parse_tree;
            if (!(DO_NOTHING != stmt->lockstatus && t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE)) {
                PreventCommandIfReadOnly(CreateCommandTag(parse_tree));
            }
            break;
        }
        default:
            /* do nothing */
            break;
    }
}

/*
 * PreventCommandIfReadOnly: throw error if XactReadOnly
 *
 * This is useful mainly to ensure consistency of the error message wording;
 * most callers have checked XactReadOnly for themselves.
 */
void PreventCommandIfReadOnly(const char* cmd_name)
{
    if (u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.replorigin_sesssion_origin == 0)
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                /* translator: %s is name of a SQL command, eg CREATE */
                errmsg("cannot execute %s in a read-only transaction", cmd_name)));
}

/*
 * PreventCommandDuringRecovery: throw error if RecoveryInProgress
 *
 * The majority of operations that are unsafe in a Hot Standby slave
 * will be rejected by XactReadOnly tests.	However there are a few
 * commands that are allowed in "read-only" xacts but cannot be allowed
 * in Hot Standby mode.  Those commands should call this function.
 */
void PreventCommandDuringRecovery(const char* cmd_name)
{
    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                /* translator: %s is name of a SQL command, eg CREATE */
                errmsg("cannot execute %s during recovery", cmd_name)));
}

/*
 * CheckRestrictedOperation: throw error for hazardous command if we're
 * inside a security restriction context.
 *
 * This is needed to protect session-local state for which there is not any
 * better-defined protection mechanism, such as ownership.
 */
static void CheckRestrictedOperation(const char* cmd_name)
{
    if (InSecurityRestrictedOperation())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                /* translator: %s is name of a SQL command, eg PREPARE */
                errmsg("cannot execute %s within security-restricted operation", cmd_name)));
}

/*
 * @NodeGroup Support
 * @Description: Determine the list objects is in same node group
 *
 * @Return: 'true' in same group, 'false' in different group
 */
static bool DropObjectsInSameNodeGroup(DropStmt* stmt)
{
    ListCell* cell = NULL;
    ListCell* cell2 = NULL;
    Oid group_oid = InvalidOid;
    Oid goid = InvalidOid;
    bool find_first = false;
    ObjectType object_type = stmt->removeType;
    Oid ins_group_oid;

    if (stmt->objects == NIL || list_length(stmt->objects) == 1) {
        return true;
    }

    ins_group_oid = ng_get_installation_group_oid();

    /* Scanning dropping list to error-out un-allowed cases */
    foreach (cell, stmt->objects) {
        ObjectAddress address;
        List* objname = (List*)lfirst(cell);
        List* objargs = NIL;
        Relation relation = NULL;
        if (stmt->arguments) {
            cell2 = (!cell2 ? list_head(stmt->arguments) : lnext(cell2));
            objargs = (List*)lfirst(cell2);
        }

        /* Get an ObjectAddress for the object. */
        address = get_object_address(stmt->removeType, objname, objargs, &relation, AccessShareLock, stmt->missing_ok);
        /* Issue NOTICE if supplied object was not found. */
        if (!OidIsValid(address.objectId)) {
            continue;
        }

        /* Fetching group_oid for given relation */
        if (object_type == OBJECT_FUNCTION) {
            goid = GetFunctionNodeGroupByFuncid(address.objectId);
            if (!OidIsValid(goid))
                goid = ins_group_oid;
        } else if (OBJECT_IS_SEQUENCE(object_type)) {
            Oid tableId = InvalidOid;
            int32 colId;
            if (!sequenceIsOwned(address.objectId, &tableId, &colId))
                goid = ins_group_oid;
            else
                goid = ng_get_baserel_groupoid(tableId, RELKIND_RELATION);
        }

        /* Close relation if necessary */
        if (relation)
            relation_close(relation, AccessShareLock);

        if (!find_first) {
            group_oid = goid;
            find_first = true;
            continue;
        }

        if (goid != group_oid) {
            return false;
        }
    }

    return true;
}

/*
 * @NodeGroup Support
 * @Description: Get  same node group for grant objects,
 * Only for tables, foreign tables, sequences and functions,
 * Caller should not call GrantStmtGetNodeGroup for other object type.
 *
 * @Return: InvalidOid means different group, or same group oid.
 */
static Oid GrantStmtGetNodeGroup(GrantStmt* stmt)
{
    Oid goid = InvalidOid;
    Oid ins_group_oid = InvalidOid;
    List* oids = NIL;
    bool find_first = false;
    ListCell* cell = NULL;
    Oid group_oid = InvalidOid;

    /* Collect the OIDs of the target objects */
    oids = GrantStmtGetObjectOids(stmt);

    ins_group_oid = ng_get_installation_group_oid();

    /* Scanning dropping list to error-out un-allowed cases */
    foreach (cell, oids) {
        Oid object_oid = lfirst_oid(cell);
        char relkind;

        /* Issue NOTICE if supplied object was not found. */
        if (!OidIsValid(object_oid)) {
            continue;
        }

        if (stmt->objtype == ACL_OBJECT_FUNCTION) {
            goid = GetFunctionNodeGroupByFuncid(object_oid);
            if (!OidIsValid(goid))
                goid = ins_group_oid;
        } else if (stmt->objtype == ACL_OBJECT_RELATION || stmt->objtype == ACL_OBJECT_SEQUENCE) {
            relkind = get_rel_relkind(object_oid);

            /* Fetching group_oid for given relation */
            if (relkind == RELKIND_RELATION || relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_STREAM) {
                goid = ng_get_baserel_groupoid(object_oid, RELKIND_RELATION);
            } else {
                /* maybe views, sequences */
                continue;
            }
        } else {
            /* Should not enter here */
            Assert(false);
        }

        if (!find_first) {
            group_oid = goid;
            find_first = true;
            continue;
        }

        if (goid != group_oid) {
            list_free(oids);
            return InvalidOid;
        }
    }

    list_free(oids);

    return find_first ? group_oid : ins_group_oid;
}

/*
 * @NodeGroup Support
 * @Description: Create ExecNodes according node group oid.
 *
 * @Return: ExecNodes.
 */
static ExecNodes* GetNodeGroupExecNodes(Oid group_oid)
{
    ExecNodes* exec_nodes = NULL;
    Oid* members = NULL;
    int nmembers;
    bool need_full_dn = false;

    exec_nodes = makeNode(ExecNodes);

    if (!in_logic_cluster()) {
        char* group_name = get_pgxc_groupname(group_oid);
        if (group_name == NULL) {
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("computing nodegroup is not a valid group.")));
        }
        need_full_dn = need_full_dn_execution(group_name);
        pfree_ext(group_name);
    }

    if (need_full_dn) {
        exec_nodes->nodeList = GetAllDataNodes();
        return exec_nodes;
    }

    nmembers = get_pgxc_groupmembers_redist(group_oid, &members);

    exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);

    pfree_ext(members);

    return exec_nodes;
}

/*
 * @NodeGroup Support
 * @Description: Get exec nodes according table that own the sequence.
 *
 * @Return: exec nodes of the sequence.
 */
static ExecNodes* GetOwnedByNodes(Node* seq_stmt)
{
    Oid goid;
    Oid table_id;
    List* owned_by = NIL;
    List* rel_name = NIL;
    ListCell* option = NULL;
    int nnames;
    RangeVar* rel = NULL;
    List* options = NULL;

    Assert(seq_stmt != NULL);

    if (IsA(seq_stmt, CreateSeqStmt))
        options = ((CreateSeqStmt*)seq_stmt)->options;
    else if (IsA(seq_stmt, AlterSeqStmt))
        options = ((AlterSeqStmt*)seq_stmt)->options;
    else
        return NULL;

    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "owned_by") == 0) {
            owned_by = defGetQualifiedName(defel);
            break;
        }
    }

    if (owned_by == NULL)
        return NULL;

    nnames = list_length(owned_by);
    Assert(nnames > 0);
    if (nnames == 1) {
        /* Must be OWNED BY NONE */
        return NULL;
    }

    /* Separate rel_name */
    rel_name = list_truncate(list_copy(owned_by), nnames - 1);
    rel = makeRangeVarFromNameList(rel_name);
    if (rel->schemaname != NULL)
        table_id = get_valid_relname_relid(rel->schemaname, rel->relname);
    else
        table_id = RelnameGetRelid(rel->relname);

    Assert(OidIsValid(table_id));

    /* Fetching group_oid for given relation */
    goid = get_pgxc_class_groupoid(table_id);
    if (!OidIsValid(goid))
        return NULL;

    return GetNodeGroupExecNodes(goid);
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @NodeGroup Support
 * @Description: Get exec nodes according table that own the sequence.
 *
 * @Return: exec nodes of the sequence.
 */
static ExecNodes* GetSequenceNodes(RangeVar* sequence, bool missing_ok)
{
    Oid goid;
    Oid seq_id;
    Oid table_id = InvalidOid;
    int32 col_id = 0;

    seq_id = RangeVarGetRelid(sequence, NoLock, missing_ok);

    if (!OidIsValid(seq_id))
        return NULL;

    if (!sequenceIsOwned(seq_id, &table_id, &col_id))
        return NULL;

    /* Fetching group_oid for given relation */
    goid = get_pgxc_class_groupoid(table_id);
    if (!OidIsValid(goid))
        return NULL;

    return GetNodeGroupExecNodes(goid);
}
#endif

/*
 * @NodeGroup Support
 * @Description: Get create function query string in datanodes.
 *
 * @Return: new query string for sql language in logic cluster, or old query string.
 */
static const char* GetCreateFuncStringInDN(CreateFunctionStmt* stmt, const char* query_string)
{
    errno_t rc;
    ListCell* option = NULL;
    bool is_sql_lang = false;
    size_t str_len;
    char* tmp = NULL;
    const char* query_str = query_string;

    if (!in_logic_cluster() || !u_sess->attr.attr_sql.check_function_bodies)
        return query_string;

    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "language") == 0) {
            if (strcmp(strVal(defel->arg), "sql") == 0) {
                is_sql_lang = true;
                break;
            }
        }
    }

    if (!is_sql_lang)
        return query_string;

    str_len = strlen(query_string) + 128;
    tmp = (char*)palloc(str_len);

    rc = snprintf_s(
        tmp, str_len, str_len - 1, "SET check_function_bodies = off;%s;SET check_function_bodies = on;", query_string);
    securec_check_ss(rc, "\0", "\0");

    query_str = tmp;

    return query_str;
}

/*
 * @NodeGroup Support
 * @Description: Determine the list objects is in same node group
 *
 * @Return: 'true' in same group, 'false' in different group
 */
static ExecNodes* GetFunctionNodes(Oid func_id)
{
    Oid goid;

    /* Fetching group_oid for given relation */
    goid = GetFunctionNodeGroupByFuncid(func_id);
    if (!OidIsValid(goid))
        return NULL;

    return GetNodeGroupExecNodes(goid);
}

/*
 * @NodeGroup Support
 * @Description: Determine the list objects is in same node group
 *
 * @Return: 'true' in same group, 'false' in different group
 */
static ExecNodes* GetDropFunctionNodes(DropStmt* stmt)
{
    ListCell* cell = NULL;
    ListCell* cell2 = NULL;

    if (stmt->objects == NIL) {
        return NULL;
    }

    /* Scanning dropping list to error-out un-allowed cases */
    foreach (cell, stmt->objects) {
        ObjectAddress address;
        List* obj_name = (List*)lfirst(cell);
        List* obj_args = NIL;
        Relation relation = NULL;

        if (stmt->arguments) {
            cell2 = (!cell2 ? list_head(stmt->arguments) : lnext(cell2));
            obj_args = (List*)lfirst(cell2);
        }

        /* Get an ObjectAddress for the object. */
        address = get_object_address(stmt->removeType, obj_name, obj_args, &relation, AccessShareLock, stmt->missing_ok);

        if (relation)
            heap_close(relation, AccessShareLock);

        /* Issue NOTICE if supplied object was not found. */
        if (!OidIsValid(address.objectId)) {
            continue;
        }

        return GetFunctionNodes(address.objectId);
    }

    return NULL;
}

/*
 *  Check whether current user can create table in logic cluster.
 */
static void check_logic_cluster_create_priv(Oid group_oid, const char* group_name)
{
    Oid redist_oid;
    Oid current_group_oid;
    const char* redist_group_name = NULL;

    char group_kind = get_pgxc_groupkind(group_oid);
    if (group_kind == 'i' || group_kind == 'e') {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unable to create table on installation group and elastic group "
                       "in logic cluster.")));
    }
    if (superuser())
        return;

    /* check whether current user attach to logic cluster,if not return ERROR */
    current_group_oid = get_current_lcgroup_oid();
    if (group_oid == current_group_oid) {
        return;
    }
    if (!OidIsValid(current_group_oid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("User \"%s\" need to attach to logic cluster \"%s\" to create table.",
                    GetUserNameFromId(GetUserId()),
                    group_name)));

        return;
    }

    redist_group_name = PgxcGroupGetInRedistributionGroup();
    redist_oid = get_pgxc_groupoid(redist_group_name);
    if (redist_oid == current_group_oid) {
        if (group_oid == PgxcGroupGetRedistDestGroupOid()) {
            return;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("User \"%s\" have no privilege to create table on logic cluster \"%s\".",
                GetUserNameFromId(GetUserId()),
                group_name)));
}

/*
 * append_internal_data_to_query
 *		append internalData to query_string
 *
 *	internalData: the string needed to append the the end of query_string
 *	query_string: original source text of command
 *
 *    If there are some semicolons in the end of query_string, change these semicolons to space
 */
static char* append_internal_data_to_query(char* internal_data, const char* query_string)
{
    int i;
    StringInfoData str;
    initStringInfo(&str);
    Assert(query_string != NULL);
    Assert(internal_data != NULL);

    appendStringInfoString(&str, query_string);

    /* remove the last semicolon if exist. */
    for (i = str.len - 1; i >= 0; i--) {
        if (str.data[i] == ';') {
            str.data[i] = ' ';
            continue;
        }

        if (!isspace(str.data[i]))
            break;
    }

    appendStringInfo(&str, " INTERNAL DATA %s ;", internal_data);

    return str.data;
}

/*
 * assemble_create_sequence_msg
 *		assemble CREATE SEQUENCE query string for create sequences.
 *
 *	stmts: CreateSeqStmt list
 *	uuids: uuids of sequences
 *
 *    return string including multiple CREATE SEQUENCE statements.
 */
static char* assemble_create_sequence_msg(List* stmts, List* uuids)
{
    char* msg = NULL;
    ListCell* l = NULL;
    StringInfoData str;

    initStringInfo(&str);

    foreach (l, stmts) {
        Node* stmt = (Node*)lfirst(l);

        if (IsA(stmt, CreateSeqStmt)) {
            char query[256];
            errno_t rc = EOK;
            CreateSeqStmt* create_seq_stmt = (CreateSeqStmt*)stmt;
            const char* schema_name = NULL;
            const char* seq_name = quote_identifier(create_seq_stmt->sequence->relname);

            if (create_seq_stmt->sequence->schemaname && create_seq_stmt->sequence->schemaname[0])
                schema_name = quote_identifier(create_seq_stmt->sequence->schemaname);

            if (schema_name == NULL) {
                rc = snprintf_s(query, sizeof(query), sizeof(query) - 1, "CREATE SEQUENCE %s;", seq_name);
            } else {
                rc = snprintf_s(query, sizeof(query), sizeof(query) - 1, "CREATE SEQUENCE %s.%s;", schema_name, seq_name);
            }
            securec_check_ss(rc, "\0", "\0");

            appendStringInfoString(&str, query);

            if (OidIsValid(create_seq_stmt->ownerId)) {
                char* rol_name = GetUserNameFromId(create_seq_stmt->ownerId);

                /* Don't need to check rol_name by quote_identifier. */
                if (schema_name == NULL) {
                    rc = snprintf_s(
                        query, sizeof(query), sizeof(query) - 1, "ALTER SEQUENCE %s OWNER TO %s;", seq_name, rol_name);
                } else {
                    rc = snprintf_s(query,
                        sizeof(query),
                        sizeof(query) - 1,
                        "ALTER SEQUENCE %s.%s OWNER TO %s;",
                        schema_name,
                        seq_name,
                        rol_name);
                }
                securec_check_ss(rc, "\0", "\0");

                appendStringInfoString(&str, query);
                pfree_ext(rol_name);
            }

            if (schema_name != NULL && schema_name != create_seq_stmt->sequence->schemaname)
                pfree_ext(schema_name);
            if (seq_name != create_seq_stmt->sequence->relname)
                pfree_ext(seq_name);
        }
    }

    char* uuid_info = nodeToString(uuids);
    AssembleHybridMessage(&msg, str.data, uuid_info);

    pfree_ext(uuid_info);
    pfree_ext(str.data);

    return msg;
}

/*
 * assemble_drop_sequence_msg
 *		assemble DROP SEQUENCE query string.
 *
 *	seqs: sequence oid list.
 *	str:  out parameter, return drop sequence string.
 *
 */
static void assemble_drop_sequence_msg(List* seqs, StringInfo str)
{
    Oid seq_id;
    ListCell* s = NULL;
    const char* rel_name = NULL;
    const char* nsp_name = NULL;
    const char* schema_name = NULL;
    const char* seq_name = NULL;
    char query[256];
    errno_t rc = EOK;

    foreach (s, seqs) {
        seq_id = lfirst_oid(s);

        rel_name = get_rel_name(seq_id);
        nsp_name = get_namespace_name(get_rel_namespace(seq_id));
        schema_name = quote_identifier(nsp_name);
        seq_name = quote_identifier(rel_name);

        Assert(seq_name != NULL && schema_name != NULL);

        rc = snprintf_s(
            query, sizeof(query), sizeof(query) - 1, "DROP SEQUENCE IF EXISTS %s.%s CASCADE;", schema_name, seq_name);
        securec_check_ss(rc, "\0", "\0");

        if (str->data == NULL) {
            initStringInfo(str);
        }
        appendStringInfoString(str, query);

        if (schema_name != nsp_name)
            pfree_ext(schema_name);
        if (seq_name != rel_name)
            pfree_ext(seq_name);
        pfree_ext(rel_name);
        pfree_ext(nsp_name);
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * get_drop_sequence_msg
 *		Get SQL including multiple DROP SEQUENCE;
 *
 *	objects: the normal table oid list.
 *
 *    If all tables in objects have not owned sequences, return null;
 *    or the return string include DROP SEQUENCE for all sequences
 *    owned by these tables.
 */
static char* get_drop_sequence_msg(List* objects)
{
    ListCell* l = NULL;
    StringInfoData str;
    List* seqs = NULL;
    Oid rel_id;

    str.data = NULL;

    foreach (l, objects) {
        rel_id = lfirst_oid(l);
        seqs = getOwnedSequences(rel_id);
        if (seqs == NULL)
            continue;
        assemble_drop_sequence_msg(seqs, &str);
    }

    return str.data;
}
#endif

/*
 * make_remote_query_for_seq
 *		return RemoteQuery for executing query_string in nodes without excluded_nodes.
 *
 *	excluded_nodes: the node list that don't need to execute query.
 *	query_string: SQL command to execute.
 *
 */
static RemoteQuery* make_remote_query_for_seq(ExecNodes* excluded_nodes, char* query_string)
{
    List* all_nodes = GetAllDataNodes();
    List* exec_nodes = list_difference_int(all_nodes, excluded_nodes->nodeList);

    RemoteQuery* step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->sql_statement = (char*)query_string;
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = false;
    step->exec_nodes = makeNode(ExecNodes);
    step->exec_nodes->nodeList = exec_nodes;
    list_free(all_nodes);
    return step;
}

#ifdef ENABLE_MULTIPLE_NODES

/*
 * exec_remote_query_4_seq
 *		Execute query_string in nodes without excluded_nodes.
 *
 *	excluded_nodes: the node list that don't need to execute query.
 *	query_string: SQL command to execute.
 *    uuid: Sequence uuid, if uuid is zero, ignore the parameter.
 *
 */
static void exec_remote_query_4_seq(ExecNodes* excluded_nodes, char* query_string, int64 uuid)
{
    char* query_string_with_uuid = query_string;
    if (uuid != INVALIDSEQUUID) {
        Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(uuid), false, true);
        char* uuid_info = nodeToString(n);

        AssembleHybridMessage(&query_string_with_uuid, query_string, uuid_info);
        pfree_ext(n);
        pfree_ext(uuid_info);
    }

    RemoteQuery* step = make_remote_query_for_seq(excluded_nodes, query_string_with_uuid);
    ExecRemoteUtility(step);

    list_free(step->exec_nodes->nodeList);
    pfree_ext(step->exec_nodes);
    pfree_ext(step);
}

/*
 * drop_sequence_4_node_group
 *		Drop sequences not in datanodes (in parameter excluded_nodes).
 *
 *   stmt: DropStmt struct, we get all dropped tables from stmt parameter.
 *	excluded_nodes: the node list that don't need to execute query.
 *
 *   The function is used to DROP TABLE in nodegroup.
 *   DROP TABLE don't drop sequences in datanodes that don't belong to the NodeGroup,
 *   so we need this function to drop these sequences in the execluded nodes.
 */
static void drop_sequence_4_node_group(DropStmt* stmt, ExecNodes* excluded_nodes)
{
    char* query_string = NULL;
    List* objects = NIL;
    ListCell* cell = NULL;
    Oid table_oid;

    if (excluded_nodes == NULL)
        return;

    if (stmt->removeType != OBJECT_TABLE)
        return;

    if (excluded_nodes->nodeList->length == u_sess->pgxc_cxt.NumDataNodes)
        return;

    foreach (cell, stmt->objects) {
        RangeVar* rel = NULL;

        rel = makeRangeVarFromNameList((List*)lfirst(cell));
        table_oid = RangeVarGetRelid(rel, NoLock, true);
        if (!OidIsValid(table_oid)) {
            continue;
        }

        if (get_rel_relkind(table_oid) != RELKIND_RELATION) {
            continue;
        }

        objects = lappend_oid(objects, table_oid);
    }

    query_string = get_drop_sequence_msg(objects);
    if (query_string == NULL)
        return;

    exec_remote_query_4_seq(excluded_nodes, query_string, INVALIDSEQUUID);
    pfree_ext(query_string);
}
/*
 * alter_sequence_all_nodes
 *		ALTER sequences not in datanodes (in parameter excluded_nodes).
 *
 *   seq_stmt: AlterSeqStmt struct, we get all altered sequences from this parameter.
 *	excluded_nodes: the node list that don't need to execute query.
 *
 *   The function is used to ALTER SEQUENCE in nodegroup.
 *
 *   Consider the scenario:
 *   Sequence s1 is owned by t1 and increment is 1;
 *   table t1 belong to nodegroup1 (datanode1,datanode2);
 *   table t2 belong to nodegroup2 (datanode3,datanode4).
 *   If execute ALTER SEQUENCE s1 increment by 2 owned by t2, how we do ?
 *
 *   We have to call alter_sequence_all_nodes in nodes excluding nodegroup2, modify
 *   increment and set owned by none first. Then we ALTER SEQUENCE in nodegroup2,
 *   modify increment and set owned by t2. The final result is: increment in all nodes is 2,
 *   owned by is none excluding nodegroup2, owned by t2 in nodegroup2.
 */
static void alter_sequence_all_nodes(AlterSeqStmt* seq_stmt, ExecNodes* exclude_nodes)
{
    if (exclude_nodes == NULL)
        return;

    if (exclude_nodes->nodeList->length == u_sess->pgxc_cxt.NumDataNodes)
        return;

    RangeVar* sequence = seq_stmt->sequence;

    /* Optimization for ALTER SEQUENCE OWNED BY. */
    if (seq_stmt->options->length == 1) {
        DefElem* def_elem = (DefElem*)linitial(seq_stmt->options);
        if (strcmp(def_elem->defname, "owned_by") == 0) {
            ExecNodes* exec_nodes = GetSequenceNodes(sequence, true);
            if (exec_nodes == NULL)
                return;

            /* Judge whether current sequence nodegroup is same as new sequence nodegroup */
            List* diff = list_difference_int(exec_nodes->nodeList, exclude_nodes->nodeList);
            if (diff == NULL) {
                FreeExecNodes(&exec_nodes);
                return;
            }
            list_free(diff);
        }
    }

    /* Get ALTER SEQUENCE statement with OWNED BY NONE */
    char* msg = deparse_alter_sequence((Node*)seq_stmt, true);

    exec_remote_query_4_seq(exclude_nodes, msg, INVALIDSEQUUID);

    pfree_ext(msg);
}
#endif
/*
 * get_drop_seq_query_string
 *		Get DROP SEQUENCE string list (used in ALTER TABLE ... DROP COLUMN).
 *
 *   stmt: AlterTableStmt struct, we get all dropped sequences from this parameter.
 *   rel_id: the normal table oid. We can get it from AlterTableStmt, but we expect
 *                  caller can give the oid for optimization.
 *
 *   The function is used to ALTER TABLE ... DROP COLUMN in nodegroup.
 *   When drop column with owned sequence for nodegroup table, DROP TABLE
 *   is executed only in nodes in the nodegroup, sequence in other datanodes won't
 *   be dropped. We have to call get_drop_seq_query_string to assemble DROP SEQUENCE
 *   statements, execute these sqls by exec_remote_query_4_seq in other datanodes.
 */
static char* get_drop_seq_query_string(AlterTableStmt* stmt, Oid rel_id)
{
    HeapTuple tuple;
    char* col_name = NULL;
    ListCell* cell = NULL;
    AlterTableCmd* cmd = NULL;
    List* seq_list = NIL;
    List* attr_list = NIL;

    foreach (cell, stmt->cmds) {
        cmd = (AlterTableCmd*)lfirst(cell);
        if (cmd->subtype == AT_DropColumn || cmd->subtype == AT_DropColumnRecurse) {
            Form_pg_attribute target_att;
            col_name = cmd->name;
            /*
             * get the number of the attribute
             */
            tuple = SearchSysCacheAttName(rel_id, col_name);
            if (!HeapTupleIsValid(tuple))
                continue;

            target_att = (Form_pg_attribute)GETSTRUCT(tuple);
            attr_list = lappend_int(attr_list, target_att->attnum);
            ReleaseSysCache(tuple);
        }
    }

    if (attr_list != NIL) {
        StringInfoData str;
        seq_list = getOwnedSequences(rel_id, attr_list);
        list_free(attr_list);

        if (seq_list != NULL) {
            str.data = NULL;
            assemble_drop_sequence_msg(seq_list, &str);
            return str.data;
        }
    }

    return NULL;
}

/*
 * ProcessUtility
 *		general utility function invoker
 *
 *	parse_tree: the parse tree for the utility statement
 *	query_string: original source text of command
 *	params: parameters to use during execution
 *	is_top_level: true if executing a "top level" (interactively issued) command
 *	dest: where to send results
 *	completion_tag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *		in which to store a command completion status string.
 *
 * Notes: as of PG 8.4, caller MUST supply a query_string; it is not
 * allowed anymore to pass NULL.  (If you really don't have source text,
 * you can pass a constant string, perhaps "(query not available)".)
 *
 * completion_tag is only set nonempty if we want to return a nondefault status.
 *
 * completion_tag may be NULL if caller doesn't want a status string.
 */
void ProcessUtility(Node* parse_tree, const char* query_string, ParamListInfo params, bool is_top_level, DestReceiver* dest,
#ifdef PGXC
    bool sent_to_remote,
#endif /* PGXC */
    char* completion_tag,
    bool isCTAS)
{
    /* required as of 8.4 */
    AssertEreport(query_string != NULL, MOD_EXECUTOR, "query string is NULL");

    /*
     * We provide a function hook variable that lets loadable plugins get
     * control when ProcessUtility is called.  Such a plugin would normally
     * call standard_ProcessUtility().
     * it's unsafe to deal with plugins hooks as dynamic lib may be released
     */
    if (ProcessUtility_hook && !(g_instance.status > NoShutdown))
        (*ProcessUtility_hook)(parse_tree,
            query_string,
            params,
            is_top_level,
            dest,
#ifdef PGXC
            sent_to_remote,
#endif /* PGXC */
            completion_tag,
            isCTAS);
    else
        standard_ProcessUtility(parse_tree,
            query_string,
            params,
            is_top_level,
            dest,
#ifdef PGXC
            sent_to_remote,
#endif /* PGXC */
            completion_tag,
            isCTAS);
}

// @Temp Table.
// Check if a SQL stmt contain temp tables, to decide if lock other coordinators
bool isAllTempObjects(Node* parse_tree, const char* query_string, bool sent_to_remote)
{
    switch (nodeTag(parse_tree)) {
        case T_CreateStmt: {
            CreateStmt* stmt = (CreateStmt*)parse_tree;
            char* temp_namespace_name = NULL;
            if (stmt->relation) {
                if (OidIsValid(u_sess->catalog_cxt.myTempNamespace) && stmt->relation->schemaname != NULL) {
                    temp_namespace_name = get_namespace_name(u_sess->catalog_cxt.myTempNamespace);
                    if (temp_namespace_name != NULL &&
                        strcmp(stmt->relation->schemaname, temp_namespace_name) == 0)
                        return true;
                }

                if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
                    return true;
            }
            break;
        }
        case T_ViewStmt: {

            return IsViewTemp((ViewStmt*)parse_tree, query_string);
            break;
        }
        case T_AlterFunctionStmt:  // @Temp Table. alter function's lock check is moved in AlterFunction
        {
            return IsFunctionTemp((AlterFunctionStmt*)parse_tree);
            break;
        }
        case T_DropStmt:
            switch (((DropStmt*)parse_tree)->removeType) {
                case OBJECT_INDEX:
                case OBJECT_TABLE:
                case OBJECT_VIEW:
                case OBJECT_CONTQUERY:
                case OBJECT_SEQUENCE:
                case OBJECT_LARGE_SEQUENCE: {
                    bool is_all_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
                    DropStmt* new_stmt = (DropStmt*)copyObject(parse_tree);
                    new_stmt->missing_ok = true;

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment(new_stmt, query_string, sent_to_remote, &is_all_temp, &exec_type);

                    pfree_ext(new_stmt);
                    return is_all_temp;
                }
                case OBJECT_SCHEMA: {
                    ListCell* cell = NULL;

                    foreach (cell, ((DropStmt*)parse_tree)->objects) {
                        List* obj_name = (List*)lfirst(cell);
                        char* name = NameListToString(obj_name);
                        if (isTempNamespaceName(name) || isToastTempNamespaceName(name))
                            return true;
                    }

                    break;
                }
                default: {
                    bool is_all_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
                    DropStmt* new_stmt = (DropStmt*)copyObject(parse_tree);
                    new_stmt->missing_ok = true;

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment(new_stmt, query_string, sent_to_remote, &is_all_temp, &exec_type);
                    pfree_ext(new_stmt);
                    return is_all_temp;
                }
            }

            break;
        case T_CreateSchemaStmt: {
            char* nsp_name = ((CreateSchemaStmt*)parse_tree)->schemaname;
            if (isTempNamespaceName(nsp_name) || isToastTempNamespaceName(nsp_name))
                return true;
            break;
        }
        case T_AlterSchemaStmt: {
            char* nsp_name = ((AlterSchemaStmt*)parse_tree)->schemaname;
            if (isTempNamespaceName(nsp_name) || isToastTempNamespaceName(nsp_name))
                return true;
            break;
        }

        case T_AlterTableStmt: {
            AlterTableStmt* alter_stmt = (AlterTableStmt*)parse_tree;
            Oid rel_id;
            LOCKMODE lock_mode;

            /*
             * Figure out lock mode, and acquire lock.	This also does
             * basic permissions checks, so that we won't wait for a lock
             * on (for example) a relation on which we have no
             * permissions.
             */
            lock_mode = AlterTableGetLockLevel(alter_stmt->cmds);
            rel_id = AlterTableLookupRelation(alter_stmt, lock_mode, true);

            if (OidIsValid(rel_id) && IsTempTable(rel_id)) {
                return true;
            }
            if (OidIsValid(rel_id) && u_sess->attr.attr_sql.enable_parallel_ddl)
                UnlockRelationOid(rel_id, lock_mode);

            break;
        }
        case T_RenameStmt: {
            RenameStmt* stmt = (RenameStmt*)parse_tree;
            bool is_all_temp = false;
            if (stmt->relation) {
                /*
                 * When a relation is defined, it is possible that this object does
                 * not exist but an IF EXISTS clause might be used. So we do not do
                 * any error check here but block the access to remote nodes to
                 * this object as it does not exisy
                 */
                Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                if (OidIsValid(rel_id)) {
                    (void)ExecUtilityFindNodes(stmt->renameType, rel_id, &is_all_temp);
                    UnlockRelationOid(rel_id, AccessShareLock);
                }
                return is_all_temp;
            }
            break;
        }
        case T_AlterObjectSchemaStmt: {
            AlterObjectSchemaStmt* stmt = (AlterObjectSchemaStmt*)parse_tree;
            bool is_all_temp = false;

            /* Try to use the object relation if possible */
            if (stmt->relation) {
                /*
                 * When a relation is defined, it is possible that this object does
                 * not exist but an IF EXISTS clause might be used. So we do not do
                 * any error check here but block the access to remote nodes to
                 * this object as it does not exisy
                 */
                Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                if (OidIsValid(rel_id)) {
                    (void)ExecUtilityFindNodes(stmt->objectType, rel_id, &is_all_temp);
                    UnlockRelationOid(rel_id, AccessShareLock);
                }
                return is_all_temp;
            }

            break;
        }
        case T_CommentStmt: {
            bool is_all_temp = false;
            CommentStmt* stmt = (CommentStmt*)parse_tree;
            (void)get_nodes_4_comment_utility(stmt, &is_all_temp, NULL);
            return is_all_temp;
        }
        case T_GrantStmt: {
            GrantStmt* stmt = (GrantStmt*)parse_tree;
            bool is_temp = false;
            bool has_temp = false;
            bool has_nontemp = false;

            /* Launch GRANT on Coordinator if object is a sequence */
            if ((stmt->objtype == ACL_OBJECT_RELATION && stmt->targtype == ACL_TARGET_OBJECT)) {
                /*
                 * In case object is a relation, differenciate the case
                 * of a sequence, a view and a table
                 */
                ListCell* cell = NULL;
                /* Check the list of objects */
                foreach (cell, stmt->objects) {
                    RangeVar* rel_var = (RangeVar*)lfirst(cell);
                    Oid rel_id = RangeVarGetRelid(rel_var, NoLock, true);
                    /* Skip if object does not exist */
                    if (!OidIsValid(rel_id)) {
                        continue;
                    }
                    (void)exec_utility_find_nodes_relkind(rel_id, &is_temp);
                    if (is_temp) {
                        has_temp = true;
                    } else {
                        has_nontemp = true;
                    }

                    if (has_temp && has_nontemp) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Grant not supported for TEMP and non-TEMP objects together"),
                                errdetail("You should separate TEMP and non-TEMP objects")));
                    }
                }
                return has_temp;
            }
            break;
        }
        case T_IndexStmt: {
            IndexStmt* stmt = (IndexStmt*)parse_tree;
            Oid rel_id;
            bool is_temp = false;

            /* INDEX on a temporary table cannot use 2PC at commit */
            rel_id = RangeVarGetRelidExtended(stmt->relation, AccessShareLock, true, false, false, true, NULL, NULL);

            if (OidIsValid(rel_id)) {
                (void)ExecUtilityFindNodes(OBJECT_INDEX, rel_id, &is_temp);
                UnlockRelationOid(rel_id, AccessShareLock);
            }
            return is_temp;
        }
        case T_RuleStmt: {
            bool is_temp = false;
            (void)get_nodes_4_rules_utility(((RuleStmt*)parse_tree)->relation, &is_temp);
            return is_temp;
        }
        case T_CreateSeqStmt: {
            CreateSeqStmt* stmt = (CreateSeqStmt*)parse_tree;
            bool is_temp = false;

            /* In case this query is related to a SERIAL execution, just bypass */
            if (!stmt->is_serial)
                is_temp = stmt->sequence->relpersistence == RELPERSISTENCE_TEMP;

            return is_temp;
        }
        case T_CreateTrigStmt: {
            CreateTrigStmt* stmt = (CreateTrigStmt*)parse_tree;
            bool is_temp = false;
            Oid rel_id = RangeVarGetRelidExtended(stmt->relation, NoLock, false, false, false, true, NULL, NULL);
            if (OidIsValid(rel_id))
                (void)ExecUtilityFindNodes(OBJECT_TABLE, rel_id, &is_temp);

            return is_temp;
        }
        case T_VacuumStmt: {
            return IsAllTempObjectsInVacuumStmt(parse_tree);
        }
        case T_AlterSeqStmt:
        case T_AlterOwnerStmt:
        default:
            break;
    }
    return false;
}

/* Find the first execute CN on parallel_ddl_mode.  */
char* find_first_exec_cn()
{
    char* result = NULL;

    result = u_sess->pgxc_cxt.co_handles[0].remoteNodeName;

    for (int i = 1; i < u_sess->pgxc_cxt.NumCoords; i++) {
        result = (strcmp(u_sess->pgxc_cxt.co_handles[i].remoteNodeName, result) < 0) ?
            u_sess->pgxc_cxt.co_handles[i].remoteNodeName :
            result;
    }

    return result;
}

/* Find with(hashbucket) options in all stmts */
bool find_hashbucket_options(List* stmts)
{
    ListCell* l = NULL;

    foreach (l, stmts) {
        Node* stmt = (Node*)lfirst(l);
        ListCell* opt = NULL;
        List* user_options = NULL;

        if (!IsA(stmt, CreateStmt)) {
            continue;
        }

        /* Debug mode, always return true for CreateStmt */
        if (u_sess->attr.attr_storage.enable_hashbucket) {
            return true;
        }

        user_options = ((CreateStmt*)stmt)->options;

        foreach (opt, user_options) {
            DefElem* def = (DefElem*)lfirst(opt);
            char* lower_string = lowerstr(def->defname);

            if (strstr(lower_string, "hashbucket") || 
                strstr(lower_string, "bucketcnt")) {
                return true;
            }
        }
    }

    return false;
}


/*
 * Notice: parse_tree could be from cached plan, do not modify it under other memory context
 */
#ifdef PGXC
void CreateCommand(CreateStmt *parse_tree, const char *query_string, ParamListInfo params, 
                   bool is_top_level, bool sent_to_remote, bool isCTAS)
#else
void CreateCommand(CreateStmt *parse_tree, const char *query_string, ParamListInfo params, bool is_top_level,
    bool isCTAS)
#endif

{
    List* stmts = NIL;
    ListCell* l = NULL;
    Oid rel_oid;
#ifdef PGXC
    bool is_temp = false;
    bool is_object_temp = false;
    PGXCSubCluster* sub_cluster = NULL;
    char* tablespace_name = NULL;
    char relpersistence = RELPERSISTENCE_PERMANENT;
    bool table_is_exist = false;
    char* internal_data = NULL;
    List* uuids = (List*)copyObject(parse_tree->uuids);

    char* first_exec_node = NULL;
    bool is_first_node = false;
    char* query_string_with_info = (char*)query_string;
    char* query_string_with_data = (char*)query_string;
    Oid namespace_id = InvalidOid;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        first_exec_node = find_first_exec_cn();
        is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
    }
#endif

    /*
     * DefineRelation() needs to know "isTopLevel"
     * by "DfsDDLIsTopLevelXact" to prevent "create hdfs table" running
     * inside a transaction block.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        u_sess->exec_cxt.DfsDDLIsTopLevelXact = is_top_level;

    /* Run parse analysis ... */
    if (u_sess->attr.attr_sql.enable_parallel_ddl)
        stmts = transformCreateStmt((CreateStmt*)parse_tree, query_string, NIL, true, &namespace_id, is_first_node);
    else
        stmts = transformCreateStmt((CreateStmt*)parse_tree, query_string, NIL, false, &namespace_id);

    /*
     * If stmts is NULL, then the table is exists.
     * we need record that for searching the group of table.
     */
    if (stmts == NIL) {
        table_is_exist = true;
        /*
         * Just return here, if we continue
         * to send if not exists stmt, may
         * cause the inconsistency of metadata.
         * If we under xc_maintenance_mode, we can do
         * this to slove some problem of inconsistency.
         */
        if (u_sess->attr.attr_common.xc_maintenance_mode == false)
            return;
    }

#ifdef PGXC
    if (IS_MAIN_COORDINATOR) {
        /*
         * Scan the list of objects.
         * Temporary tables are created on Datanodes only.
         * Non-temporary objects are created on all nodes.
         * In case temporary and non-temporary objects are mized return an error.
         */
        bool is_first = true;

        foreach (l, stmts) {
            Node* stmt = (Node*)lfirst(l);

            if (IsA(stmt, CreateStmt)) {
                CreateStmt* stmt_loc = (CreateStmt*)stmt;
                sub_cluster = stmt_loc->subcluster;
                tablespace_name = stmt_loc->tablespacename;
                relpersistence = stmt_loc->relation->relpersistence;
                is_object_temp = stmt_loc->relation->relpersistence == RELPERSISTENCE_TEMP;
                internal_data = stmt_loc->internalData;
                if (is_object_temp)
                    u_sess->exec_cxt.hasTempObject = true;

                if (is_first) {
                    is_first = false;
                    if (is_object_temp)
                        is_temp = true;
                } else {
                    if (is_object_temp != is_temp)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("CREATE not supported for TEMP and non-TEMP objects"),
                                errdetail("You should separate TEMP and non-TEMP objects")));
                }
            } else if (IsA(stmt, CreateForeignTableStmt)) {
#ifdef ENABLE_MULTIPLE_NODES
                validate_streaming_engine_status(stmt);
#endif
                if (in_logic_cluster()) {
                    CreateStmt* stmt_loc = (CreateStmt*)stmt;
                    sub_cluster = stmt_loc->subcluster;
                }

                /* There are no temporary foreign tables */
                if (is_first) {
                    is_first = false;
                } else {
                    if (!is_temp)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("CREATE not supported for TEMP and non-TEMP objects"),
                                errdetail("You should separate TEMP and non-TEMP objects")));
                }
            } else if (IsA(stmt, CreateSeqStmt)) {
                CreateSeqStmt* sstmt = (CreateSeqStmt*)stmt;

                Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(sstmt->uuid), false, true);

                uuids = lappend(uuids, n);
            }
        }

        /* Package the internalData after the query_string */
        if (internal_data != NULL) {
            query_string_with_data = append_internal_data_to_query(internal_data, query_string);
        }

        /*
         * Now package the uuids message that create table on RemoteNode need.
         */
        if (uuids != NIL) {
            char* uuid_info = nodeToString(uuids);
            AssembleHybridMessage(&query_string_with_info, query_string_with_data, uuid_info);
        } else
            query_string_with_info = query_string_with_data;
    }

    /*
     * If I am the main execute CN but not CCN,
     * Notify the CCN to create firstly, and then notify other CNs except me.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
            if (!sent_to_remote) {
                RemoteQuery* step = makeNode(RemoteQuery);
                step->combine_type = COMBINE_TYPE_SAME;
                step->sql_statement = (char*)query_string_with_info;

                if (is_object_temp)
                    step->exec_type = EXEC_ON_NONE;
                else
                    step->exec_type = EXEC_ON_COORDS;

                step->exec_nodes = NULL;
                step->is_temp = is_temp;
                /* current CN is not the first node, need release the namespace lock, avoid distributed deadlocks. */
                if (namespace_id != InvalidOid) {
                    UnlockDatabaseObject(NamespaceRelationId, namespace_id, 0, AccessShareLock);
                }
                ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                pfree_ext(step);
            }
        }
    }

    if (u_sess->attr.attr_sql.enable_parallel_ddl) {
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !is_first_node)
            stmts = transformCreateStmt((CreateStmt*)parse_tree, query_string, uuids, false, &namespace_id);
    }
#endif

#ifdef PGXC
    /*
     * Add a RemoteQuery node for a query at top level on a remote
     * Coordinator, if not already done so
     */
    if (!sent_to_remote) {
        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node)
            stmts = AddRemoteQueryNode(stmts, query_string_with_info, EXEC_ON_DATANODES, is_temp);
        else
            stmts = AddRemoteQueryNode(stmts, query_string_with_info, CHOOSE_EXEC_NODES(is_object_temp), is_temp);

        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
            (sub_cluster == NULL || sub_cluster->clustertype == SUBCLUSTER_GROUP)) {
            const char* group_name = NULL;
            Oid group_oid = InvalidOid;

            /*
             * If TO-GROUP clause is specified when creating table, we
             * only have to add required datanode in remote DDL execution
             */
            if (sub_cluster != NULL) {
                ListCell* lc = NULL;
                foreach (lc, sub_cluster->members) {
                    group_name = strVal(lfirst(lc));
                }
            } else if (in_logic_cluster() && !table_is_exist) {
                /*
                 *  for CreateForeignTableStmt ,
                 *  CreateTableStmt with user not attached to logic cluster
                 */
                group_name = PgxcGroupGetCurrentLogicCluster();
                if (group_name == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Cannot find logic cluster.")));
                }
            } else {
                Oid tablespace_id = InvalidOid;
                bool dfs_tablespace = false;

                if (tablespace_name != NULL) {
                    tablespace_id = get_tablespace_oid(tablespace_name, false);
                } else {
                    tablespace_id = GetDefaultTablespace(relpersistence);
                }

                /* Determine if we are working on a HDFS table. */
                dfs_tablespace = IsSpecifiedTblspc(tablespace_id, FILESYSTEM_HDFS);

                /*
                 * If TO-GROUP clause is not specified we are using the installation group to
                 * distribute table.
                 *
                 * For HDFS table/Foreign Table we don't refer default_storage_nodegroup
                 * to make table creation.
                 */
                if (table_is_exist) {
                    Oid rel_id = RangeVarGetRelid(((CreateStmt*)parse_tree)->relation, NoLock, true);
                    if (OidIsValid(rel_id)) {
                        Oid table_groupoid = get_pgxc_class_groupoid(rel_id);
                        if (OidIsValid(table_groupoid)) {
                            group_name = get_pgxc_groupname(table_groupoid);
                        }
                    }
                    if (group_name == NULL) {
                        group_name = PgxcGroupGetInstallationGroup();
                    }
                } else if (dfs_tablespace || IsA(parse_tree, CreateForeignTableStmt)) {
                    group_name = PgxcGroupGetInstallationGroup();
                } else if (strcmp(u_sess->attr.attr_sql.default_storage_nodegroup, INSTALLATION_MODE) == 0 ||
                           u_sess->attr.attr_common.IsInplaceUpgrade) {
                    group_name = PgxcGroupGetInstallationGroup();
                } else {
                    group_name = u_sess->attr.attr_sql.default_storage_nodegroup;
                }

                /* If we didn't identify an installation node group error it out out */
                if (group_name == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Installation node group is not defined in current cluster")));
                }
            }

            /* Fetch group name */
            group_oid = get_pgxc_groupoid(group_name);
            if (!OidIsValid(group_oid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Target node group \"%s\" doesn't exist", group_name)));
            }

            if (in_logic_cluster()) {
                check_logic_cluster_create_priv(group_oid, group_name);
            } else {
                /* No limit in logic cluster mode */
                /* check to block non-redistribution process creating table to old group */
                if (!u_sess->attr.attr_sql.enable_cluster_resize) {
                    char in_redistribution = get_pgxc_group_redistributionstatus(group_oid);
                    if (in_redistribution == 'y') {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Unable to create table on old installation group \"%s\" while in cluster "
                                       "resizing.",
                                    group_name)));
                    }
                }
            }

            /* Build exec_nodes to table creation */
            const int total_len = list_length(stmts);
            Node* node = (Node*)list_nth(stmts, (total_len - 1));

            // *node* should be a RemoteQuery Node
            AssertEreport(query_string != NULL, MOD_EXECUTOR, "Node type is not remote type");
            RemoteQuery* rquery = (RemoteQuery*)node;
            // *exec_nodes* should be a NULL pointer
            AssertEreport(!rquery->exec_nodes, MOD_EXECUTOR, "remote query is not DN");
            rquery->exec_nodes = makeNode(ExecNodes);
            /* Set group oid here for sending bucket map to dn */
            t_thrd.xact_cxt.PGXCGroupOid = group_oid;
            if (find_hashbucket_options(stmts)) {
                rquery->is_send_bucket_map = true;
            }
            /*
             * Check node group permissions, we only do such kind of ACL check
             * for user-defined nodegroup(none-installation)
             */
            AclResult acl_result = pg_nodegroup_aclcheck(group_oid, GetUserId(), ACL_CREATE);
            if (acl_result != ACLCHECK_OK) {
                aclcheck_error(acl_result, ACL_KIND_NODEGROUP, group_name);
            }

            /*
             * Notice!!
             * In cluster resizing stage we need special processing logics in table creation as:
             *	[1]. create table delete_delta ... to group old_group on all DN
             *	[2]. display pgxc_group.group_members
             *	[3]. drop table delete_delta ==> drop delete_delta on all DN
             *
             * So, as normal, when target node group's status is marked as 'installation' or
             * 'redistribution', we have to issue a full-DN create table request, remeber
             * pgxc_class.group_members still reflects table's logic distribution to tell pgxc
             * planner to build Scan operator in multi_nodegroup way. The reason we have to so is
             * to be compatible with current gs_switch_relfilenode() invokation in cluster expand
             * and shrunk mechanism.
             */
            if (need_full_dn_execution(group_name)) {
                /* Sepcial path, issue full-DN create table request */
                rquery->exec_nodes->nodeList = GetAllDataNodes();
            } else {
                /* Normal path, issue only needs DNs in create table request */
                Oid* members = NULL;
                int nmembers = 0;
                nmembers = get_pgxc_groupmembers(group_oid, &members);

                /* Append nodeId to exec_nodes */
                rquery->exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
                pfree_ext(members);

                if (uuids && nmembers < u_sess->pgxc_cxt.NumDataNodes) {
                    char* create_seqs;
                    RemoteQuery* step;

                    /* Create table in NodeGroup with sequence. */
                    create_seqs = assemble_create_sequence_msg(stmts, uuids);
                    step = make_remote_query_for_seq(rquery->exec_nodes, create_seqs);
                    stmts = lappend(stmts, step);
                }
            }
        }
    }
#endif

    if (uuids != NIL) {
        list_free_deep(uuids);
        uuids = NIL;
    }

    /* ... and do it */
    foreach (l, stmts) {
        Node* stmt = (Node*)lfirst(l);

        if (IsA(stmt, CreateStmt)) {
            Datum toast_options;
            static const char* const validnsps[] = HEAP_RELOPT_NAMESPACES;

            /* forbid user to set or change inner options */
            ForbidOutUsersToSetInnerOptions(((CreateStmt*)stmt)->options);

            /* Create the table itself */
            rel_oid = DefineRelation((CreateStmt*)stmt,
                                    ((CreateStmt*)stmt)->relkind == RELKIND_MATVIEW ?
                                                                    RELKIND_MATVIEW : RELKIND_RELATION,
                                    InvalidOid, isCTAS);
            /*
             * Let AlterTableCreateToastTable decide if this one
             * needs a secondary relation too.
             */
            CommandCounterIncrement();

            /* parse and validate reloptions for the toast table */
            toast_options =
                transformRelOptions((Datum)0, ((CreateStmt*)stmt)->options, "toast", validnsps, true, false);

            (void)heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

            AlterTableCreateToastTable(rel_oid, toast_options, AccessShareLock);
            AlterCStoreCreateTables(rel_oid, toast_options, (CreateStmt*)stmt);
            AlterDfsCreateTables(rel_oid, toast_options, (CreateStmt*)stmt);
            AlterCreateChainTables(rel_oid, toast_options, (CreateStmt *)stmt);
#ifdef ENABLE_MULTIPLE_NODES
            Datum reloptions = transformRelOptions(
                (Datum)0, ((CreateStmt*)stmt)->options, NULL, validnsps, true, false);
            StdRdOptions* std_opt = (StdRdOptions*)heap_reloptions(RELKIND_RELATION, reloptions, true);
            if (StdRelOptIsTsStore(std_opt)) {
                create_ts_store_tables(rel_oid, toast_options);
            }
            /* create partition policy if ttl or period defined */
            create_part_policy_if_needed((CreateStmt*)stmt, rel_oid);
#endif   /* ENABLE_MULTIPLE_NODES */
        } else if (IsA(stmt, CreateForeignTableStmt)) {
            /* forbid user to set or change inner options */
            ForbidOutUsersToSetInnerOptions(((CreateStmt*)stmt)->options);

            /* if this is a log ft, check its definition */
            check_log_ft_definition((CreateForeignTableStmt*)stmt);

            /* Create the table itself */
            if (pg_strcasecmp(((CreateForeignTableStmt *)stmt)->servername, 
                STREAMING_SERVER) == 0) {
                /* Create stream */
                rel_oid = DefineRelation((CreateStmt*)stmt, RELKIND_STREAM, InvalidOid);
            } else {
                /* Create foreign table */
                rel_oid = DefineRelation((CreateStmt*)stmt, RELKIND_FOREIGN_TABLE, InvalidOid);
            }
            CreateForeignTable((CreateForeignTableStmt*)stmt, rel_oid);
        } else {
            if (IsA(stmt, AlterTableStmt))
                ((AlterTableStmt*)stmt)->fromCreate = true;

            /* Recurse for anything else */
            ProcessUtility(stmt,
                query_string_with_info,
                params,
                false,
                None_Receiver,
#ifdef PGXC
                true,
#endif /* PGXC */
                NULL,
                isCTAS);
        }

        /* Need CCI between commands */
        if (lnext(l) != NULL)
            CommandCounterIncrement();
    }

    parse_tree->uuids = NIL;
    /* all job done reset create schema flag */
    if (IS_MAIN_COORDINATOR) {
        u_sess->catalog_cxt.setCurCreateSchema = false;
        pfree_ext(u_sess->catalog_cxt.curCreateSchema);
    }
    list_free_ext(stmts);
}

void ReindexCommand(ReindexStmt* stmt, bool is_top_level)
{
    /* we choose to allow this during "read only" transactions */
    PreventCommandDuringRecovery("REINDEX");
    switch (stmt->kind) {
        case OBJECT_INDEX:
        case OBJECT_INDEX_PARTITION:
            ReindexIndex(stmt->relation, (const char*)stmt->name, &stmt->memUsage);
            break;
        case OBJECT_TABLE:
        case OBJECT_MATVIEW:
        case OBJECT_TABLE_PARTITION:
            ReindexTable(stmt->relation, (const char*)stmt->name, &stmt->memUsage);
            break;
        case OBJECT_INTERNAL:
        case OBJECT_INTERNAL_PARTITION:
            ReindexInternal(stmt->relation, (const char*)stmt->name);
            break;
        case OBJECT_DATABASE:

            /*
             * This cannot run inside a user transaction block; if
             * we were inside a transaction, then its commit- and
             * start-transaction-command calls would not have the
             * intended effect!
             */
            PreventTransactionChain(is_top_level, "REINDEX DATABASE");
            ReindexDatabase(stmt->name, stmt->do_system, stmt->do_user, &stmt->memUsage);
            break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized object type: %d", (int)stmt->kind)));
        } break;
    }
}

/* Called by standard_ProcessUtility() for the cases TRANS_STMT_BEGIN and TRANS_STMT_START */
static void set_item_arg_according_to_def_name(DefElem* item)
{
    if (strcmp(item->defname, "transaction_isolation") == 0) {
        SetPGVariable("transaction_isolation", list_make1(item->arg), true);
    } else if (strcmp(item->defname, "transaction_read_only") == 0) {
        /* Set read only state from CN when this DN is not read only. */
        if (u_sess->attr.attr_storage.DefaultXactReadOnly == false) {
            SetPGVariable("transaction_read_only", list_make1(item->arg), true);
        }
    } else if (strcmp(item->defname, "transaction_deferrable") == 0) {
        SetPGVariable("transaction_deferrable", list_make1(item->arg), true);
    }
}

/* Check proc->commitCSN is valid before do commit opertion, fatal if failed */
static void CheckProcCsnValid()
{
    /* if coordinator send the 'N' and commit message, check the csn number valid */
    if (!(useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE) &&
        IsConnFromCoord() && (GetCommitCsn() == InvalidCommitSeqNo)) {
        ereport(FATAL, (errmsg("invalid commit csn: %lu.", GetCommitCsn())));
    }
}

/* Add a RemoteQuery node for a query at top level on a remote Coordinator */
static void add_remote_query_4_alter_stmt(bool is_first_node, AlterTableStmt* atstmt, const char* query_string, List** stmts,
    char** drop_seq_string, ExecNodes** exec_nodes)
{
    if (!PointerIsValid(atstmt) || !PointerIsValid(stmts) || !PointerIsValid(drop_seq_string) ||
        !PointerIsValid(exec_nodes)) {
        return;
    }

    bool is_temp = false;
    RemoteQueryExecType exec_type;
    Oid rel_id = RangeVarGetRelid(atstmt->relation, NoLock, true);

    if (!OidIsValid(rel_id)) {
        return;
    }

    exec_type = ExecUtilityFindNodes(atstmt->relkind, rel_id, &is_temp);
    if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
        if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES) {
            *stmts = AddRemoteQueryNode(*stmts, query_string, EXEC_ON_DATANODES, is_temp);
        }
    } else {
        *stmts = AddRemoteQueryNode(*stmts, query_string, exec_type, is_temp);
    }

    /* nodegroup attch execnodes */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        Node* node = (Node*)lfirst(list_tail(*stmts));

        if (IsA(node, RemoteQuery)) {
            RemoteQuery* rquery = (RemoteQuery*)node;
            rquery->exec_nodes = RelidGetExecNodes(rel_id);
            if (rquery->exec_nodes->nodeList != NULL &&
                rquery->exec_nodes->nodeList->length < u_sess->pgxc_cxt.NumDataNodes) {
                *drop_seq_string = get_drop_seq_query_string(atstmt, rel_id);
                *exec_nodes = rquery->exec_nodes;
            }
        }
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/* Check the case of create index on timeseries table */
bool check_ts_create_idx(const Node* parsetree, Oid* nspname)
{
    if (nodeTag(parsetree) == T_IndexStmt) {
        IndexStmt* stmt = (IndexStmt*)parsetree;
        if (check_ts_idx_ddl(stmt->relation, NULL, nspname, stmt)) {
            return true;
        }
    }
    return false;
}
#endif

void ExecAlterDatabaseSetStmt(Node* parse_tree, const char* query_string, bool sent_to_remote)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errmodule(MOD_COMMAND),
                   errmsg("Alter database set guc value is not supported."),
                   errhint("Use gs_guc to set global guc value.")));
#endif
#ifdef PGXC
    if (IS_PGXC_COORDINATOR) {
        char* first_exec_node = find_first_exec_cn();
        bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
            ExecUtilityStmtOnNodes_ParallelDDLMode(
                query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
            AlterDatabaseSet((AlterDatabaseSetStmt*)parse_tree);
            ExecUtilityStmtOnNodes_ParallelDDLMode(
                query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
        } else {
            AlterDatabaseSet((AlterDatabaseSetStmt*)parse_tree);
            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
        }
    } else {
        AlterDatabaseSet((AlterDatabaseSetStmt*)parse_tree);
    }
#else
    AlterDatabaseSet((AlterDatabaseSetStmt*)parse_tree);
#endif
}

void ExecAlterRoleSetStmt(Node* parse_tree, const char* query_string, bool sent_to_remote)
{
#ifdef ENABLE_MULTIPLE_NODES
ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_COMMAND),
                errmsg("Alter user set guc value is not supported."),
                errhint("Use gs_guc to set global guc value.")));
#endif
#ifdef PGXC
        if (IS_PGXC_COORDINATOR) {
            char* first_exec_node = find_first_exec_cn();
            bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

            if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                ExecUtilityStmtOnNodes_ParallelDDLMode(
                    query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                AlterRoleSet((AlterRoleSetStmt*)parse_tree);
                ExecUtilityStmtOnNodes_ParallelDDLMode(
                    query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
            } else {
                AlterRoleSet((AlterRoleSetStmt*)parse_tree);
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
            }
        } else {
            AlterRoleSet((AlterRoleSetStmt*)parse_tree);
        }
#else
    AlterRoleSet((AlterRoleSetStmt*)parse_tree);
#endif
}

void standard_ProcessUtility(Node* parse_tree, const char* query_string, ParamListInfo params, bool is_top_level,
    DestReceiver* dest,
#ifdef PGXC
    bool sent_to_remote,
#endif /* PGXC */
    char* completion_tag,
    bool isCTAS)
{
    /* This can recurse, so check for excessive recursion */
    check_stack_depth();

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * If in scenario of create index for timeseries table, some object
     * replacement needs to be performed to replace the target object
     * with the timeseries tag rel.
     */
    char tag_tbl_name[NAMEDATALEN] = {0};
    Oid nspname;
    bool ts_idx_create = check_ts_create_idx(parse_tree, &nspname);
    if (ts_idx_create) {
        IndexStmt* stmt = (IndexStmt*)parse_tree;
        stmt->relation->schemaname = get_namespace_name(nspname);
        get_ts_idx_tgt(tag_tbl_name, stmt->relation);
        /* replace with the actual tag rel */
        stmt->relation->schemaname = pstrdup("cstore");
        stmt->relation->relname = tag_tbl_name;
    }
#endif

    /* Check the statement during online expansion. */
    BlockUnsupportedDDL(parse_tree);
#ifdef ENABLE_MULTIPLE_NODES
    block_ts_rangevar_unsupport_ddl(parse_tree);
#endif

    /* reset t_thrd.vacuum_cxt.vac_context in case that invalid t_thrd.vacuum_cxt.vac_context would be used */
    t_thrd.vacuum_cxt.vac_context = NULL;

#ifdef PGXC
    // if use rule in cluster resize, we will get the new query string for each stmt
#ifdef ENABLE_MULTIPLE_NODES
    char* new_query = NULL;
    new_query = rewrite_query_string(parse_tree);
    if (new_query != NULL) {
        /*
         * cannot free old queryString. it may point to other memory context, 
         * after switch to this context, it can be free. left to free with this context,
         * rewrite to 1 table, can free, rewrite 2 table, cannot free
         */
        query_string = new_query;
    }
#endif
    /*
     * For more detail see comments in function pgxc_lock_for_backup.
     *
     * Cosider the following scenario:
     * Imagine a two cordinator cluster CO1, CO2
     * Suppose a client connected to CO1 issues select pgxc_lock_for_backup()
     * Now assume that a client connected to CO2 issues a create table
     * select pgxc_lock_for_backup() would try to acquire the advisory lock
     * in exclusive mode, whereas create table would try to acquire the same
     * lock in shared mode. Both these requests will always try acquire the
     * lock in the same order i.e. they would both direct the request first to
     * CO1 and then to CO2. One of the two requests would therefore pass
     * and the other would fail.
     *
     * Consider another scenario:
     * Suppose we have a two cooridnator cluster CO1 and CO2
     * Assume one client connected to each coordinator
     * Further assume one client starts a transaction
     * and issues a DDL. This is an unfinished transaction.
     * Now assume the second client issues
     * select pgxc_lock_for_backup()
     * This request would fail because the unfinished transaction
     * would already hold the advisory lock.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsNormalProcessingMode()) {
        /* Is the statement a prohibited one? */
        if (!is_stmt_allowed_in_locked_mode(parse_tree, query_string))
            pgxc_lock_for_utility_stmt(parse_tree, isAllTempObjects(parse_tree, query_string, sent_to_remote));
    }
#endif
    if (t_thrd.proc->workingVersionNum >= 91275) {
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsAbortedTransactionBlockState()) {
            Oid node_oid = get_pgxc_nodeoid(g_instance.attr.attr_common.PGXCNodeName);
            bool nodeis_active = true;
            nodeis_active = is_pgxc_nodeactive(node_oid);
            if (OidIsValid(node_oid) && nodeis_active == false && !IS_CN_OBS_DISASTER_RECOVER_MODE)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Current Node is not active")));
        }
    }

    check_xact_readonly(parse_tree);

    if (completion_tag != NULL)
        completion_tag[0] = '\0';

    errno_t errorno = EOK;
    switch (nodeTag(parse_tree)) {
            /*
             * ******************** transactions ********************
             */
        case T_TransactionStmt: {
            TransactionStmt* stmt = (TransactionStmt*)parse_tree;

            switch (stmt->kind) {
                    /*
                     * START TRANSACTION, as defined by SQL99: Identical
                     * to BEGIN.  Same code for both.
                     */
                case TRANS_STMT_BEGIN:
                case TRANS_STMT_START: {
                    ListCell* lc = NULL;
                    BeginTransactionBlock();
                    foreach (lc, stmt->options) {
                        DefElem* item = (DefElem*)lfirst(lc);
                        set_item_arg_according_to_def_name(item);
                    }
                    u_sess->need_report_top_xid = true;
                } break;

                case TRANS_STMT_COMMIT:
                    /* Only generate one time when u_sess->debug_query_id = 0 in CN */
                    if ((IS_SINGLE_NODE || IS_PGXC_COORDINATOR) && u_sess->debug_query_id == 0) {
                        u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
                        pgstat_report_queryid(u_sess->debug_query_id);
                    }
                    /* only check write nodes csn valid */
                    if (TransactionIdIsValid(GetTopTransactionIdIfAny())) {
                        CheckProcCsnValid();
                    }
                    if (!EndTransactionBlock()) {
                        /* report unsuccessful commit in completion_tag */
                        if (completion_tag != NULL) {
                            errorno = strcpy_s(completion_tag, COMPLETION_TAG_BUFSIZE, "ROLLBACK");
                            securec_check(errorno, "\0", "\0");
                        }
                    }
                    FreeSavepointList();
                    break;

                case TRANS_STMT_PREPARE:
                    PreventCommandDuringRecovery("PREPARE TRANSACTION");
#ifdef PGXC

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !u_sess->attr.attr_common.xc_maintenance_mode) {
                        /* Explicit prepare transaction is not support */
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                (errmsg("Explicit prepare transaction is not supported."))));

                        /* Add check if xid is valid */
                        if (IsXidImplicit((const char*)stmt->gid)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                                    (errmsg("Invalid transaciton_id to prepare."))));
                            break;
                        }
                    }
#endif

                    if (!PrepareTransactionBlock(stmt->gid)) {
                        /* report unsuccessful commit in completion_tag */
                        if (completion_tag != NULL) {
                            errorno = strcpy_s(completion_tag, COMPLETION_TAG_BUFSIZE, "ROLLBACK");
                            securec_check(errorno, "\0", "\0");
                        }
                    }
                    break;

                case TRANS_STMT_COMMIT_PREPARED:
                    PreventTransactionChain(is_top_level, "COMMIT PREPARED");
                    PreventCommandDuringRecovery("COMMIT PREPARED");

                    /* for commit in progress, extract the latest local csn for set */
                    if (COMMITSEQNO_IS_COMMITTING(stmt->csn)) {
                        stmt->csn = GET_COMMITSEQNO(stmt->csn);
                    }
#ifdef PGXC
                    /*
                     * Commit a transaction which was explicitely prepared
                     * before
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (!u_sess->attr.attr_common.xc_maintenance_mode) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    (errmsg("Explicit commit prepared transaction is not supported."))));
                        } else {
                            if (!(useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE) &&
                                    COMMITSEQNO_IS_COMMITTED(stmt->csn)) {
                                setCommitCsn(stmt->csn);
                            }
                            FinishPreparedTransaction(stmt->gid, true);
                        }
                    } else {
#endif
#ifdef ENABLE_DISTRIBUTE_TEST
                        if (IS_PGXC_DATANODE && TEST_STUB(DN_COMMIT_PREPARED_SLEEP, twophase_default_error_emit)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("GTM_TEST  %s: DN commit prepare sleep",
                                    g_instance.attr.attr_common.PGXCNodeName)));
                            /* sleep 30s or more */
                            pg_usleep(g_instance.distribute_test_param_instance->sleep_time * 1000000);
                        }

                        /* white box test start */
                        if (IS_PGXC_DATANODE)
                            execute_whitebox(WHITEBOX_LOC, stmt->gid, WHITEBOX_WAIT, 0.0001);
                            /* white box test end */
#endif
                        if (!(useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE) &&
                            COMMITSEQNO_IS_COMMITTED(stmt->csn)) {
                            setCommitCsn(stmt->csn);
                        }
                        CheckProcCsnValid();
                        FinishPreparedTransaction(stmt->gid, true);
#ifdef PGXC
                    }
#endif
                    break;

                case TRANS_STMT_ROLLBACK_PREPARED:
                    PreventTransactionChain(is_top_level, "ROLLBACK PREPARED");
                    PreventCommandDuringRecovery("ROLLBACK PREPARED");
#ifdef PGXC
                    /*
                     * Abort a transaction which was explicitely prepared
                     * before
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (!u_sess->attr.attr_common.xc_maintenance_mode) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    (errmsg("Explicit rollback prepared transaction is not supported."))));
                        } else
                            FinishPreparedTransaction(stmt->gid, false);
                    } else
#endif
                        FinishPreparedTransaction(stmt->gid, false);
                    break;

                case TRANS_STMT_ROLLBACK:
                    UserAbortTransactionBlock();
                    FreeSavepointList();
                    break;

                case TRANS_STMT_SAVEPOINT: {
                    char* name = NULL;

                    RequireTransactionChain(is_top_level, "SAVEPOINT");

                    name = GetSavepointName(stmt->options);

                    /*
                     *  CN send the following info to DNs and other CNs before itself DefineSavepoint
                     * 1)parent xid
                     * 2)start transaction command if need
                     * 3)SAVEPOINT command
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        /* record the savepoint cmd, send to other CNs until DDL is executed. */
                        RecordSavepoint(query_string, name, false, SUB_STMT_SAVEPOINT);
                        pgxc_node_remote_savepoint(query_string, EXEC_ON_DATANODES, true, true);

#ifdef ENABLE_DISTRIBUTE_TEST
                        if (TEST_STUB(CN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED, twophase_default_error_emit)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("SUBXACT_TEST %s: cn local define savepoint \"%s\" failed",
                                    g_instance.attr.attr_common.PGXCNodeName,
                                    name)));
                        }
                        /* white box test start */
                        if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.0001)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("WHITE_BOX TEST  %s: cn local define savepoint failed",
                                    g_instance.attr.attr_common.PGXCNodeName)));
                        }
                        /* white box test end */
#endif
                    } else {
                        /* Dn or other cn should assign next_xid from cn to local, set parent xid for
                         * CurrentTransactionState */
                        GetCurrentTransactionId();

#ifdef ENABLE_DISTRIBUTE_TEST
                        if (TEST_STUB(DN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED, twophase_default_error_emit)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("SUBXACT_TEST %s: dn local define savepoint \"%s\" failed",
                                    g_instance.attr.attr_common.PGXCNodeName,
                                    name)));
                        }
                        /* white box test start */
                        if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.0001)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("WHITE_BOX TEST  %s: dn local define savepoint failed",
                                    g_instance.attr.attr_common.PGXCNodeName)));
                        }
                        /* white box test end */
#endif
                    }

                    DefineSavepoint(name);
                } break;

                case TRANS_STMT_RELEASE:
                    RequireTransactionChain(is_top_level, "RELEASE SAVEPOINT");

                    ReleaseSavepoint(GetSavepointName(stmt->options), false);

#ifdef ENABLE_DISTRIBUTE_TEST
                    /* CN send xid for remote nodes for it assignning parentxid when need. */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {

                        if (TEST_STUB(CN_RELEASESAVEPOINT_BEFORE_SEND_FAILED, twophase_default_error_emit)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("SUBXACT_TEST %s: cn release savepoint before send to remote nodes failed.",
                                    g_instance.attr.attr_common.PGXCNodeName)));
                        }
                    } else if (TEST_STUB(DN_RELEASESAVEPOINT_AFTER_LOCAL_DEAL_FAILED, twophase_default_error_emit)) {
                        ereport(g_instance.distribute_test_param_instance->elevel,
                            (errmsg("SUBXACT_TEST %s: dn release savepoint after loal deal failed",
                                g_instance.attr.attr_common.PGXCNodeName)));
                    }

                    /* white box test start */
                    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.0001)) {
                        ereport(g_instance.distribute_test_param_instance->elevel,
                            (errmsg("WHITE_BOX TEST  %s: release savepoint failed",
                                g_instance.attr.attr_common.PGXCNodeName)));
                    }
                    /* white box test end */
#endif
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        HandleReleaseOrRollbackSavepoint(
                            query_string, GetSavepointName(stmt->options), SUB_STMT_RELEASE);
                        pgxc_node_remote_savepoint(query_string, EXEC_ON_DATANODES, false, false);
                    }

                    break;

                case TRANS_STMT_ROLLBACK_TO:
                    RequireTransactionChain(is_top_level, "ROLLBACK TO SAVEPOINT");
                    RollbackToSavepoint(GetSavepointName(stmt->options), false);

#ifdef ENABLE_DISTRIBUTE_TEST
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (TEST_STUB(CN_ROLLBACKTOSAVEPOINT_BEFORE_SEND_FAILED, twophase_default_error_emit)) {
                            ereport(g_instance.distribute_test_param_instance->elevel,
                                (errmsg("SUBXACT_TEST %s: cn rollback to savepoint failed before send to remote nodes.",
                                    g_instance.attr.attr_common.PGXCNodeName)));
                        }
                    } else if (TEST_STUB(DN_ROLLBACKTOSAVEPOINT_AFTER_LOCAL_DEAL_FAILED, twophase_default_error_emit)) {
                        ereport(g_instance.distribute_test_param_instance->elevel,
                            (errmsg("SUBXACT_TEST %s: dn rollback to savepoint failed after local deal failed",
                                g_instance.attr.attr_common.PGXCNodeName)));
                    }
                    /* white box test start */
                    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_DEFAULT, 0.0001)) {
                        ereport(LOG,
                            (errmsg("WHITE_BOX TEST  %s: rollback to savepoint failed",
                                g_instance.attr.attr_common.PGXCNodeName)));
                    }
                    /* white box test end */
#endif

                    /*
                     * CN needn't send xid, as savepoint must be sent and executed before.
                     * And the parent xid must be in transaction state pushed and remained.
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        HandleReleaseOrRollbackSavepoint(
                            query_string, GetSavepointName(stmt->options), SUB_STMT_ROLLBACK_TO);
                        pgxc_node_remote_savepoint(query_string, EXEC_ON_DATANODES, false, false);
                    }
                    /*
                     * CommitTransactionCommand is in charge of
                     * re-defining the savepoint again
                     */
                    break;
                default:
                    break;
            }
        } break;

            /*
             * Portal (cursor) manipulation
             *
             * Note: DECLARE CURSOR is processed mostly as a SELECT, and
             * therefore what we will get here is a PlannedStmt not a bare
             * DeclareCursorStmt.
             */
        case T_PlannedStmt: {
            PlannedStmt* stmt = (PlannedStmt*)parse_tree;

            if (stmt->utilityStmt == NULL || !IsA(stmt->utilityStmt, DeclareCursorStmt)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("non-DECLARE CURSOR PlannedStmt passed to ProcessUtility")));
            }
            PerformCursorOpen(stmt, params, query_string, is_top_level);
        } break;

        case T_ClosePortalStmt: {
            ClosePortalStmt* stmt = (ClosePortalStmt*)parse_tree;

            CheckRestrictedOperation("CLOSE");
            stop_query();  // stop datanode query asap.
            PerformPortalClose(stmt->portalname);
        } break;

        case T_FetchStmt:
            PerformPortalFetch((FetchStmt*)parse_tree, dest, completion_tag);
            break;

            /*
             * relation and attribute manipulation
             */
        case T_CreateSchemaStmt:
#ifdef PGXC
            CreateSchemaCommand((CreateSchemaStmt*)parse_tree, query_string, sent_to_remote);
#else
            CreateSchemaCommand((CreateSchemaStmt*)parse_tree, query_string);
#endif
            break;

        case T_AlterSchemaStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterSchemaCommand((AlterSchemaStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterSchemaCommand((AlterSchemaStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterSchemaCommand((AlterSchemaStmt*)parse_tree);
            }
#else
            AlterSchemaCommand((AlterSchemaStmt*)parse_tree);
#endif
            break;

        case T_CreateForeignTableStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (!IsInitdb && IS_SINGLE_NODE) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Current mode does not support FOREIGN table yet"),
                        errdetail("The feature is not currently supported")));
            }
#endif
            /* fall through */
        case T_CreateStmt: {
#ifdef PGXC
            CreateCommand((CreateStmt*)parse_tree, query_string, params, is_top_level, sent_to_remote, isCTAS);
#else
            CreateCommand((CreateStmt*)parse_tree, query_string, params, is_top_level, isCTAS);
#endif
        } break;

        case T_CreateTableSpaceStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                PreventTransactionChain(is_top_level, "CREATE TABLESPACE");

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    exec_utility_with_message_parallel_ddl_mode(
                        query_string, sent_to_remote, false, first_exec_node, EXEC_ON_COORDS);
                    CreateTableSpace((CreateTableSpaceStmt*)parse_tree);
                    exec_utility_with_message_parallel_ddl_mode(
                        query_string, sent_to_remote, false, first_exec_node, EXEC_ON_DATANODES);
                } else {
                    CreateTableSpace((CreateTableSpaceStmt*)parse_tree);
                    ExecUtilityWithMessage(query_string, sent_to_remote, false);
                }
            } else {
                /* Don't allow this to be run inside transaction block on single node */
                if (IS_SINGLE_NODE)
                    PreventTransactionChain(is_top_level, "CREATE TABLESPACE");
                CreateTableSpace((CreateTableSpaceStmt*)parse_tree);
            }
#else
            PreventTransactionChain(is_top_level, "CREATE TABLESPACE");
            CreateTableSpace((CreateTableSpaceStmt*)parse_tree);
#endif
            break;

        case T_DropTableSpaceStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
                PreventTransactionChain(is_top_level, "DROP TABLESPACE");
            /* Allow this to be run inside transaction block on remote nodes */
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropTableSpace((DropTableSpaceStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropTableSpace((DropTableSpaceStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropTableSpace((DropTableSpaceStmt*)parse_tree);
            }
#else
            PreventTransactionChain(is_top_level, "DROP TABLESPACE");
            DropTableSpace((DropTableSpaceStmt*)parse_tree);
#endif
            break;

        case T_AlterTableSpaceOptionsStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterTableSpaceOptions((AlterTableSpaceOptionsStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterTableSpaceOptions((AlterTableSpaceOptionsStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterTableSpaceOptions((AlterTableSpaceOptionsStmt*)parse_tree);
            }
#else
            AlterTableSpaceOptions((AlterTableSpaceOptionsStmt*)parse_tree);
#endif
            break;

        case T_CreateExtensionStmt:
            CreateExtension((CreateExtensionStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterExtensionStmt:
#ifdef PGXC
            FEATURE_NOT_PUBLIC_ERROR("EXTENSION is not yet supported.");
#endif /* PGXC */
            ExecAlterExtensionStmt((AlterExtensionStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterExtensionContentsStmt:
#ifdef PGXC
            FEATURE_NOT_PUBLIC_ERROR("EXTENSION is not yet supported.");
#endif /* PGXC */
            ExecAlterExtensionContentsStmt((AlterExtensionContentsStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateFdwStmt:
#ifdef ENABLE_MULTIPLE_NODES		
#ifdef PGXC
            /* enable CREATE FOREIGN DATA WRAPPER when initdb */
            if (!IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("openGauss does not support FOREIGN DATA WRAPPER yet"),
                        errdetail("The feature is not currently supported")));
            }
#endif
#endif
            CreateForeignDataWrapper((CreateFdwStmt*)parse_tree);

#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateWeakPasswordDictionaryStmt:
            CreateWeakPasswordDictionary((CreateWeakPasswordDictionaryStmt*)parse_tree);
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
#endif
            break;
            /* We decide to apply ExecuteTruncate for realizing DROP WEAK PASSWORD DICTIONARY operation
               by set gs_weak_password name directly, making the system find our system table */
        case T_DropWeakPasswordDictionaryStmt:
            {
                DropWeakPasswordDictionary();
            }
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
#endif            
            break;
    
        case T_AlterFdwStmt:
#ifdef ENABLE_MULTIPLE_NODES
#ifdef PGXC
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support FOREIGN DATA WRAPPER yet"),
                    errdetail("The feature is not currently supported")));
#endif
#endif
            AlterForeignDataWrapper((AlterFdwStmt*)parse_tree);
            break;

        case T_CreateForeignServerStmt:
#ifdef ENABLE_MULTIPLE_NODES		
            if (!IsInitdb && IS_SINGLE_NODE) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Current mode does not support FOREIGN server yet"),
                        errdetail("The feature is not currently supported")));
            }
#endif			
            CreateForeignServer((CreateForeignServerStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterForeignServerStmt:
            AlterForeignServer((AlterForeignServerStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateUserMappingStmt:
#ifdef ENABLE_MULTIPLE_NODES		
#ifdef PGXC
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support USER MAPPING yet"),
                    errdetail("The feature is not currently supported")));
#endif
#endif	
            CreateUserMapping((CreateUserMappingStmt*)parse_tree);
            break;

        case T_AlterUserMappingStmt:
#ifdef ENABLE_MULTIPLE_NODES			
#ifdef PGXC
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support USER MAPPING yet"),
                    errdetail("The feature is not currently supported")));
#endif
#endif	
            AlterUserMapping((AlterUserMappingStmt*)parse_tree);
            break;

        case T_DropUserMappingStmt:
#ifdef ENABLE_MULTIPLE_NODES		
#ifdef PGXC
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support USER MAPPING yet"),
                    errdetail("The feature is not currently supported")));
#endif
#endif	
            RemoveUserMapping((DropUserMappingStmt*)parse_tree);
            break;

        case T_CreateDataSourceStmt:
            CreateDataSource((CreateDataSourceStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterDataSourceStmt:
            AlterDataSource((AlterDataSourceStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateRlsPolicyStmt: /* CREATE ROW LEVEL SECURITY POLICY */
            CreateRlsPolicy((CreateRlsPolicyStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
#endif
            break;

        case T_AlterRlsPolicyStmt: /* ALTER ROW LEVEL SECURITY POLICY */
            AlterRlsPolicy((AlterRlsPolicyStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IsInitdb)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
#endif
            break;

        case T_DropStmt:
            CheckObjectInBlackList(((DropStmt*)parse_tree)->removeType, query_string);

            /*
             * performMultipleDeletions() needs to know is_top_level by
             * "DfsDDLIsTopLevelXact" to prevent "drop hdfs table"
             * running inside a transaction block.
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
                u_sess->exec_cxt.DfsDDLIsTopLevelXact = is_top_level;

            switch (((DropStmt*)parse_tree)->removeType) {
                case OBJECT_INDEX:
#ifdef ENABLE_MULTIPLE_NODES
                    if (((DropStmt*)parse_tree)->concurrent) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("PGXC does not support concurrent INDEX yet"),
                                errdetail("The feature is not currently supported")));
                    }
#endif
                    if (((DropStmt*)parse_tree)->concurrent)
                        PreventTransactionChain(is_top_level, "DROP INDEX CONCURRENTLY");
                    /* fall through */
                case OBJECT_FOREIGN_TABLE:
                case OBJECT_STREAM:
                case OBJECT_MATVIEW:
                case OBJECT_TABLE: {
#ifdef PGXC
                    /*
                     * For table batch-dropping, we we only support to drop tables
                     * belonging same nodegroup.
                     *
                     * Note: we only have to such kind of check at CN node
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        DropStmt* ds = (DropStmt*)parse_tree;

                        if (!ObjectsInSameNodeGroup(ds->objects, T_DropStmt)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("NOT-SUPPORT: Not support DROP multiple objects different nodegroup")));
                        }
                    }
                    /*
                     * Need to let ProcSleep know if we could cancel redistribution transaction which
                     * locks the table we want to drop. ProcSleep will make sure we only cancel the
                     * transaction doing redistribution.
                     *
                     * need to refactor this part into a common function where all supported cancel-redistribution
                     * DDL statements sets it
                     */
                    if (IS_PGXC_COORDINATOR && ((DropStmt*)parse_tree)->removeType == OBJECT_TABLE) {
                        u_sess->exec_cxt.could_cancel_redistribution = true;
                    }
#endif
                }
                    /* fall through */
                case OBJECT_SEQUENCE:
                case OBJECT_LARGE_SEQUENCE:
                case OBJECT_VIEW:
                case OBJECT_CONTQUERY:
#ifdef PGXC
                {
                    if (((DropStmt*)parse_tree)->removeType == OBJECT_FOREIGN_TABLE ||
                        ((DropStmt*)parse_tree)->removeType == OBJECT_STREAM) {
                        /*
                         * In the security mode, the useft privilege of a user must be
                         * checked before the user creates a foreign table.
                         */
                        if (isSecurityMode && !have_useft_privilege()) {
                            ereport(ERROR,
                                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                    errmsg("permission denied to drop foreign table in security mode")));
                        }
                    }
                    bool is_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
                    ObjectAddresses* new_objects = NULL;

                    /*
                     * For DROP TABLE/INDEX/VIEW/... IF EXISTS query, only notice is emitted
                     * if the referred objects are not found. In such case, the atomicity and consistency
                     * of the query or transaction among local CN and remote nodes can not be guaranteed
                     * against concurrent CREATE TABLE/INDEX/VIEW/... query.
                     *
                     * To ensure such atomicity and consistency, we only refer to local CN about
                     * the visibility of the objects to be deleted and rewrite the query into tmp_query_string
                     * without the inivisible objects. Later, if the objects in tmp_query_string are not
                     * found on remote nodes, which should not happen, just ERROR.
                     */
                    StringInfo tmp_query_string = makeStringInfo();

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment((DropStmt*)parse_tree, query_string, sent_to_remote, &is_temp, &exec_type);

                    char* first_exec_node = NULL;
                    bool is_first_node = false;

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        first_exec_node = find_first_exec_cn();
                        is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                    }

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && u_sess->attr.attr_sql.enable_parallel_ddl) {
                        if (!is_first_node) {
                            new_objects = PreCheckforRemoveRelation((DropStmt*)parse_tree, tmp_query_string, &exec_type);
                        }
                    }

                    /*
                     * If I am the main execute CN but not CCN,
                     * Notify the CCN to create firstly, and then notify other CNs except me.
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                            RemoteQuery* step = makeNode(RemoteQuery);
                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = tmp_query_string->data[0] ? tmp_query_string->data : (char*)query_string;
                            step->exec_type = EXEC_ON_COORDS;
                            step->exec_nodes = NULL;
                            step->is_temp = false;
                            ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                            pfree_ext(step);
                        }
                    }

                    /*
                     * @NodeGroup Support
                     *
                     * Scan for first object from drop-list in DropStmt to find target DNs,
                     * here for TO-GROUP aware objects, we need pass DropStmt handler into
                     * ExecUtilityStmtOnNodes() to further evaluate which DNs wend utility.
                     */
                    ExecNodes* exec_nodes = NULL;
                    Node* reparse = NULL;
                    ObjectType object_type = ((DropStmt*)parse_tree)->removeType;
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (object_type == OBJECT_TABLE || object_type == OBJECT_INDEX ||
                        object_type == OBJECT_MATVIEW)) {
                        reparse = (Node*)parse_tree;
                        ListCell* lc = list_head(((DropStmt*)parse_tree)->objects);
                        RangeVar* rel = makeRangeVarFromNameList((List*)lfirst(lc));
                        Oid rel_id;
                        LOCKMODE lockmode = NoLock;
                        if (object_type == OBJECT_TABLE || object_type == OBJECT_MATVIEW)
                            lockmode = AccessExclusiveLock;

                        rel_id = RangeVarGetRelid(rel, lockmode, ((DropStmt*)parse_tree)->missing_ok);
                        if (OidIsValid(rel_id)) {
                            Oid check_id = rel_id;
                            char relkind = get_rel_relkind(rel_id);
                            /*
                             * If a view is droped using drop index or drop table cmd, the errmsg will
                             * be somewhat ambiguous since it does not exist in pgxc_class. So we check
                             * in advance to give clear error message.
                             */
                            CheckDropViewValidity(object_type, relkind, rel->relname);
                            if (relkind == RELKIND_INDEX || relkind == RELKIND_GLOBAL_INDEX) {
                                check_id = IndexGetRelation(rel_id, false);
                            }
                            Oid group_oid = get_pgxc_class_groupoid(check_id);
                            char* group_name = get_pgxc_groupname(group_oid);

                            /*
                             * Reminding, when supported user-defined node group expansion,
                             * we need create ExecNodes from target node group.
                             *
                             * Notice!!
                             * In cluster resizing stage we need special processing logics in dropping table as:
                             *	[1]. create table delete_delta ... to group old_group on all DN
                             *	[2]. display pgxc_group.group_members
                             *	[3]. drop table delete_delta ==> drop delete_delta on all DN
                             *
                             * So, as normal, when target node group's status is marked as 'installation' or
                             * 'redistribution', we have to issue a full-DN drop table request, remeber
                             * pgxc_class.group_members still reflects table's logic distribution to tell pgxc
                             * planner to build Scan operator in multi_nodegroup way. The reason we have to so is
                             * to be compatible with current gs_switch_relfilenode() invokation in cluster expand
                             * and shrunk mechanism.
                             */
                            if (need_full_dn_execution(group_name)) {
                                exec_nodes = makeNode(ExecNodes);
                                exec_nodes->nodeList = GetAllDataNodes();
                            } else {
                                exec_nodes = RelidGetExecNodes(rel_id);
                            }
                        } else {
                            exec_nodes = RelidGetExecNodes(rel_id);
                        }
                    } else if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && 
                               (object_type == OBJECT_FOREIGN_TABLE ||
                               object_type == OBJECT_STREAM) &&
                               in_logic_cluster()) {
                        ListCell* lc = list_head(((DropStmt*)parse_tree)->objects);
                        RangeVar* relvar = makeRangeVarFromNameList((List*)lfirst(lc));
                        Oid rel_id = RangeVarGetRelid(relvar, NoLock, true);
                        if (OidIsValid(rel_id))
                            exec_nodes = RelidGetExecNodes(rel_id);
                        else if (!((DropStmt*)parse_tree)->missing_ok) {
                            if (relvar->schemaname)
                                ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_TABLE),
                                        errmsg("foreign table \"%s.%s\" does not exist",
                                            relvar->schemaname,
                                            relvar->relname)));
                            else
                                ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_TABLE),
                                        errmsg("foreign table \"%s\" does not exist", relvar->relname)));
                        }
                    }

#ifdef ENABLE_MULTIPLE_NODES
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        drop_sequence_4_node_group((DropStmt*)parse_tree, exec_nodes);
                    }
#endif
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && u_sess->attr.attr_sql.enable_parallel_ddl) {
                        if (!is_first_node)
                            RemoveRelationsonMainExecCN((DropStmt*)parse_tree, new_objects);
                        else
                            RemoveRelations((DropStmt*)parse_tree, tmp_query_string, &exec_type);
                    } else
                        RemoveRelations((DropStmt*)parse_tree, tmp_query_string, &exec_type);

                    /* DROP is done depending on the object type and its temporary type */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                                ExecUtilityStmtOnNodes_ParallelDDLMode(
                                    tmp_query_string->data[0] ? tmp_query_string->data : query_string,
                                    exec_nodes,
                                    sent_to_remote,
                                    false,
                                    EXEC_ON_DATANODES,
                                    is_temp,
                                    first_exec_node,
                                    reparse);
                        } else {
                            ExecUtilityStmtOnNodes(tmp_query_string->data[0] ? tmp_query_string->data : query_string,
                                exec_nodes,
                                sent_to_remote,
                                false,
                                exec_type,
                                is_temp,
                                reparse);
                        }
                    }

                    pfree_ext(tmp_query_string->data);
                    pfree_ext(tmp_query_string);
                    FreeExecNodes(&exec_nodes);

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && new_objects != NULL)
                        free_object_addresses(new_objects);
#endif
                } break;
                case OBJECT_SCHEMA:
                case OBJECT_FUNCTION: {
#ifdef PGXC
                    bool is_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
                    ObjectAddresses* new_objects = NULL;
                    StringInfo tmp_query_string = makeStringInfo();

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment((DropStmt*)parse_tree, query_string, sent_to_remote, &is_temp, &exec_type);

                    char* first_exec_node = NULL;
                    bool is_first_node = false;

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        first_exec_node = find_first_exec_cn();
                        is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                    }

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && u_sess->attr.attr_sql.enable_parallel_ddl) {
                        new_objects =
                            PreCheckforRemoveObjects((DropStmt*)parse_tree, tmp_query_string, &exec_type, is_first_node);
                    }

                    /*
                     * @NodeGroup Support
                     *
                     * Scan for first object from drop-list in DropStmt to find target DNs.
                     */
                    ExecNodes* exec_nodes = NULL;
                    ObjectType object_type = ((DropStmt*)parse_tree)->removeType;
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && object_type == OBJECT_FUNCTION &&
                        in_logic_cluster()) {
                        if (!DropObjectsInSameNodeGroup((DropStmt*)parse_tree)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("NOT-SUPPORT: Not support DROP multiple functions in different nodegroup")));
                        }

                        exec_nodes = GetDropFunctionNodes((DropStmt*)parse_tree);
                    }

                    /*
                     * If I am the main execute CN but not CCN,
                     * Notify the CCN to create firstly, and then notify other CNs except me.
                     */
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                            RemoteQuery* step = makeNode(RemoteQuery);
                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = tmp_query_string->data[0] ? tmp_query_string->data : (char*)query_string;
                            step->exec_type = EXEC_ON_COORDS;
                            step->exec_nodes = NULL;
                            step->is_temp = false;
                            ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                            pfree_ext(step);
                        }
                    }

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && u_sess->attr.attr_sql.enable_parallel_ddl) {
                        RemoveObjectsonMainExecCN((DropStmt*)parse_tree, new_objects, is_first_node);
                    } else {
                        if (IS_SINGLE_NODE) {
                            RemoveObjects((DropStmt*)parse_tree, true);
                        } else {
                            if (u_sess->attr.attr_sql.enable_parallel_ddl)
                                RemoveObjects((DropStmt*)parse_tree, false);
                            else
                                RemoveObjects((DropStmt*)parse_tree, true);
                        }
                    }

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                                ExecUtilityStmtOnNodes_ParallelDDLMode(
                                    tmp_query_string->data[0] ? tmp_query_string->data : query_string,
                                    exec_nodes,
                                    sent_to_remote,
                                    false,
                                    EXEC_ON_DATANODES,
                                    is_temp,
                                    first_exec_node);
                        } else {
                            ExecUtilityStmtOnNodes(tmp_query_string->data[0] ? tmp_query_string->data : query_string,
                                exec_nodes,
                                sent_to_remote,
                                false,
                                exec_type,
                                is_temp);
                        }
                    }

                    pfree_ext(tmp_query_string->data);
                    pfree_ext(tmp_query_string);
                    FreeExecNodes(&exec_nodes);

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && new_objects != NULL)
                        free_object_addresses(new_objects);
#endif
                } break;
                case OBJECT_PACKAGE:
                case OBJECT_PACKAGE_BODY: {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("NOT-SUPPORT: Not support DROP PACKAGE in distributed database")));
#endif
                    RemoveObjects((DropStmt*)parse_tree, true);
                } break;
                case OBJECT_GLOBAL_SETTING: {
                    bool is_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment((DropStmt *) parse_tree, query_string, sent_to_remote,
                                            &is_temp, &exec_type);

                    /*
                    * If I am the main execute CN but not CCN,
                    * Notify the CCN to create firstly, and then notify other CNs except me.
                    */
                    char *FirstExecNode = NULL;
                    bool isFirstNode = false;

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        FirstExecNode = find_first_exec_cn();
                        isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);
                    }
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                            RemoteQuery *step = makeNode(RemoteQuery);
                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = (char *) query_string;
                            step->exec_type = EXEC_ON_COORDS;
                            step->exec_nodes = NULL;
                            step->is_temp = false;
                            ExecRemoteUtility_ParallelDDLMode(step, FirstExecNode);
                            pfree_ext(step);
                        }
                    }
                    (void)drop_global_settings((DropStmt *)parse_tree);
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                                ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false,
                                    EXEC_ON_DATANODES, is_temp, FirstExecNode);
                        } else {
                            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, exec_type, is_temp);
                        }
                    }
                    break;
                }
                case OBJECT_COLUMN_SETTING: {
                    bool is_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment((DropStmt *) parse_tree, query_string, sent_to_remote,
                                            &is_temp, &exec_type);

                    /*
                    * If I am the main execute CN but not CCN,
                    * Notify the CCN to create firstly, and then notify other CNs except me.
                    */
                    char *FirstExecNode = NULL;
                    bool isFirstNode = false;

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        FirstExecNode = find_first_exec_cn();
                        isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);
                    }
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                            RemoteQuery *step = makeNode(RemoteQuery);
                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = (char *) query_string;
                            step->exec_type = EXEC_ON_COORDS;
                            step->exec_nodes = NULL;
                            step->is_temp = false;
                            ExecRemoteUtility_ParallelDDLMode(step, FirstExecNode);
                            pfree_ext(step);
                        }
                    }
                    (void)drop_column_settings((DropStmt *)parse_tree);
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {	
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                                ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false,
                                    EXEC_ON_DATANODES, is_temp, FirstExecNode);
                        } else {
                            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, exec_type, is_temp);
                        }
                    }
                    break;
                }
                default: {
#ifdef PGXC
                    bool is_temp = false;
                    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;

                    /* Check restrictions on objects dropped */
                    drop_stmt_pre_treatment((DropStmt*)parse_tree, query_string, sent_to_remote, &is_temp, &exec_type);

                    /*
                     * If I am the main execute CN but not CCN,
                     * Notify the CCN to create firstly, and then notify other CNs except me.
                     */
                    char* first_exec_node = NULL;
                    bool is_first_node = false;

                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        first_exec_node = find_first_exec_cn();
                        is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                    }
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                        (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node && !sent_to_remote) {
                            RemoteQuery* step = makeNode(RemoteQuery);
                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = (char*)query_string;
                            step->exec_type = EXEC_ON_COORDS;
                            step->exec_nodes = NULL;
                            step->is_temp = false;
                            ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                            pfree_ext(step);
                        }
                    }
#endif
                    RemoveObjects((DropStmt*)parse_tree, true);
#ifdef PGXC
                    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                                ExecUtilityStmtOnNodes_ParallelDDLMode(
                                    query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, is_temp, first_exec_node);
                        } else {
                            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, exec_type, is_temp);
                        }
                    }
#endif
                } break;
            }
            break;

        case T_TruncateStmt:
            PG_TRY();
            {
                u_sess->exec_cxt.isExecTrunc = true;
#ifdef PGXC
            /*
             * In Postgres-XC, TRUNCATE needs to be launched to remote nodes
             * before AFTER triggers. As this needs an internal control it is
             * managed by this function internally.
             */
            ExecuteTruncate((TruncateStmt*)parse_tree, query_string);
#else
        ExecuteTruncate((TruncateStmt*)parse_tree);
#endif
                u_sess->exec_cxt.isExecTrunc = false;
            }
            PG_CATCH();
            {
                u_sess->exec_cxt.isExecTrunc = false;
                PG_RE_THROW();
            }
            PG_END_TRY();

            break;

        case T_PurgeStmt:
            ExecutePurge((PurgeStmt*)parse_tree);
            break;

        case T_TimeCapsuleStmt:
            ExecuteTimeCapsule((TimeCapsuleStmt*)parse_tree);
            break;

        case T_CommentStmt:
            CommentObject((CommentStmt*)parse_tree);

#ifdef PGXC
            /* Comment objects depending on their object and temporary types */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                bool is_temp = false;
                ExecNodes* exec_nodes = NULL;
                CommentStmt* stmt = (CommentStmt*)parse_tree;
                RemoteQueryExecType exec_type = get_nodes_4_comment_utility(stmt, &is_temp, &exec_nodes);
                ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, false, exec_type, is_temp);
                FreeExecNodes(&exec_nodes);
            }
#endif
            break;

        case T_SecLabelStmt:
#ifdef PGXC
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SECURITY LABEL is not yet supported.")));
#endif /* PGXC */
            ExecSecLabelStmt((SecLabelStmt*)parse_tree);
            break;

        case T_CopyStmt: {
            if (((CopyStmt*)parse_tree)->filename != NULL && isSecurityMode && !IsInitdb) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("operation copy with file is forbidden in security mode.")));
            }
            uint64 processed;
            uint64 histhash;
            bool has_histhash;
            processed = DoCopy((CopyStmt*)parse_tree, query_string);
            has_histhash = ((CopyStmt*)parse_tree)->hashstate.has_histhash;
            histhash = ((CopyStmt*)parse_tree)->hashstate.histhash;
            if (completion_tag != NULL) {
                if (has_histhash && !IsConnFromApp()) {
                    errorno = snprintf_s(completion_tag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "COPY " UINT64_FORMAT " " UINT64_FORMAT,
                    processed, histhash);
                } else {
                    errorno = snprintf_s(completion_tag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "COPY " UINT64_FORMAT,
                    processed);
                }
                securec_check_ss(errorno, "\0", "\0");
            }
            report_utility_time(parse_tree);
        } break;

        case T_PrepareStmt:
            CheckRestrictedOperation("PREPARE");
            PrepareQuery((PrepareStmt*)parse_tree, query_string);
            break;

        case T_ExecuteStmt:
            /*
             * Check the prepared stmt is ok for executing directly, otherwise
             * RePrepareQuery proc should be called to re-generated a new prepared stmt.
             */
            if (needRecompileQuery((ExecuteStmt*)parse_tree))
                RePrepareQuery((ExecuteStmt*)parse_tree);
            ExecuteQuery((ExecuteStmt*)parse_tree, NULL, query_string, params, dest, completion_tag);
            break;

        case T_DeallocateStmt:
            CheckRestrictedOperation("DEALLOCATE");
            DeallocateQuery((DeallocateStmt*)parse_tree);
            break;

            /*
             * schema
             */
        case T_RenameStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                RenameStmt* stmt = (RenameStmt*)parse_tree;
                RemoteQueryExecType exec_type;
                bool is_temp = false;
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                CheckObjectInBlackList(stmt->renameType, query_string);

                /* Try to use the object relation if possible */
                if (stmt->relation) {
                    /*
                     * When a relation is defined, it is possible that this object does
                     * not exist but an IF EXISTS clause might be used. So we do not do
                     * any error check here but block the access to remote nodes to
                     * this object as it does not exisy
                     */
                    Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);

                    if (OidIsValid(rel_id)) {
                        // Check relations's internal mask
                        Relation rel = relation_open(rel_id, NoLock);
                        if ((RelationGetInternalMask(rel) & INTERNAL_MASK_DALTER))
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("internal relation doesn't allow ALTER")));

                        if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("target table is a foreign table")));

                        if (rel->rd_rel->relkind == RELKIND_STREAM)
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("target table is a stream")));

                        if (RelationIsPAXFormat(rel)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("RENAME operation is not supported for DFS table.")));
                        }
                        relation_close(rel, NoLock);

                        UnlockRelationOid(rel_id, AccessShareLock);

                        exec_type = ExecUtilityFindNodes(stmt->renameType, rel_id, &is_temp);
                    } else
                        exec_type = EXEC_ON_NONE;
                } else {
                    exec_type = ExecUtilityFindNodes(stmt->renameType, InvalidOid, &is_temp);
                }

                /* Clean also remote Coordinators */
                if (stmt->renameType == OBJECT_DATABASE) {
                    /* clean all connections with dbname on all CNs before db operations */
                    PreCleanAndCheckConns(stmt->subname, stmt->missing_ok);
                } else if (stmt->renameType == OBJECT_USER || stmt->renameType == OBJECT_ROLE) {
                    /* clean all connections with username on all CNs before user operations */
                    PreCleanAndCheckUserConns(stmt->subname, stmt->missing_ok);
                }

                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node &&
                    (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        NULL,
                        sent_to_remote,
                        false,
                        EXEC_ON_COORDS,
                        is_temp,
                        first_exec_node,
                        (Node*)parse_tree);
                }

                ExecRenameStmt((RenameStmt*)parse_tree);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            EXEC_ON_DATANODES,
                            is_temp,
                            first_exec_node,
                            (Node*)parse_tree);
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, NULL, sent_to_remote, false, exec_type, is_temp, (Node*)parse_tree);
                }
#ifdef ENABLE_MULTIPLE_NODES
                UpdatePartPolicyWhenRenameRelation((RenameStmt*)parse_tree);
#endif
            } else {
                if (IS_SINGLE_NODE) {
                    CheckObjectInBlackList(((RenameStmt*)parse_tree)->renameType, query_string);
                    RenameStmt* stmt = (RenameStmt*)parse_tree;
                    /* Try to use the object relation if possible */
                    if (stmt->relation) {
                        Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                        if (OidIsValid(rel_id)) {
                            // Check relations's internal mask
                            Relation rel = relation_open(rel_id, NoLock);
                            if ((RelationGetInternalMask(rel) & INTERNAL_MASK_DALTER))
                                ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                        errmsg("Un-support feature"),
                                        errdetail("internal relation doesn't allow ALTER")));

                            relation_close(rel, NoLock);
                            UnlockRelationOid(rel_id, AccessShareLock);
                        }
                    }
                }
                ExecRenameStmt((RenameStmt*)parse_tree);
            }
#else
        ExecRenameStmt((RenameStmt*)parse_tree);
#endif
            break;

        case T_AlterObjectSchemaStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                AlterObjectSchemaStmt* stmt = (AlterObjectSchemaStmt*)parse_tree;
                RemoteQueryExecType exec_type;
                bool is_temp = false;

                CheckObjectInBlackList(stmt->objectType, query_string);

                /* Try to use the object relation if possible */
                if (stmt->relation) {
                    /*
                     * When a relation is defined, it is possible that this object does
                     * not exist but an IF EXISTS clause might be used. So we do not do
                     * any error check here but block the access to remote nodes to
                     * this object as it does not exisy
                     */
                    Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);

                    if (OidIsValid(rel_id)) {
                        Relation rel = relation_open(rel_id, NoLock);
                        if ((RelationGetInternalMask(rel) & INTERNAL_MASK_DALTER))
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("internal relation doesn't allow ALTER")));

                        if (rel->rd_rel->relkind == RELKIND_RELATION && RelationIsPAXFormat(rel)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("Un-support feature"),
                                    errdetail("DFS table doesn't allow ALTER TABLE SET SCHEMA")));
                        }
                        relation_close(rel, NoLock);
                        UnlockRelationOid(rel_id, AccessShareLock);
                        exec_type = ExecUtilityFindNodes(stmt->objectType, rel_id, &is_temp);
                    } else
                        exec_type = EXEC_ON_NONE;
                } else {
                    exec_type = ExecUtilityFindNodes(stmt->objectType, InvalidOid, &is_temp);
                }

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            EXEC_ON_COORDS,
                            is_temp,
                            first_exec_node,
                            (Node*)parse_tree);
                    }
                }

                ExecAlterObjectSchemaStmt((AlterObjectSchemaStmt*)parse_tree);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            EXEC_ON_DATANODES,
                            is_temp,
                            first_exec_node,
                            (Node*)parse_tree);
                    }
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, NULL, sent_to_remote, false, exec_type, is_temp, (Node*)parse_tree);
                }
            } else {
                if (IS_SINGLE_NODE) {
                    CheckObjectInBlackList(((AlterObjectSchemaStmt*)parse_tree)->objectType, query_string);
                    AlterObjectSchemaStmt* stmt = (AlterObjectSchemaStmt*)parse_tree;
                    if (stmt->relation) {
                        Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, true);
                        if (OidIsValid(rel_id)) {
                            // Check relations's internal mask
                            Relation rel = relation_open(rel_id, NoLock);
                            if ((RelationGetInternalMask(rel) & INTERNAL_MASK_DALTER))
                                ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                        errmsg("Un-support feature"),
                                        errdetail("internal relation doesn't allow ALTER")));

                            relation_close(rel, NoLock);
                            UnlockRelationOid(rel_id, AccessShareLock);
                        }
                    }
                }
                ExecAlterObjectSchemaStmt((AlterObjectSchemaStmt*)parse_tree);
            }
#else
        ExecAlterObjectSchemaStmt((AlterObjectSchemaStmt*)parse_tree);
#endif
            break;

        case T_AlterOwnerStmt:
            CheckObjectInBlackList(((AlterOwnerStmt*)parse_tree)->objectType, query_string);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                ExecNodes* exec_nodes = NULL;
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                AlterOwnerStmt* OwnerStmt = (AlterOwnerStmt*)parse_tree;

                if (OwnerStmt->objectType == OBJECT_FUNCTION) {
                    Oid funcid = LookupFuncNameTypeNames(OwnerStmt->object, OwnerStmt->objarg, false);

                    exec_nodes = GetFunctionNodes(funcid);
                }

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    ExecAlterOwnerStmt((AlterOwnerStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, exec_nodes, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    ExecAlterOwnerStmt((AlterOwnerStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                ExecAlterOwnerStmt((AlterOwnerStmt*)parse_tree);
            }
#else
        ExecAlterOwnerStmt((AlterOwnerStmt*)parse_tree);
#endif

            break;

        case T_AlterTableStmt: {
            AlterTableStmt* atstmt = (AlterTableStmt*)parse_tree;
            LOCKMODE lockmode;
            char* first_exec_node = NULL;
            bool is_first_node = false;

            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                first_exec_node = find_first_exec_cn();
                is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
            }

            /*
             * Figure out lock mode, and acquire lock.	This also does
             * basic permissions checks, so that we won't wait for a lock
             * on (for example) a relation on which we have no
             * permissions.
             */
            lockmode = AlterTableGetLockLevel(atstmt->cmds);

#ifdef PGXC
            /*
             * If I am the main execute CN but not CCN,
             * Notify the CCN to create firstly, and then notify other CNs except me.
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (!sent_to_remote) {
                        bool isTemp = false;
                        RemoteQueryExecType exec_type;
                        RemoteQuery* step = makeNode(RemoteQuery);

                        Oid rel_id = RangeVarGetRelid(atstmt->relation, lockmode, true);

                        if (OidIsValid(rel_id)) {
                            exec_type = ExecUtilityFindNodes(atstmt->relkind, rel_id, &isTemp);

                            if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)
                                step->exec_type = EXEC_ON_COORDS;
                            else
                                step->exec_type = EXEC_ON_NONE;

                            step->combine_type = COMBINE_TYPE_SAME;
                            step->sql_statement = (char*)query_string;
                            step->is_temp = isTemp;
                            step->exec_nodes = NULL;
                            UnlockRelationOid(rel_id, lockmode);
                            ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                            pfree_ext(step);
                        }
                    }
                }
            }
#endif
            Oid rel_id;
            List* stmts = NIL;
            ListCell* l = NULL;
            char* drop_seq_string = NULL;
            ExecNodes* exec_nodes = NULL;

            rel_id = AlterTableLookupRelation(atstmt, lockmode);
            elog(DEBUG1,
                "[GET LOCK] Get the lock %d successfully on relation %s for altering operator.",
                lockmode,
                atstmt->relation->relname);

            if (OidIsValid(rel_id)) {
                TrForbidAccessRbObject(RelationRelationId, rel_id, atstmt->relation->relname);

                /* Run parse analysis ... */
                stmts = transformAlterTableStmt(rel_id, atstmt, query_string);

                if (u_sess->attr.attr_sql.enable_cluster_resize) {
                    ATMatviewGroup(stmts, rel_id, lockmode);
                }
#ifdef PGXC
                /*
                 * Add a RemoteQuery node for a query at top level on a remote
                 * Coordinator, if not already done so
                 */
                if (!sent_to_remote && !ISMATMAP(atstmt->relation->relname) && !ISMLOG(atstmt->relation->relname)) {
                    /* nodegroup attch execnodes */
                    add_remote_query_4_alter_stmt(is_first_node, atstmt, query_string, &stmts, &drop_seq_string, &exec_nodes);
                }
#endif

                /* ... and do it */
                foreach (l, stmts) {
                    Node* stmt = (Node*)lfirst(l);

                    if (IsA(stmt, AlterTableStmt)) {
                        /* Do the table alteration proper */
                        AlterTable(rel_id, lockmode, (AlterTableStmt*)stmt);
                    } else {
                        /* Recurse for anything else */
                        ProcessUtility(stmt,
                            query_string,
                            params,
                            false,
                            None_Receiver,
#ifdef PGXC
                            true,
#endif /* PGXC */
                            NULL);
                    }

                    /* Need CCI between commands */
                    if (lnext(l) != NULL)
                        CommandCounterIncrement();
                }
#ifdef ENABLE_MULTIPLE_NODES
                if (drop_seq_string != NULL) {
                    Assert(exec_nodes != NULL);
                    exec_remote_query_4_seq(exec_nodes, drop_seq_string, INVALIDSEQUUID);
                }
#endif
            } else {
                ereport(NOTICE, (errmsg("relation \"%s\" does not exist, skipping", atstmt->relation->relname)));
            }
            report_utility_time(parse_tree);
            pfree_ext(drop_seq_string);
        } break;

        case T_AlterDomainStmt:
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("domain is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
            {
                AlterDomainStmt* stmt = (AlterDomainStmt*)parse_tree;

                /*
                 * Some or all of these functions are recursive to cover
                 * inherited things, so permission checks are done there.
                 */
                switch (stmt->subtype) {
                    case 'T': /* ALTER DOMAIN DEFAULT */

                        /*
                         * Recursively alter column default for table and, if
                         * requested, for descendants
                         */
                        AlterDomainDefault(stmt->typname, stmt->def);
                        break;
                    case 'N': /* ALTER DOMAIN DROP NOT NULL */
                        AlterDomainNotNull(stmt->typname, false);
                        break;
                    case 'O': /* ALTER DOMAIN SET NOT NULL */
                        AlterDomainNotNull(stmt->typname, true);
                        break;
                    case 'C': /* ADD CONSTRAINT */
                        AlterDomainAddConstraint(stmt->typname, stmt->def);
                        break;
                    case 'X': /* DROP CONSTRAINT */
                        AlterDomainDropConstraint(stmt->typname, stmt->name, stmt->behavior, stmt->missing_ok);
                        break;
                    case 'V': /* VALIDATE CONSTRAINT */
                        AlterDomainValidateConstraint(stmt->typname, stmt->name);
                        break;
                    default: /* oops */
                    {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized alter domain type: %d", (int)stmt->subtype)));
                    } break;
                }
            }
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_GrantStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                RemoteQueryExecType remoteExecType = EXEC_ON_ALL_NODES;
                GrantStmt* stmt = (GrantStmt*)parse_tree;
                bool is_temp = false;
                ExecNodes* exec_nodes = NULL;

                /* Launch GRANT on Coordinator if object is a sequence */
                if ((stmt->objtype == ACL_OBJECT_RELATION && stmt->targtype == ACL_TARGET_OBJECT)) {
                    /*
                     * In case object is a relation, differenciate the case
                     * of a sequence, a view and a table
                     */
                    ListCell* cell = NULL;
                    /* Check the list of objects */
                    bool first = true;
                    RemoteQueryExecType type_local = remoteExecType;

                    foreach (cell, stmt->objects) {
                        RangeVar* relvar = (RangeVar*)lfirst(cell);
                        Oid rel_id = RangeVarGetRelid(relvar, NoLock, true);

                        /* Skip if object does not exist */
                        if (!OidIsValid(rel_id))
                            continue;

                        remoteExecType = exec_utility_find_nodes_relkind(rel_id, &is_temp);

                        /* Check if object node type corresponds to the first one */
                        if (first) {
                            type_local = remoteExecType;
                            first = false;
                        } else {
                            if (type_local != remoteExecType)
                                ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                        errmsg("PGXC does not support GRANT on multiple object types"),
                                        errdetail("Grant VIEW/TABLE with separate queries")));
                        }
                    }
                } else if (stmt->objtype == ACL_OBJECT_NODEGROUP && stmt->targtype == ACL_TARGET_OBJECT) {
                    /* For NodeGroup's grant/revoke operation we only issue comments on CN nodes */
                    remoteExecType = EXEC_ON_COORDS;
                }

                if (remoteExecType != EXEC_ON_COORDS &&
                    (stmt->objtype == ACL_OBJECT_RELATION || stmt->objtype == ACL_OBJECT_SEQUENCE ||
                        stmt->objtype == ACL_OBJECT_FUNCTION)) {
                    /* Only for tables, foreign tables, sequences and functions, not views */
                    Oid group_oid = GrantStmtGetNodeGroup(stmt);
                    if (!OidIsValid(group_oid))
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("NOT-SUPPORT: Not support Grant/Revoke privileges"
                                       " to objects in different nodegroup")));

                    exec_nodes = GetNodeGroupExecNodes(group_oid);
                }

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (remoteExecType == EXEC_ON_ALL_NODES || remoteExecType == EXEC_ON_COORDS) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            EXEC_ON_COORDS,
                            is_temp,
                            first_exec_node,
                            (Node*)stmt);
                    }
                }

                ExecuteGrantStmt((GrantStmt*)parse_tree);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (remoteExecType == EXEC_ON_ALL_NODES || remoteExecType == EXEC_ON_DATANODES) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            exec_nodes,
                            sent_to_remote,
                            false,
                            EXEC_ON_DATANODES,
                            is_temp,
                            first_exec_node,
                            (Node*)stmt);
                    }
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, exec_nodes, sent_to_remote, false, remoteExecType, is_temp, (Node*)stmt);
                }
            } else {
                ExecuteGrantStmt((GrantStmt*)parse_tree);
            }
#else
        ExecuteGrantStmt((GrantStmt*)parse_tree);
#endif
            break;

        case T_GrantRoleStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    GrantRole((GrantRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    GrantRole((GrantRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                GrantRole((GrantRoleStmt*)parse_tree);
            }
#else
        GrantRole((GrantRoleStmt*)parse_tree);
#endif
            break;

        case T_GrantDbStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    ExecuteGrantDbStmt((GrantDbStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    ExecuteGrantDbStmt((GrantDbStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                ExecuteGrantDbStmt((GrantDbStmt*)parse_tree);
            }
#else
        ExecuteGrantDbStmt((GrantDbStmt*)parse_tree);
#endif
            break;

        case T_AlterDefaultPrivilegesStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    ExecAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    ExecAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                ExecAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)parse_tree);
            }
#else
        ExecAlterDefaultPrivilegesStmt((AlterDefaultPrivilegesStmt*)parse_tree);
#endif
            break;

            /*
             * **************** object creation / destruction *****************
             */
        case T_DefineStmt: {
            DefineStmt* stmt = (DefineStmt*)parse_tree;

            switch (stmt->kind) {
                case OBJECT_AGGREGATE:
#ifdef ENABLE_MULTIPLE_NODES
                    if (!u_sess->attr.attr_common.IsInplaceUpgrade && !u_sess->exec_cxt.extension_is_valid)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("user defined aggregate is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
                    DefineAggregate(stmt->defnames, stmt->args, stmt->oldstyle, stmt->definition);
                    break;
                case OBJECT_OPERATOR:
#ifdef ENABLE_MULTIPLE_NODES
                    if (!u_sess->attr.attr_common.IsInplaceUpgrade && !u_sess->exec_cxt.extension_is_valid)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("user defined operator is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineOperator(stmt->defnames, stmt->definition);
                    break;
                case OBJECT_TYPE:
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineType(stmt->defnames, stmt->definition);
                    break;
                case OBJECT_TSPARSER:
#ifdef PGXC
                    if (!IsInitdb) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("user-defined text search parser is not yet supported.")));
                    }
#endif /* PGXC */
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineTSParser(stmt->defnames, stmt->definition);
                    break;
                case OBJECT_TSDICTIONARY:
                    /* not support with 300 */
                    ts_check_feature_disable();
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineTSDictionary(stmt->defnames, stmt->definition);
                    break;
                case OBJECT_TSTEMPLATE:
#ifdef PGXC
                    /*
                     * An erroneous text search template definition could confuse or
                     * even crash the server, so we just forbid user to create a user
                     * defined text search template definition
                     */
                    if (!IsInitdb) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("user-defined text search template is not yet supported.")));
                    }
#endif /* PGXC */
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineTSTemplate(stmt->defnames, stmt->definition);
                    break;
                case OBJECT_TSCONFIGURATION:
                    ts_check_feature_disable();
                    /* use 'args' filed to record configuration options */
                    DefineTSConfiguration(stmt->defnames, stmt->definition, stmt->args);
                    break;
                case OBJECT_COLLATION:
#ifdef PGXC
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("user defined collation is not yet supported.")));
#endif /* PGXC */
                    AssertEreport(stmt->args == NIL, MOD_EXECUTOR, "stmt args is NULL");
                    DefineCollation(stmt->defnames, stmt->definition);
                    break;
                default: {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized define stmt type: %d", (int)stmt->kind)));
                } break;
            }
        }
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CompositeTypeStmt: /* CREATE TYPE (composite) */
        {
            CompositeTypeStmt* stmt = (CompositeTypeStmt*)parse_tree;

#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DefineCompositeType(stmt->typevar, stmt->coldeflist);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DefineCompositeType(stmt->typevar, stmt->coldeflist);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else
#endif
            {
                DefineCompositeType(stmt->typevar, stmt->coldeflist);
            }
        } break;

        case T_TableOfTypeStmt: /* CREATE TYPE AS TABLE OF */
        {
            TableOfTypeStmt* stmt = (TableOfTypeStmt*)parse_tree;

            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DefineTableOfType(stmt);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DefineTableOfType(stmt);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DefineTableOfType(stmt);
            }
        } break;

        case T_CreateEnumStmt: /* CREATE TYPE AS ENUM */
        {
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DefineEnum((CreateEnumStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DefineEnum((CreateEnumStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else
#endif
            {
                DefineEnum((CreateEnumStmt*)parse_tree);
            }
        } break;

        case T_CreateRangeStmt: /* CREATE TYPE AS RANGE */
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined range type is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
            DefineRange((CreateRangeStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterEnumStmt: /* ALTER TYPE (enum) */
        {
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
#endif
            {
                /*
                 * We disallow this in transaction blocks, because we can't cope
                 * with enum OID values getting into indexes and then having
                 * their defining pg_enum entries go away.
                 */
                PreventTransactionChain(is_top_level, "ALTER TYPE ... ADD");
            }

#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterEnum((AlterEnumStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterEnum((AlterEnumStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else
#endif
            {
                AlterEnum((AlterEnumStmt*)parse_tree);
            }
        } break;

        case T_ViewStmt: /* CREATE VIEW */
        {
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                ViewStmt *vstmt = (ViewStmt*)parse_tree;

                /*
                 * Run parse analysis to convert the raw parse tree to a Query.  Note this
                 * also acquires sufficient locks on the source table(s).
                 *
                 * Since parse analysis scribbles on its input, copy the raw parse tree;
                 * this ensures we don't corrupt a prepared statement, for example.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    bool is_temp = IsViewTemp((ViewStmt*)parse_tree, query_string);

                    if (!is_temp) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    }

                    if (vstmt->relkind == OBJECT_MATVIEW) {
                        CreateCommand((CreateStmt *)vstmt->mv_stmt, vstmt->mv_sql, NULL, true, true);
                        CommandCounterIncrement();

                        acquire_mativew_tables_lock((Query *)vstmt->query, true);
                    }
                    DefineView((ViewStmt*)parse_tree, query_string, sent_to_remote, is_first_node);

                    if (!is_temp) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            (u_sess->attr.attr_common.IsInplaceUpgrade ? EXEC_ON_DATANODES : EXEC_ON_NONE),
                            false,
                            first_exec_node);
                    }

                } else {
                    if (vstmt->relkind == OBJECT_MATVIEW) {
                        CreateCommand((CreateStmt *)vstmt->mv_stmt, vstmt->mv_sql, NULL, true, true);
                        CommandCounterIncrement();

                        acquire_mativew_tables_lock((Query *)vstmt->query, true);
                    }
                    DefineView((ViewStmt*)parse_tree, query_string, sent_to_remote, is_first_node);

                    char* schema_name = ((ViewStmt*)parse_tree)->view->schemaname;
                    if (schema_name == NULL)
                        schema_name = DatumGetCString(DirectFunctionCall1(current_schema, PointerGetDatum(NULL)));

                    bool temp_schema = false;
                    if (schema_name != NULL)
                        temp_schema = (strncasecmp(schema_name, "pg_temp", 7) == 0) ? true : false;
                    if (!ExecIsTempObjectIncluded() && !temp_schema)
                        ExecUtilityStmtOnNodes(query_string,
                            NULL,
                            sent_to_remote,
                            false,
                            (u_sess->attr.attr_common.IsInplaceUpgrade ? EXEC_ON_ALL_NODES : EXEC_ON_COORDS),
                            false);
                }
            } else {
                ViewStmt *vstmt = (ViewStmt*)parse_tree;
                if (vstmt->relkind == OBJECT_MATVIEW) {
                    CreateCommand((CreateStmt *)vstmt->mv_stmt, vstmt->mv_sql, NULL, true, true);
                    CommandCounterIncrement();

                    acquire_mativew_tables_lock((Query *)vstmt->query, true);
                }
                DefineView((ViewStmt*)parse_tree, query_string, sent_to_remote, query_string);
            }
#else
        DefineView((ViewStmt*)parse_tree, query_string);
#endif
        } break;

        case T_CreateFunctionStmt: /* CREATE FUNCTION */
        {
            PG_TRY();
            {
                CreateFunction((CreateFunctionStmt*)parse_tree, query_string, InvalidOid);
            }
            PG_CATCH();
            {
                if (u_sess->plsql_cxt.debug_query_string) {
                    pfree_ext(u_sess->plsql_cxt.debug_query_string);
                }
                PG_RE_THROW();
            }
            PG_END_TRY();
#ifdef PGXC
            Oid group_oid;
            bool multi_group = false;
            ExecNodes* exec_nodes = NULL;
            const char* query_str = NULL;

            if (IS_PGXC_COORDINATOR) {
                group_oid = GetFunctionNodeGroup((CreateFunctionStmt*)parse_tree, &multi_group);
                if (multi_group) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Does not support FUNCTION with multiple nodegroup table type in logic cluster."),
                            errdetail("The feature is not currently supported")));
                }

                query_str = GetCreateFuncStringInDN((CreateFunctionStmt*)parse_tree, query_string);

                if (OidIsValid(group_oid)) {
                    exec_nodes = GetNodeGroupExecNodes(group_oid);
                }

                ExecUtilityStmtOnNodes(
                    query_str, exec_nodes, sent_to_remote, false, CHOOSE_EXEC_NODES(ExecIsTempObjectIncluded()), false);

                FreeExecNodes(&exec_nodes);
                if (query_str != query_string)
                    pfree_ext(query_str);
            }
#endif
        } break;

        case T_CreatePackageStmt: /* CREATE PACKAGE SPECIFICATION*/
        {
#ifdef ENABLE_MULTIPLE_NODES
                ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
            PG_TRY();
            {
                CreatePackageCommand((CreatePackageStmt*)parse_tree, query_string);
            }
            PG_CATCH();
            {
                if (u_sess->plsql_cxt.debug_query_string) {
                    pfree_ext(u_sess->plsql_cxt.debug_query_string);
                }
                PG_RE_THROW();
            }
            PG_END_TRY();
        } break;
        case T_CreatePackageBodyStmt: /* CREATE PACKAGE SPECIFICATION*/
        {
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
            PG_TRY();
            {
                CreatePackageBodyCommand((CreatePackageBodyStmt*)parse_tree, query_string);
            }
            PG_CATCH();
            {
                if (u_sess->plsql_cxt.debug_query_string) {
                    pfree_ext(u_sess->plsql_cxt.debug_query_string);
                }
                PG_RE_THROW();
            }
            PG_END_TRY();
        } break;


        case T_AlterFunctionStmt: /* ALTER FUNCTION */
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                ExecNodes* exec_nodes = NULL;

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    bool is_temp = IsFunctionTemp((AlterFunctionStmt*)parse_tree);
                    if (!is_temp) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    }

                    AlterFunction((AlterFunctionStmt*)parse_tree);
                    Oid group_oid = GetFunctionNodeGroup((AlterFunctionStmt*)parse_tree);
                    if (OidIsValid(group_oid)) {
                        exec_nodes = GetNodeGroupExecNodes(group_oid);
                    }

                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, exec_nodes, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterFunction((AlterFunctionStmt*)parse_tree);
                    Oid group_oid = GetFunctionNodeGroup((AlterFunctionStmt*)parse_tree);
                    if (OidIsValid(group_oid)) {
                        exec_nodes = GetNodeGroupExecNodes(group_oid);
                    }

                    ExecUtilityStmtOnNodes(query_string,
                        exec_nodes,
                        sent_to_remote,
                        false,
                        CHOOSE_EXEC_NODES(ExecIsTempObjectIncluded()),
                        false);
                }
                FreeExecNodes(&exec_nodes);
            } else {
                AlterFunction((AlterFunctionStmt*)parse_tree);
            }
#else
        AlterFunction((AlterFunctionStmt*)parse_tree);
#endif
            break;

        case T_IndexStmt: /* CREATE INDEX */
        {
            IndexStmt* stmt = (IndexStmt*)parse_tree;
            Oid rel_id;
            LOCKMODE lockmode;

            if (stmt->concurrent) {
                PreventTransactionChain(is_top_level, "CREATE INDEX CONCURRENTLY");
            }

            /* forbid user to set or change inner options */
            ForbidOutUsersToSetInnerOptions(stmt->options);
            ForbidToSetTdeOptionsForNonTdeTbl(stmt->options);

#ifdef PGXC
            bool is_temp = false;
            ExecNodes* exec_nodes = NULL;
            RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
            char* first_exec_node = NULL;
            bool is_first_node = false;

            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                first_exec_node = find_first_exec_cn();
                is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
            }

#ifdef ENABLE_MULTIPLE_NODES
            if (stmt->concurrent && t_thrd.proc->workingVersionNum < CREATE_INDEX_CONCURRENTLY_DIST_VERSION_NUM) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PGXC does not support concurrent INDEX yet"),
                        errdetail("The feature is not currently supported")));
            }
#endif
            /* INDEX on a temporary table cannot use 2PC at commit */
            rel_id = RangeVarGetRelidExtended(stmt->relation, AccessShareLock, true, false, false, true, NULL, NULL);

            if (OidIsValid(rel_id)) {
                exec_type = ExecUtilityFindNodes(OBJECT_INDEX, rel_id, &is_temp);
                UnlockRelationOid(rel_id, AccessShareLock);
            } else {
                exec_type = EXEC_ON_NONE;
            }

            /*
             * If I am the main execute CN but not CCN,
             * Notify the CCN to create firstly, and then notify other CNs except me.
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !stmt->isconstraint) {
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node &&
                    (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, stmt->concurrent, EXEC_ON_COORDS, is_temp, first_exec_node);
                }
            }
#endif
            /*
             * Look up the relation OID just once, right here at the
             * beginning, so that we don't end up repeating the name
             * lookup later and latching onto a different relation
             * partway through.  To avoid lock upgrade hazards, it's
             * important that we take the strongest lock that will
             * eventually be needed here, so the lockmode calculation
             * needs to match what DefineIndex() does.
             */
            lockmode = stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock;
            rel_id = RangeVarGetRelidExtended(
                stmt->relation, lockmode, false, false, false, true, RangeVarCallbackOwnsRelation, NULL);
            /* Run parse analysis ... */
            stmt = transformIndexStmt(rel_id, stmt, query_string);
#ifdef PGXC
            /* Find target datanode list that we need send CREATE-INDEX on.
             * If it's a view, skip it. */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && get_rel_relkind(rel_id) == RELKIND_RELATION) {
                int nmembers = 0;
                Oid group_oid = InvalidOid;
                Oid* members = NULL;
                exec_nodes = makeNode(ExecNodes);
                char in_redistribution = 'n';

                AssertEreport(rel_id != InvalidOid, MOD_EXECUTOR, "relation OID is invalid");

                /* Get nodegroup Oid from the index's base table */
                group_oid = get_pgxc_class_groupoid(rel_id);
                AssertEreport(group_oid != InvalidOid, MOD_EXECUTOR, "group OID is invalid");

                in_redistribution = get_pgxc_group_redistributionstatus(group_oid);
                char* group_name = get_pgxc_groupname(group_oid);
                /* Get node list and appending to exec_nodes */
                if (need_full_dn_execution(group_name)) {
                    /* Sepcial path, issue full-DN create index request */
                    exec_nodes->nodeList = GetAllDataNodes();
                } else {
                    nmembers = get_pgxc_groupmembers(group_oid, &members);
                    exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
                    pfree_ext(members);
                }
            }
#endif
            pgstat_set_io_state(IOSTATE_WRITE);
            /* ... and do it */
            WaitState oldStatus = pgstat_report_waitstatus(STATE_CREATE_INDEX);
#ifdef ENABLE_MULTIPLE_NODES
            /*
             * timeseries index create should be in allowSystemTableMods for
             * the index should be create in cstore namespace.
             */
            bool origin_sysTblMode = g_instance.attr.attr_common.allowSystemTableMods;
            if (ts_idx_create && !origin_sysTblMode) {
                g_instance.attr.attr_common.allowSystemTableMods = true;
            }
#endif

            Oid indexRelOid = DefineIndex(rel_id,
                stmt,
                InvalidOid,                                /* no predefined OID */
                false,                                     /* is_alter_table */
                true,                                      /* check_rights */
                !u_sess->upg_cxt.new_catalog_need_storage, /* skip_build */
                false);                                    /* quiet */

#ifndef ENABLE_MULTIPLE_NODES
            if (RelationIsCUFormatByOid(rel_id) && (stmt->primary || stmt->unique)) {
                DefineDeltaUniqueIndex(rel_id, stmt, indexRelOid);
            }
#endif
            pgstat_report_waitstatus(oldStatus);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !stmt->isconstraint && !IsConnFromCoord()) {
                query_string = ConstructMesageWithMemInfo(query_string, stmt->memUsage);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node &&
                    (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)) {
                    ExecUtilityStmtOnNodes(
                        query_string, exec_nodes, sent_to_remote, stmt->concurrent, EXEC_ON_DATANODES, is_temp);
                } else {
                    ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, stmt->concurrent, exec_type, is_temp);
                }
#ifdef ENABLE_MULTIPLE_NODES
                /* Force non-concurrent build on temporary relations, even if CONCURRENTLY was requested */
                char relPersistence = get_rel_persistence(rel_id);
                if (stmt->concurrent &&
                    !(relPersistence == RELPERSISTENCE_TEMP || relPersistence == RELPERSISTENCE_GLOBAL_TEMP)) {
                    /* for index if caller didn't specify, use oid get indexname. */
                    mark_indisvalid_all_cns(stmt->relation->schemaname, get_rel_name(indexRelOid));
                }
#endif
            }
            FreeExecNodes(&exec_nodes);
#endif
        } break;

        case T_RuleStmt: /* CREATE RULE */
        {
#ifdef ENABLE_MULTIPLE_NODES
            bool isredis_rule = false;
            isredis_rule = is_redis_rule((RuleStmt*)parse_tree);
            if (!IsInitdb && !u_sess->attr.attr_sql.enable_cluster_resize && !u_sess->exec_cxt.extension_is_valid 
                            && !IsConnFromCoord() && !isredis_rule)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("RULE is not yet supported.")));
#endif
            DefineRule((RuleStmt*)parse_tree, query_string);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                RemoteQueryExecType exec_type;
                bool is_temp = false;
                exec_type = get_nodes_4_rules_utility(((RuleStmt*)parse_tree)->relation, &is_temp);
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, exec_type, is_temp);
            }
#endif
        }
            break;

        case T_CreateSeqStmt:
        {
#ifdef PGXC
            CreateSeqStmt* stmt = (CreateSeqStmt*)parse_tree;
            /*
             * We must not scribble on the passed-in CreateSeqStmt, so copy it.  (This is
             * overkill, but easy.)
             */
            stmt = (CreateSeqStmt*)copyObject(stmt);
            if (IS_PGXC_COORDINATOR) {
                ExecNodes* exec_nodes = NULL;

                char* query_stringWithUUID = gen_hybirdmsg_for_CreateSeqStmt(stmt, query_string);

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (!stmt->is_serial) {
                        bool is_temp = stmt->sequence->relpersistence == RELPERSISTENCE_TEMP;

                        if (!is_temp) {
                            ExecUtilityStmtOnNodes_ParallelDDLMode(
                                query_stringWithUUID, NULL, sent_to_remote, false, EXEC_ON_COORDS, is_temp, first_exec_node);
                        }
                    }
                }

                DefineSequenceWrapper(stmt);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (!stmt->is_serial) {
                        bool is_temp = stmt->sequence->relpersistence == RELPERSISTENCE_TEMP;

                        exec_nodes = GetOwnedByNodes((Node*)stmt);
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_stringWithUUID,
                            exec_nodes,
                            sent_to_remote,
                            false,
                            EXEC_ON_DATANODES,
                            is_temp,
                            first_exec_node);
                    }
                } else {
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (!stmt->is_serial) {
                        bool is_temp = stmt->sequence->relpersistence == RELPERSISTENCE_TEMP;
                        exec_nodes = GetOwnedByNodes((Node*)stmt);
                        /* Set temporary object flag in pooler */
                        ExecUtilityStmtOnNodes(
                            query_stringWithUUID, exec_nodes, sent_to_remote, false, CHOOSE_EXEC_NODES(is_temp), is_temp);
                    }
                }
#ifdef ENABLE_MULTIPLE_NODES
                if (IS_MAIN_COORDINATOR && exec_nodes != NULL &&
                    exec_nodes->nodeList->length < u_sess->pgxc_cxt.NumDataNodes) {
                    /* NodeGroup: Create sequence in other datanodes without owned by */
                    char* msg = deparse_create_sequence((Node*)stmt, true);
                    exec_remote_query_4_seq(exec_nodes, msg, stmt->uuid);
                    pfree_ext(msg);
                }
#endif
                pfree_ext(query_stringWithUUID);
                FreeExecNodes(&exec_nodes);
            } else {
                DefineSequenceWrapper(stmt);
            }
#else
        DefineSequenceWrapper(stmt);
#endif

            ClearCreateSeqStmtUUID(stmt);
            break;
        }
        case T_AlterSeqStmt:
#ifdef PGXC
            if (IS_MAIN_COORDINATOR || IS_SINGLE_NODE) {
                PreventAlterSeqInTransaction(is_top_level, (AlterSeqStmt*)parse_tree);
            }
            if (IS_PGXC_COORDINATOR) {
                AlterSeqStmt* stmt = (AlterSeqStmt*)parse_tree;

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                ExecNodes* exec_nodes = NULL;

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (!stmt->is_serial) {
                        bool is_temp = false;
                        RemoteQueryExecType exec_type;
                        Oid rel_id = RangeVarGetRelid(stmt->sequence, NoLock, stmt->missing_ok);

                        if (!OidIsValid(rel_id))
                            break;

                        exec_type = ExecUtilityFindNodes(OBJECT_SEQUENCE, rel_id, &is_temp);

                        if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS) {
                            ExecUtilityStmtOnNodes_ParallelDDLMode(
                                query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, is_temp, first_exec_node);
                        }
                    }

                    AlterSequenceWrapper((AlterSeqStmt*)parse_tree);
#ifdef ENABLE_MULTIPLE_NODES
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (IS_MAIN_COORDINATOR && !stmt->is_serial) {
                        bool is_temp = false;
                        RemoteQueryExecType exec_type;
                        Oid rel_id = RangeVarGetRelid(stmt->sequence, NoLock, true);

                        if (!OidIsValid(rel_id))
                            break;

                        exec_type = ExecUtilityFindNodes(OBJECT_SEQUENCE, rel_id, &is_temp);

                        if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES) {
                            exec_nodes = GetOwnedByNodes((Node*)stmt);
                            alter_sequence_all_nodes(stmt, exec_nodes);

                            ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                                exec_nodes,
                                sent_to_remote,
                                false,
                                EXEC_ON_DATANODES,
                                is_temp,
                                first_exec_node);
                        }
                    }
#endif
                } else {
                    AlterSequenceWrapper((AlterSeqStmt*)parse_tree);

#ifdef ENABLE_MULTIPLE_NODES
                    /* In case this query is related to a SERIAL execution, just bypass */
                    if (IS_MAIN_COORDINATOR && !stmt->is_serial) {
                        bool is_temp = false;
                        RemoteQueryExecType exec_type;
                        Oid rel_id = RangeVarGetRelid(stmt->sequence, NoLock, true);

                        if (!OidIsValid(rel_id))
                            break;

                        exec_type = ExecUtilityFindNodes(OBJECT_SEQUENCE, rel_id, &is_temp);

                        exec_nodes = GetOwnedByNodes((Node*)stmt);
                        alter_sequence_all_nodes(stmt, exec_nodes);

                        ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, false, exec_type, is_temp);
                    }
#endif
                }
                FreeExecNodes(&exec_nodes);
            } else {
                AlterSequenceWrapper((AlterSeqStmt*)parse_tree);
            }
#else
        PreventAlterSeqInTransaction(is_top_level, (AlterSeqStmt*)parse_tree);
        AlterSequenceWrapper((AlterSeqStmt*)parse_tree);
#endif
            break;

        case T_DoStmt:
            /* This change is from PG11 commit/rollback patch */
            ExecuteDoStmt((DoStmt*) parse_tree, 
                (!u_sess->SPI_cxt.is_allow_commit_rollback));
            break;

        case T_CreatedbStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                PreventTransactionChain(is_top_level, "CREATE DATABASE");

                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    exec_utility_with_message_parallel_ddl_mode(
                        query_string, sent_to_remote, false, first_exec_node, EXEC_ON_COORDS);
                    createdb((CreatedbStmt*)parse_tree);
                    exec_utility_with_message_parallel_ddl_mode(
                        query_string, sent_to_remote, false, first_exec_node, EXEC_ON_DATANODES);
                } else {
                    createdb((CreatedbStmt*)parse_tree);
                    ExecUtilityWithMessage(query_string, sent_to_remote, false);
                }
            } else {
                if (IS_SINGLE_NODE)
                    PreventTransactionChain(is_top_level, "CREATE DATABASE");
                createdb((CreatedbStmt*)parse_tree);
            }
#else
        PreventTransactionChain(is_top_level, "CREATE DATABASE");
        createdb((CreatedbStmt*)parse_tree);
#endif
            break;

        case T_AlterDatabaseStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    /*
                     * If this is not a SET TABLESPACE statement, just propogate the
                     * cmd as usual.
                     */
                    if (!IsSetTableSpace((AlterDatabaseStmt*)parse_tree))
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    else
                        exec_utility_with_message_parallel_ddl_mode(
                            query_string, sent_to_remote, false, first_exec_node, EXEC_ON_COORDS);

                    AlterDatabase((AlterDatabaseStmt*)parse_tree, is_top_level);
                    if (!IsSetTableSpace((AlterDatabaseStmt*)parse_tree))
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                    else
                        exec_utility_with_message_parallel_ddl_mode(
                            query_string, sent_to_remote, false, first_exec_node, EXEC_ON_DATANODES);
                } else {
                    AlterDatabase((AlterDatabaseStmt*)parse_tree, is_top_level);
                    /*
                     * If this is not a SET TABLESPACE statement, just propogate the
                     * cmd as usual.
                     */
                    if (!IsSetTableSpace((AlterDatabaseStmt*)parse_tree))
                        ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                    else
                        ExecUtilityWithMessage(query_string, sent_to_remote, false);
                }
            } else {
                AlterDatabase((AlterDatabaseStmt*)parse_tree, is_top_level);
            }
#else
        AlterDatabase((AlterDatabaseStmt*)parse_tree, is_top_level);
#endif
            break;
        case T_AlterDatabaseSetStmt:
            ExecAlterDatabaseSetStmt(parse_tree, query_string, sent_to_remote);
            break;

        case T_DropdbStmt: {
            DropdbStmt* stmt = (DropdbStmt*)parse_tree;
#ifdef PGXC
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                /* clean all connections with dbname on all CNs before db operations */
                PreCleanAndCheckConns(stmt->dbname, stmt->missing_ok);
                /* Allow this to be run inside transaction block on remote nodes */
                PreventTransactionChain(is_top_level, "DROP DATABASE");
            }
#else
            /* Disallow dropping db to be run inside transaction block on single node */
            PreventTransactionChain(is_top_level, "DROP DATABASE");
#endif
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    dropdb(stmt->dbname, stmt->missing_ok);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    dropdb(stmt->dbname, stmt->missing_ok);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                dropdb(stmt->dbname, stmt->missing_ok);
            }
#else
        PreventTransactionChain(is_top_level, "DROP DATABASE");
        dropdb(stmt->dbname, stmt->missing_ok);
#endif
        } break;

            /* Query-level asynchronous notification */
        case T_NotifyStmt:
#ifdef PGXC
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("NOFITY statement is not yet supported.")));
#endif /* PGXC */
            {
                NotifyStmt* stmt = (NotifyStmt*)parse_tree;

                PreventCommandDuringRecovery("NOTIFY");
                Async_Notify(stmt->conditionname, stmt->payload);
            }
            break;

        case T_ListenStmt:
#ifdef PGXC
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("LISTEN statement is not yet supported.")));
#endif /* PGXC */
            {
                ListenStmt* stmt = (ListenStmt*)parse_tree;

                PreventCommandDuringRecovery("LISTEN");
                CheckRestrictedOperation("LISTEN");
                Async_Listen(stmt->conditionname);
            }
            break;

        case T_UnlistenStmt:
#ifdef PGXC
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("UNLISTEN statement is not yet supported.")));
#endif /* PGXC */
            {
                UnlistenStmt* stmt = (UnlistenStmt*)parse_tree;

                PreventCommandDuringRecovery("UNLISTEN");
                CheckRestrictedOperation("UNLISTEN");
                if (stmt->conditionname)
                    Async_Unlisten(stmt->conditionname);
                else
                    Async_UnlistenAll();
            }
            break;

        case T_LoadStmt:
        {
            LoadStmt* stmt = (LoadStmt*)parse_tree;
            if (stmt->is_load_data) {
                if (IS_PGXC_COORDINATOR) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("LOAD DATA statement is not yet supported.")));
                }
                TransformLoadDataToCopy(stmt);
                break;
            }

#ifdef PGXC
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("LOAD statement is not yet supported.")));
#endif /* PGXC */

                closeAllVfds(); /* probably not necessary... */
                /* Allowed names are restricted if you're not superuser */
                load_file(stmt->filename, !superuser());
            }
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false);
#endif
            break;

        case T_ClusterStmt:
            /* we choose to allow this during "read only" transactions */
            PreventCommandDuringRecovery("CLUSTER");
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                ExecNodes* exec_nodes = NULL;
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                bool is_temp = false;
                RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;
                ClusterStmt* cstmt = (ClusterStmt*)parse_tree;

                if (cstmt->relation) {
                    Oid rel_id = RangeVarGetRelid(cstmt->relation, NoLock, true);
                    (void)ExecUtilityFindNodes(OBJECT_TABLE, rel_id, &is_temp);
                    exec_type = CHOOSE_EXEC_NODES(is_temp);
                } else if (in_logic_cluster()) {
                    /*
                     * In logic cluster mode, superuser and system DBA can execute CLUSTER
                     * on all nodes; logic cluster users can execute CLUSTER on its node group;
                     * other users can't execute CLUSTER in DNs, only CLUSTER one table.
                     */
                    Oid group_oid = get_current_lcgroup_oid();
                    if (OidIsValid(group_oid)) {
                        exec_nodes = GetNodeGroupExecNodes(group_oid);
                    } else if (!superuser()) {
                        exec_type = EXEC_ON_NONE;
                        ereport(NOTICE,
                            (errmsg("CLUSTER do not run in DNs because User \"%s\" don't "
                                    "attach to any logic cluster.",
                                GetUserNameFromId(GetUserId()))));
                    }
                }
                query_string = ConstructMesageWithMemInfo(query_string, cstmt->memUsage);
                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (!is_temp) {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                            NULL,
                            sent_to_remote,
                            true,
                            EXEC_ON_COORDS,
                            false,
                            first_exec_node,
                            (Node*)parse_tree);
                    }
                }

                cluster((ClusterStmt*)parse_tree, is_top_level);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        exec_nodes,
                        sent_to_remote,
                        true,
                        EXEC_ON_DATANODES,
                        false,
                        first_exec_node,
                        (Node*)parse_tree);
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, exec_nodes, sent_to_remote, true, exec_type, false, (Node*)parse_tree);
                }
                FreeExecNodes(&exec_nodes);
            } else {
                cluster((ClusterStmt*)parse_tree, is_top_level);
            }
#else
        cluster((ClusterStmt*)parse_tree, is_top_level);
#endif
            break;

        case T_VacuumStmt: {
            bool tmp_enable_autoanalyze = u_sess->attr.attr_sql.enable_autoanalyze;
            VacuumStmt* stmt = (VacuumStmt*)parse_tree;
            stmt->dest = dest;

            // for vacuum lazy, no need do IO collection and IO scheduler
            if (ENABLE_WORKLOAD_CONTROL && !(stmt->options & VACOPT_FULL) && u_sess->wlm_cxt->wlm_params.iotrack == 0)
                WLMCleanIOHashTable();

            /*
             * "vacuum full compact" means "vacuum full" in fact
             * for dfs table if VACOPT_COMPACT is enabled
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                if (stmt->options & VACOPT_COMPACT)
                    stmt->options |= VACOPT_FULL;
            }

            /* @hdfs
             * isForeignTableAnalyze will be set to true when we need to 
             * analyze a foreign table/foreign tables
             */
            bool isForeignTableAnalyze = IsHDFSForeignTableAnalyzable(stmt);

#ifdef ENABLE_MOT
            if (stmt->isMOTForeignTable && (stmt->options & VACOPT_VACUUM)) {
                stmt->options |= VACOPT_FULL;
            }
#endif

            /*
             * @hdfs
             * Log in data node and run analyze foreign table/tables command is illegal.
             * We need to do scheduling to run analyze foreign table/tables command which
             * can only be done on coordinator node.
             */
            if (IS_PGXC_DATANODE && isForeignTableAnalyze && !IsConnFromCoord()) {
                ereport(WARNING,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Running analyze on table/tables reside in HDFS directly from data node is not "
                               "supported.")));
                break;
            }

            /*
             * @hdfs
             * On data node, we got hybridmesage includeing data node No.
             * We judge if analyze work belongs to us by PGXCNodeId. If not, we break out.
             */
            if (IS_PGXC_DATANODE && IsConnFromCoord() && isForeignTableAnalyze &&
                (u_sess->pgxc_cxt.PGXCNodeId != (int)stmt->nodeNo)) {
                break;
            }

            if (stmt->isPgFdwForeignTables && stmt->va_cols) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("This relation doesn't support analyze with column.")));
            }

            /* @hdfs Process MPP Local table "vacuum" "analyze" command. */
            /* forbid auto-analyze inside vacuum/analyze */
            u_sess->attr.attr_sql.enable_autoanalyze = false;
            pgstat_set_io_state(IOSTATE_VACUUM);

            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                /* init the special nodeId identify which receive analyze command from client */
                stmt->orgCnNodeNo = ~0;
            }

            DoVacuumMppTable(stmt, query_string, is_top_level, sent_to_remote);
            u_sess->attr.attr_sql.enable_autoanalyze = tmp_enable_autoanalyze;
        } break;

        case T_ExplainStmt:
            /*
             * To pass all the llt, do not generate parallel plan
             * if we need to explain the query plan.
             */
            if (u_sess->opt_cxt.parallel_debug_mode == LLT_MODE) {
                u_sess->opt_cxt.query_dop = 1;
                u_sess->opt_cxt.skew_strategy_opt = SKEW_OPT_OFF;
            }
            u_sess->attr.attr_sql.under_explain = true;
            /* Set PTFastQueryShippingStore value. */
            PTFastQueryShippingStore = u_sess->attr.attr_sql.enable_fast_query_shipping;
            PG_TRY();
            {
                if (completion_tag != NULL) {
                    ExplainQuery((ExplainStmt*)parse_tree, query_string, params, dest, completion_tag);
                }
            }
            PG_CATCH();
            {
                u_sess->attr.attr_sql.under_explain = false;
                PG_RE_THROW();
            }
            PG_END_TRY();
            u_sess->attr.attr_sql.under_explain = false;
            /* Reset u_sess->opt_cxt.query_dop. */
            if (u_sess->opt_cxt.parallel_debug_mode == LLT_MODE) {
                u_sess->opt_cxt.query_dop = u_sess->opt_cxt.query_dop_store;
                u_sess->opt_cxt.skew_strategy_opt = u_sess->attr.attr_sql.skew_strategy_store;
            }

            /* Rest PTFastQueryShippingStore. */
            PTFastQueryShippingStore = true;
            break;

        case T_CreateTableAsStmt: {
            CreateTableAsStmt *stmt = (CreateTableAsStmt*)parse_tree;

            /* ExecCreateMatInc */
            if (stmt->into->ivm) {
                ExecCreateMatViewInc((CreateTableAsStmt*)parse_tree, query_string, params);
            } else {
                ExecCreateTableAs((CreateTableAsStmt*)parse_tree, query_string, params, completion_tag);
            }
        } break;

        case T_RefreshMatViewStmt: {
            RefreshMatViewStmt *stmt = (RefreshMatViewStmt *)parse_tree;
 
#ifdef ENABLE_MULTIPLE_NODES
            Query *query = NULL;
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                Relation matview = NULL;

                PushActiveSnapshot(GetTransactionSnapshot());

                /* if current node are not fist node, then need to send to first node first */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        NULL,
                        sent_to_remote,
                        false,
                        EXEC_ON_COORDS,
                        false,
                        first_exec_node);

                    matview = heap_openrv(stmt->relation, ExclusiveLock);
                    query = get_matview_query(matview);
                    acquire_mativew_tables_lock(query, stmt->incremental);
                } else {
                    matview = heap_openrv(stmt->relation, ExclusiveLock);
                    query = get_matview_query(matview);
                    acquire_mativew_tables_lock(query, stmt->incremental);

                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
                }

                /* once all coordinators acquire lock then send to datanode */
                CheckRefreshMatview(matview, is_incremental_matview(matview->rd_id));
                ExecNodes *exec_nodes = getRelationExecNodes(matview->rd_id);

                ExecUtilityStmtOnNodes(query_string,
                    exec_nodes,
                    sent_to_remote,
                    false,
                    EXEC_ON_DATANODES,
                    false);
 
                 /* Pop active snapshow */
                if (ActiveSnapshotSet()) {
                    PopActiveSnapshot();
                }

                heap_close(matview, NoLock);
            } else if (IS_PGXC_COORDINATOR) {
                /* attach lock on matview and its table */
                Relation matview = heap_openrv(stmt->relation, ExclusiveLock);
                query = get_matview_query(matview);
                acquire_mativew_tables_lock(query, stmt->incremental);
                heap_close(matview, NoLock);
            }

            /* something happen on datanodes */
            if (IS_PGXC_DATANODE)
#else
            Relation matview = heap_openrv(stmt->relation, stmt->incremental ? ExclusiveLock : AccessExclusiveLock);
            CheckRefreshMatview(matview, is_incremental_matview(matview->rd_id));
            heap_close(matview, NoLock);
#endif
            {
                if (stmt->incremental) {
                    ExecRefreshMatViewInc(stmt, query_string, params, completion_tag);
                }
                if (!stmt->incremental) {
                    ExecRefreshMatView(stmt, query_string, params, completion_tag);
                }
            }
        } break;
		
#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt:
            /*
             * 1.AlterSystemSet don't care whether the node is PRIMARY or STANDBY as same as gs_guc.
             * 2.AlterSystemSet don't care whether the database is read-only, as same as gs_guc.
             * 3.It cannot be executed in a transaction because it is not rollbackable.
             */
            PreventTransactionChain(is_top_level, "ALTER SYSTEM SET");

            AlterSystemSetConfigFile((AlterSystemStmt*)parse_tree);
            break;
#endif

        case T_VariableSetStmt:
            ExecSetVariableStmt((VariableSetStmt*)parse_tree);
#ifdef PGXC
            /* Let the pooler manage the statement */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                VariableSetStmt* stmt = (VariableSetStmt*)parse_tree;
                char* mask_string = NULL;

                mask_string = maskPassword(query_string);
                if (mask_string == NULL)
                    mask_string = (char*)query_string;

                // do not send the variable which in blacklist  to other nodes
                if (IsVariableinBlackList(stmt->name)) {
                    break;
                }

                if (u_sess->catalog_cxt.overrideStack && (!pg_strcasecmp(stmt->name, SEARCH_PATH_GUC_NAME) ||
                                                             !pg_strcasecmp(stmt->name, CURRENT_SCHEMA_GUC_NAME))) {
                    /*
                     * set search_path or current_schema inside stored procedure is invalid,
                     * do not send to other nodes.
                     */
                    OverrideStackEntry* entry = NULL;
                    entry = (OverrideStackEntry*)linitial(u_sess->catalog_cxt.overrideStack);
                    if (entry->inProcedure)
                        break;
                }

                /*
                 * If command is local and we are not in a transaction block do NOT
                 * send this query to backend nodes, it is just bypassed by the backend.
                 */
                if (stmt->is_local) {
                    if (IsTransactionBlock()) {
                        if (PoolManagerSetCommand(POOL_CMD_LOCAL_SET, mask_string, stmt->name) < 0) {
                            /* Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query". */
                            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("openGauss: ERROR SET query")));
                        }
                    }
                } else {
                    /* when set command in function, treat it as in transaction. */
                    if (!IsTransactionBlock() && !(dest->mydest == DestSPI)) {
                        if (PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, mask_string, stmt->name) < 0) {
                            /* Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query". */
                            ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("openGauss: ERROR SET query")));
                        }
                    } else {
                        ExecUtilityStmtOnNodes(mask_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);

                        // Append set command to input_set_message, this will be sent to pool when this Transaction
                        // commit.
                        int ret = check_set_message_to_send(stmt, query_string);

                        if (ret != -1) {
                            if (ret == 0)
                                append_set_message(query_string);
                            else
                                make_set_message(); /* make new message string */
                        }
                    }
                }
                if (mask_string != query_string)
                    pfree(mask_string);
            }
#endif
            break;

        case T_VariableShowStmt: {
            VariableShowStmt* n = (VariableShowStmt*)parse_tree;

            GetPGVariable(n->name, n->likename, dest);
        } break;
        case T_ShutdownStmt: {
            ShutdownStmt* n = (ShutdownStmt*)parse_tree;

            DoShutdown(n);
        } break;

        case T_DiscardStmt:
#ifdef PGXC
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("DISCARD statement is not yet supported.")));
#endif /* PGXC */
            /* should we allow DISCARD PLANS? */
            CheckRestrictedOperation("DISCARD");
            DiscardCommand((DiscardStmt*)parse_tree, is_top_level);
#ifdef PGXC
            /*
             * Discard objects for all the sessions possible.
             * For example, temporary tables are created on all Datanodes
             * and Coordinators.
             */
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string,
                    NULL,
                    sent_to_remote,
                    true,
                    CHOOSE_EXEC_NODES(((DiscardStmt*)parse_tree)->target == DISCARD_TEMP),
                    false);
#endif
            break;

        case T_CreateTrigStmt:
            (void)CreateTrigger(
                (CreateTrigStmt*)parse_tree, query_string, InvalidOid, InvalidOid, InvalidOid, InvalidOid, false);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                CreateTrigStmt* stmt = (CreateTrigStmt*)parse_tree;
                RemoteQueryExecType exec_type;
                bool is_temp = false;
                Oid rel_id = RangeVarGetRelidExtended(stmt->relation, NoLock, false, false, false, true, NULL, NULL);

                exec_type = ExecUtilityFindNodes(OBJECT_TABLE, rel_id, &is_temp);

                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, exec_type, is_temp, (Node*)stmt);
            }
#endif
            break;

        case T_CreatePLangStmt:
            if (!IsInitdb)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("new language is not yet supported.")));
            CreateProceduralLanguage((CreatePLangStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

            /*
             * ******************************** DOMAIN statements ****
             */
        case T_CreateDomainStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (!IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("domain is not yet supported.")));
#endif
            DefineDomain((CreateDomainStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

            /*
             * ******************************** ROLE statements ****
             */
        case T_CreateRoleStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    CreateRole((CreateRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    CreateRole((CreateRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                CreateRole((CreateRoleStmt*)parse_tree);
            }
#else
        CreateRole((CreateRoleStmt*)parse_tree);
#endif
            break;

        case T_AlterRoleStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterRole((AlterRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterRole((AlterRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterRole((AlterRoleStmt*)parse_tree);
            }
#else
        AlterRole((AlterRoleStmt*)parse_tree);
#endif
            break;
        case T_AlterRoleSetStmt:
            ExecAlterRoleSetStmt(parse_tree, query_string, sent_to_remote);
            break;

        case T_DropRoleStmt:
#ifdef PGXC
            /* Clean connections before dropping a user on local node */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ListCell* item = NULL;
                foreach (item, ((DropRoleStmt*)parse_tree)->roles) {
                    const char* role = strVal(lfirst(item));
                    DropRoleStmt* stmt = (DropRoleStmt*)parse_tree;

                    /* clean all connections with role on all CNs */
                    PreCleanAndCheckUserConns(role, stmt->missing_ok);
                }
            }

            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropRole((DropRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropRole((DropRoleStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropRole((DropRoleStmt*)parse_tree);
            }
#else
        DropRole((DropRoleStmt*)parse_tree);
#endif
            break;

        case T_DropOwnedStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropOwnedObjects((DropOwnedStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropOwnedObjects((DropOwnedStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropOwnedObjects((DropOwnedStmt*)parse_tree);
            }
#else
        DropOwnedObjects((DropOwnedStmt*)parse_tree);
#endif
            break;

        case T_ReassignOwnedStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    ReassignOwnedObjects((ReassignOwnedStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    ReassignOwnedObjects((ReassignOwnedStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                ReassignOwnedObjects((ReassignOwnedStmt*)parse_tree);
            }
#else
        ReassignOwnedObjects((ReassignOwnedStmt*)parse_tree);
#endif
            break;

        case T_LockStmt:

            /*
             * Since the lock would just get dropped immediately, LOCK TABLE
             * outside a transaction block is presumed to be user error.
             */
            RequireTransactionChain(is_top_level, "LOCK TABLE");
#ifdef PGXC
            /* only lock local table if cm_agent do Lock Stmt */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                !(u_sess->attr.attr_common.xc_maintenance_mode &&
                    (strcmp(u_sess->attr.attr_common.application_name, "cm_agent") == 0))) {
                ListCell* cell = NULL;
                bool has_nontemp = false;
                bool has_temp = false;
                bool is_temp = false;
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                foreach (cell, ((LockStmt*)parse_tree)->relations) {
                    RangeVar* r = (RangeVar*)lfirst(cell);
                    Oid rel_id = RangeVarGetRelid(r, NoLock, true);
                    (void)ExecUtilityFindNodes(OBJECT_TABLE, rel_id, &is_temp);
                    has_temp |= is_temp;
                    has_nontemp |= !is_temp;

                    if (has_temp && has_nontemp)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("LOCK not supported for TEMP and non-TEMP objects together"),
                                errdetail("You should separate TEMP and non-TEMP objects")));
                }

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    RemoteQuery* step = makeNode(RemoteQuery);
                    step->combine_type = COMBINE_TYPE_SAME;
                    step->sql_statement = (char*)query_string;
                    step->exec_type = has_temp ? EXEC_ON_NONE : EXEC_ON_COORDS;
                    step->exec_nodes = NULL;
                    step->is_temp = has_temp;
                    ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
                    pfree_ext(step);
                }

                LockTableCommand((LockStmt*)parse_tree);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        NULL,
                        sent_to_remote,
                        false,
                        EXEC_ON_DATANODES,
                        false,
                        first_exec_node,
                        (Node*)parse_tree);
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, NULL, sent_to_remote, false, CHOOSE_EXEC_NODES(has_temp), false, (Node*)parse_tree);
                }
            } else {
                LockTableCommand((LockStmt*)parse_tree);
            }
#else
        LockTableCommand((LockStmt*)parse_tree);
#endif
            break;

        case T_ConstraintsSetStmt:
            AfterTriggerSetState((ConstraintsSetStmt*)parse_tree);
#ifdef PGXC
            /*
             * Let the pooler manage the statement, SET CONSTRAINT can just be used
             * inside a transaction block, hence it has no effect outside that, so use
             * it as a local one.
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsTransactionBlock()) {
                const int MAX_PARAM_LEN = 1024;
                char tmpName[MAX_PARAM_LEN + 1] = {0};
                char *guc_name = GetGucName(query_string, tmpName);
                if (PoolManagerSetCommand(POOL_CMD_LOCAL_SET, query_string, guc_name) < 0) {
                    /* Add retry query error code ERRCODE_SET_QUERY for error "ERROR SET query". */
                    ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("openGauss: ERROR SET query")));
                }
            }
#endif
            break;

        case T_CheckPointStmt:
            if (!(superuser() || isOperatoradmin(GetUserId())))
                ereport(
                    ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be system admin or operator admin to do CHECKPOINT")));
            /*
             * You might think we should have a PreventCommandDuringRecovery()
             * here, but we interpret a CHECKPOINT command during recovery as
             * a request for a restartpoint instead. We allow this since it
             * can be a useful way of reducing switchover time when using
             * various forms of replication.
             */
            RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT | (RecoveryInProgress() ? 0 : CHECKPOINT_FORCE));
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, true, EXEC_ON_DATANODES, false);
#endif
            break;

#ifdef PGXC
        case T_BarrierStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            RequestBarrier(((BarrierStmt*)parse_tree)->id, completion_tag);
            break;

        /*
         * Node DDL is an operation local to Coordinator.
         * In case of a new node being created in the cluster,
         * it is necessary to create this node on all the Coordinators independently.
         */
        case T_AlterNodeStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            PgxcNodeAlter((AlterNodeStmt*)parse_tree);
            break;

        case T_CreateNodeStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            PgxcNodeCreate((CreateNodeStmt*)parse_tree);
            break;

        case T_AlterCoordinatorStmt: {
            AlterCoordinatorStmt* stmt = (AlterCoordinatorStmt*)parse_tree;
            if (IS_PGXC_COORDINATOR && IsConnFromCoord())
                PgxcCoordinatorAlter((AlterCoordinatorStmt*)parse_tree);
            const char* coor_name = stmt->node_name;
            Oid noid = get_pgxc_nodeoid(coor_name);
            char node_type = get_pgxc_nodetype(noid);
            List* cn_list = NIL;

            if (node_type != 'C')
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PGXC Node %s is not a valid coordinator", coor_name)));

            /* alter coordinator node should run on CN nodes only */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && node_type == PGXC_NODE_COORDINATOR) {
                /* exec cn record stmt method */
                t_thrd.xact_cxt.AlterCoordinatorStmt = true;
                /* generate cn list */
                ListCell* lc = NULL;
                foreach (lc, stmt->coor_nodes) {
                    char* lc_name = strVal(lfirst(lc));
                    int lc_idx = PgxcGetNodeIndex(lc_name);

                    /* only support cn in with clause */
                    if (lc_idx == -1)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid value \"%s\" in WITH clause", lc_name)));
                    /* except myself */
                    if (u_sess->pgxc_cxt.PGXCNodeId - 1 != lc_idx)
                        cn_list = lappend_int(cn_list, lc_idx);
                    else
                        PgxcCoordinatorAlter((AlterCoordinatorStmt*)parse_tree);
                }

                /* need to alter on remote CN(s) */
                if (cn_list != NIL) {
                    ExecNodes* nodes = makeNode(ExecNodes);
                    nodes->nodeList = cn_list;
                    ExecUtilityStmtOnNodes(query_string, nodes, sent_to_remote, false, EXEC_ON_COORDS, false);
                    FreeExecNodes(&nodes);
                }
            }
        } break;

        case T_DropNodeStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            {
                DropNodeStmt* stmt = (DropNodeStmt*)parse_tree;
                char node_type = PgxcNodeRemove(stmt);

                /* drop node should run on CN nodes only */
                if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && node_type != PGXC_NODE_NONE &&
                    (!g_instance.attr.attr_storage.IsRoachStandbyCluster)) {
                    List* cn_list = NIL;
                    int cn_num = 0;
                    int cn_delete_idx = -2;

                    /* for drop node cn */
                    if (node_type == PGXC_NODE_COORDINATOR) {
                        /* get the node index of the dropped cn */
                        const char* node_name = stmt->node_name;
                        cn_delete_idx = PgxcGetNodeIndex(node_name);

                        /* special case: node is created just in this session */
                        if (cn_delete_idx == -1) {
                            elog(DEBUG2, "Drop Node %s: get node index -1", node_name);
                        }
                    }

                    /* generate cn list */
                    if (stmt->remote_nodes) {
                        ListCell* lc = NULL;
                        foreach (lc, stmt->remote_nodes) {
                            char* lc_name = strVal(lfirst(lc));
                            int lc_idx = PgxcGetNodeIndex(lc_name);

                            /* only support cn in with clause */
                            if (lc_idx == -1 || lc_idx == cn_delete_idx)
                                ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmsg("Invalid value \"%s\" in WITH clause", lc_name)));

                            /* except myself */
                            if (u_sess->pgxc_cxt.PGXCNodeId - 1 != lc_idx)
                                cn_list = lappend_int(cn_list, lc_idx);
                        }
                    } else {
                        cn_list = GetAllCoordNodes();
                        cn_num = list_length(cn_list);
                        /* remove the dropped cn from nodelist */
                        if (cn_num > 0 && cn_delete_idx > -1) {
                            cn_list = list_delete_int(cn_list, cn_delete_idx);
                            Assert(cn_num - 1 == list_length(cn_list));
                        }
                    }

                    cn_num = list_length(cn_list);
                    /* need to drop on remote CN(s) */
                    if (cn_num > 0) {
                        ExecNodes* nodes = makeNode(ExecNodes);
                        nodes->nodeList = cn_list;
                        ExecUtilityStmtOnNodes(query_string, nodes, sent_to_remote, false, EXEC_ON_COORDS, false);
                        FreeExecNodes(&nodes);
                    } else if (cn_list != NULL) {
                        list_free_ext(cn_list);
                    }
                }
            }
            break;

        case T_CreateGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (IS_SINGLE_NODE)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Don't support node group in single_node mode.")));

            PgxcGroupCreate((CreateGroupStmt*)parse_tree);

            /* create node group should run on CN nodes only */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
            }
            break;

        case T_AlterGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (IS_SINGLE_NODE)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Don't support node group in single_node mode.")));

#ifdef ENABLE_MULTIPLE_NODES
            PgxcGroupAlter((AlterGroupStmt*)parse_tree);
            /* alter node group should run on CN nodes only */
            {
                AlterGroupType alter_type = ((AlterGroupStmt*)parse_tree)->alter_type;
                if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                    (alter_type != AG_SET_DEFAULT && alter_type != AG_SET_SEQ_ALLNODES &&
                        alter_type != AG_SET_SEQ_SELFNODES)) {
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
                }
            }
#endif
            break;

        case T_DropGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (IS_SINGLE_NODE)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Don't support node group in single_node mode.")));

            PgxcGroupRemove((DropGroupStmt*)parse_tree);

            /* drop node group should run on CN nodes only */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
            }
            break;

        case T_CreatePolicyLabelStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote,
                                                           false, EXEC_ON_COORDS, false, FirstExecNode);
                    create_policy_label((CreatePolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote,
                                                           false, EXEC_ON_DATANODES, false, FirstExecNode);
                } else {
                    create_policy_label((CreatePolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                create_policy_label((CreatePolicyLabelStmt *) parse_tree);
            }
#else
            create_policy_label((CreatePolicyLabelStmt *) parse_tree);
#endif
            break;

        case T_AlterPolicyLabelStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote,
                                                           false, EXEC_ON_COORDS, false, FirstExecNode);
                    alter_policy_label((AlterPolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote,
                                                           false, EXEC_ON_DATANODES, false, FirstExecNode);
                } else {
                    alter_policy_label((AlterPolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                alter_policy_label((AlterPolicyLabelStmt *) parse_tree);
            }
#else
            alter_policy_label((AlterPolicyLabelStmt *) parse_tree);
#endif
            break;

        case T_DropPolicyLabelStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    drop_policy_label((DropPolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES,
                        false, FirstExecNode);
                } else {
                    drop_policy_label((DropPolicyLabelStmt *) parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                drop_policy_label((DropPolicyLabelStmt *) parse_tree);
            }
#else
            drop_policy_label((DropPolicyLabelStmt *) parse_tree);
#endif
            break;

        case T_CreateAuditPolicyStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    create_audit_policy((CreateAuditPolicyStmt *)parse_tree);
                } else {
                    create_audit_policy((CreateAuditPolicyStmt *)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
                }
            } 
            if (IS_SINGLE_NODE) {
                create_audit_policy((CreateAuditPolicyStmt *) parse_tree);
            }
#else
            create_audit_policy((CreateAuditPolicyStmt *) parse_tree);
#endif
            break;

        case T_AlterAuditPolicyStmt:
#ifdef PGXC
           if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    alter_audit_policy((AlterAuditPolicyStmt *)parse_tree);
                } else {
                    alter_audit_policy((AlterAuditPolicyStmt *) parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
                }
            }
            if (IS_SINGLE_NODE) {
                alter_audit_policy((AlterAuditPolicyStmt *) parse_tree);
            }
#else
            alter_audit_policy((AlterAuditPolicyStmt *) parse_tree);
#endif

            break;

        case T_DropAuditPolicyStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    drop_audit_policy((DropAuditPolicyStmt *) parse_tree);
                } else {
                    drop_audit_policy((DropAuditPolicyStmt *) parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false);
                }
            } 
            if (IS_SINGLE_NODE) {
                drop_audit_policy((DropAuditPolicyStmt *) parse_tree);
            }
#else
            drop_audit_policy((DropAuditPolicyStmt *) parse_tree);
#endif
            break;

        case T_CreateMaskingPolicyStmt:
#ifdef PGXC
           if (IS_PGXC_COORDINATOR)
           {
               char *FirstExecNode = find_first_exec_cn();
               bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

			   if(u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode)
               {
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, FirstExecNode);
                   create_masking_policy((CreateMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, FirstExecNode);
               } else {
                   create_masking_policy((CreateMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
               }
           } else {
               create_masking_policy((CreateMaskingPolicyStmt *) parse_tree);
           }
#else
           create_masking_policy((CreateMaskingPolicyStmt *) parse_tree);
#endif
            break;

        case T_AlterMaskingPolicyStmt:
#ifdef PGXC
           if (IS_PGXC_COORDINATOR)
           {
               char *FirstExecNode = find_first_exec_cn();
               bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

			   if(u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode)
               {
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, FirstExecNode);
                   alter_masking_policy((AlterMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, FirstExecNode);
               } else {
                   alter_masking_policy((AlterMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
               }
           } else {
               alter_masking_policy((AlterMaskingPolicyStmt *) parse_tree);
           }
#else
           alter_masking_policy((AlterMaskingPolicyStmt *) parse_tree);
#endif

            break;

        case T_DropMaskingPolicyStmt:
#ifdef PGXC
           if (IS_PGXC_COORDINATOR)
           {
               char *FirstExecNode = find_first_exec_cn();
               bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

			   if(u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode)
               {
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, FirstExecNode);
                   drop_masking_policy((DropMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, FirstExecNode);
               } else {
                   drop_masking_policy((DropMaskingPolicyStmt *) parse_tree);
                   ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
               }
           } else {
               drop_masking_policy((DropMaskingPolicyStmt *) parse_tree);
           }
#else
           drop_masking_policy((DropMaskingPolicyStmt *) parse_tree);
#endif
            break;

        case T_CreateSynonymStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    CreateSynonym((CreateSynonymStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    CreateSynonym((CreateSynonymStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                CreateSynonym((CreateSynonymStmt*)parse_tree);
            }
#else
            CreateSynonym((CreateSynonymStmt*)parse_tree);
#endif
            break;

        case T_DropSynonymStmt:
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropSynonym((DropSynonymStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropSynonym((DropSynonymStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropSynonym((DropSynonymStmt*)parse_tree);
            }
#else
            DropSynonym((DropSynonymStmt*)parse_tree);
#endif
            break;

        case T_CreateResourcePoolStmt:
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    if (in_logic_cluster() && get_current_lcgroup_name()) {
                        char* create_respool_stmt = NULL;
                        create_respool_stmt = GenerateResourcePoolStmt((CreateResourcePoolStmt*)parse_tree, query_string);
                        Assert(create_respool_stmt != NULL);

                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            create_respool_stmt, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);

                        pfree_ext(create_respool_stmt);
                    } else {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    }

                    CreateResourcePool((CreateResourcePoolStmt*)parse_tree);

                    if (in_logic_cluster() && get_current_lcgroup_name()) {
                        char* create_respool_stmt = NULL;
                        create_respool_stmt = GenerateResourcePoolStmt((CreateResourcePoolStmt*)parse_tree, query_string);
                        Assert(create_respool_stmt != NULL);

                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            create_respool_stmt, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);

                        pfree_ext(create_respool_stmt);
                    } else {
                        ExecUtilityStmtOnNodes_ParallelDDLMode(
                            query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                    }
                } else {
                    CreateResourcePool((CreateResourcePoolStmt*)parse_tree);

                    if (in_logic_cluster() && get_current_lcgroup_name()) {
                        char* create_respool_stmt = NULL;
                        create_respool_stmt = GenerateResourcePoolStmt((CreateResourcePoolStmt*)parse_tree, query_string);
                        Assert(create_respool_stmt != NULL);

                        ExecUtilityStmtOnNodes(
                            create_respool_stmt, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);

                        pfree_ext(create_respool_stmt);
                    } else {
                        ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                    }
                }
            } else {
                CreateResourcePool((CreateResourcePoolStmt*)parse_tree);
            }
            break;

        case T_AlterResourcePoolStmt:
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterResourcePool((AlterResourcePoolStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterResourcePool((AlterResourcePoolStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterResourcePool((AlterResourcePoolStmt*)parse_tree);
            }
            break;

        case T_DropResourcePoolStmt:
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropResourcePool((DropResourcePoolStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropResourcePool((DropResourcePoolStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropResourcePool((DropResourcePoolStmt*)parse_tree);
            }
            break;

        case T_AlterGlobalConfigStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterGlobalConfig((AlterGlobalConfigStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterGlobalConfig((AlterGlobalConfigStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterGlobalConfig((AlterGlobalConfigStmt*)parse_tree);
            }
#else
            AlterGlobalConfig((AlterGlobalConfigStmt*)parse_tree);
#endif
            break;

        case T_DropGlobalConfigStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropGlobalConfig((DropGlobalConfigStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropGlobalConfig((DropGlobalConfigStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropGlobalConfig((DropGlobalConfigStmt*)parse_tree);
            }
#else
            DropGlobalConfig((DropGlobalConfigStmt*)parse_tree);
#endif
            break;

        case T_CreateWorkloadGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "CREATE WORKLOAD GROUP");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    CreateWorkloadGroup((CreateWorkloadGroupStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    CreateWorkloadGroup((CreateWorkloadGroupStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                CreateWorkloadGroup((CreateWorkloadGroupStmt*)parse_tree);
            }
            break;
        case T_AlterWorkloadGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "ALTER WORKLOAD GROUP");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterWorkloadGroup((AlterWorkloadGroupStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterWorkloadGroup((AlterWorkloadGroupStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterWorkloadGroup((AlterWorkloadGroupStmt*)parse_tree);
            }
            break;
        case T_DropWorkloadGroupStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "DROP WORKLOAD GROUP");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropWorkloadGroup((DropWorkloadGroupStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropWorkloadGroup((DropWorkloadGroupStmt*)parse_tree);

                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropWorkloadGroup((DropWorkloadGroupStmt*)parse_tree);
            }
            break;

        case T_CreateAppWorkloadGroupMappingStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "CREATE APP WORKLOAD GROUP MAPPING");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    CreateAppWorkloadGroupMapping((CreateAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    CreateAppWorkloadGroupMapping((CreateAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                CreateAppWorkloadGroupMapping((CreateAppWorkloadGroupMappingStmt*)parse_tree);
            }
            break;

        case T_AlterAppWorkloadGroupMappingStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "ALTER APP WORKLOAD GROUP MAPPING");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    AlterAppWorkloadGroupMapping((AlterAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    AlterAppWorkloadGroupMapping((AlterAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                AlterAppWorkloadGroupMapping((AlterAppWorkloadGroupMappingStmt*)parse_tree);
            }
            break;

        case T_DropAppWorkloadGroupMappingStmt:
#ifndef ENABLE_MULTIPLE_NODES
            DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
            if (!IsConnFromCoord())
                PreventTransactionChain(is_top_level, "DROP APP WORKLOAD GROUP MAPPING");
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropAppWorkloadGroupMapping((DropAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropAppWorkloadGroupMapping((DropAppWorkloadGroupMappingStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                DropAppWorkloadGroupMapping((DropAppWorkloadGroupMappingStmt*)parse_tree);
            }
            break;
#endif

        case T_ReindexStmt: {
            ReindexStmt* stmt = (ReindexStmt*)parse_tree;
            RemoteQueryExecType exec_type;
            bool is_temp = false;
            pgstat_set_io_state(IOSTATE_WRITE);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (stmt->relation) {
                    Oid rel_id = InvalidOid;
                    rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
                    if (OidIsValid(rel_id)) {
                        exec_type = ExecUtilityFindNodes(stmt->kind, rel_id, &is_temp);
                        UnlockRelationOid(rel_id, AccessShareLock);
                    } else
                        exec_type = EXEC_ON_NONE;
                } else
                    exec_type = EXEC_ON_ALL_NODES;

                query_string = ConstructMesageWithMemInfo(query_string, stmt->memUsage);
                /*
                 * If I am the main execute CN but not CCN,
                 * Notify the CCN to create firstly, and then notify other CNs except me.
                 */
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node &&
                    (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS)) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        NULL,
                        sent_to_remote,
                        stmt->kind == OBJECT_DATABASE,
                        EXEC_ON_COORDS,
                        false,
                        first_exec_node,
                        (Node*)stmt);
                }

                ReindexCommand(stmt, is_top_level);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node &&
                    (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string,
                        NULL,
                        sent_to_remote,
                        stmt->kind == OBJECT_DATABASE,
                        EXEC_ON_DATANODES,
                        false,
                        first_exec_node,
                        (Node*)stmt);
                } else {
                    ExecUtilityStmtOnNodes(
                        query_string, NULL, sent_to_remote, stmt->kind == OBJECT_DATABASE, exec_type, false, (Node*)stmt);
                }
            } else {
                ReindexCommand(stmt, is_top_level);
            }
#else
        ReindexCommand(stmt, is_top_level);
#endif
        } break;

        case T_CreateConversionStmt:
#ifdef PGXC
            if (!IsInitdb)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined conversion is not yet supported.")));
#endif /* PGXC */
            CreateConversionCommand((CreateConversionStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateCastStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (!IsInitdb && !u_sess->exec_cxt.extension_is_valid && !u_sess->attr.attr_common.IsInplaceUpgrade)
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined cast is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
            CreateCast((CreateCastStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateOpClassStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if (!u_sess->attr.attr_common.IsInplaceUpgrade && !u_sess->exec_cxt.extension_is_valid)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined operator is not yet supported.")));
#endif /* ENABLE_MULTIPLE_NODES */
            DefineOpClass((CreateOpClassStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_CreateOpFamilyStmt:
#ifdef PGXC
            if (!u_sess->attr.attr_common.IsInplaceUpgrade)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined operator is not yet supported.")));
#endif /* PGXC */
            DefineOpFamily((CreateOpFamilyStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterOpFamilyStmt:
#ifdef PGXC
            if (!u_sess->attr.attr_common.IsInplaceUpgrade)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("user defined operator is not yet supported.")));
#endif /* PGXC */
            AlterOpFamily((AlterOpFamilyStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterTSDictionaryStmt:
            ts_check_feature_disable();
            AlterTSDictionary((AlterTSDictionaryStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;

        case T_AlterTSConfigurationStmt:
            ts_check_feature_disable();
            AlterTSConfiguration((AlterTSConfigurationStmt*)parse_tree);
#ifdef PGXC
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
#endif
            break;
#ifdef PGXC
        case T_RemoteQuery:
            AssertEreport(IS_PGXC_COORDINATOR, MOD_EXECUTOR, "not a coordinator node");
            /*
             * Do not launch query on Other Datanodes if remote connection is a Coordinator one
             * it will cause a deadlock in the cluster at Datanode levels.
             */
#ifdef ENABLE_MULTIPLE_NODES
            if (!IsConnFromCoord())
                ExecRemoteUtility((RemoteQuery*)parse_tree);
#endif
            break;

        case T_CleanConnStmt:
            CleanConnection((CleanConnStmt*)parse_tree);

            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, true, EXEC_ON_COORDS, false);
            break;
#endif
        case T_CreateDirectoryStmt:
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    CreatePgDirectory((CreateDirectoryStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    CreatePgDirectory((CreateDirectoryStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else
                CreatePgDirectory((CreateDirectoryStmt*)parse_tree);
            break;

        case T_DropDirectoryStmt:
            if (IS_PGXC_COORDINATOR) {
                char* first_exec_node = find_first_exec_cn();
                bool is_first_node = (strcmp(first_exec_node, g_instance.attr.attr_common.PGXCNodeName) == 0);
                if (u_sess->attr.attr_sql.enable_parallel_ddl && !is_first_node) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS, false, first_exec_node);
                    DropPgDirectory((DropDirectoryStmt*)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(
                        query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES, false, first_exec_node);
                } else {
                    DropPgDirectory((DropDirectoryStmt*)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else
                DropPgDirectory((DropDirectoryStmt*)parse_tree);
            break;
        /* Client Logic */
        case T_CreateClientLogicGlobal: 
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
            if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("Un-support to create client master key when client encryption is disabled.")));
            }
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    (void)process_global_settings((CreateClientLogicGlobal *)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES,
                        false, FirstExecNode);
                } else {
                    (void)process_global_settings((CreateClientLogicGlobal *)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                (void)process_global_settings((CreateClientLogicGlobal *)parse_tree);
            }
            break;
        case T_CreateClientLogicColumn:
#ifdef ENABLE_MULTIPLE_NODES
            if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
            if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                        errmsg("Un-support to create column encryption key when client encryption is disabled.")));
            }
            if (IS_PGXC_COORDINATOR) {
                char *FirstExecNode = find_first_exec_cn();
                bool isFirstNode = (strcmp(FirstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);

                if (u_sess->attr.attr_sql.enable_parallel_ddl && !isFirstNode) {
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_COORDS,
                        false, FirstExecNode);
                    (void)process_column_settings((CreateClientLogicColumn *)parse_tree);
                    ExecUtilityStmtOnNodes_ParallelDDLMode(query_string, NULL, sent_to_remote, false, EXEC_ON_DATANODES,
                        false, FirstExecNode);
                } else {
                    (void)process_column_settings((CreateClientLogicColumn *)parse_tree);
                    ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, false);
                }
            } else {
                (void)process_column_settings((CreateClientLogicColumn *)parse_tree);
            }
            break;

        case T_CreateModelStmt:{ // DB4AI

            exec_create_model((CreateModelStmt*) parse_tree, query_string, params, completion_tag);

            break;
        }

        case T_CreatePublicationStmt:
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support PUBLICATION yet"),
                    errdetail("The feature is not currently supported")));
#endif
            CreatePublication((CreatePublicationStmt *) parse_tree);
            break;
        case T_AlterPublicationStmt:
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support PUBLICATION yet"),
                    errdetail("The feature is not currently supported")));
#endif
            AlterPublication((AlterPublicationStmt *) parse_tree);
            break;
        case T_CreateSubscriptionStmt:
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support SUBSCRIPTION yet"),
                    errdetail("The feature is not currently supported")));
#endif
            CreateSubscription((CreateSubscriptionStmt *) parse_tree, is_top_level);
            break;
        case T_AlterSubscriptionStmt:
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support SUBSCRIPTION yet"),
                    errdetail("The feature is not currently supported")));
#endif
            AlterSubscription((AlterSubscriptionStmt *) parse_tree);
            break;
        case T_DropSubscriptionStmt:
#if defined(ENABLE_MULTIPLE_NODES) || defined(ENABLE_LITE_MODE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("openGauss does not support SUBSCRIPTION yet"),
                    errdetail("The feature is not currently supported")));
#endif
            DropSubscription((DropSubscriptionStmt *) parse_tree, is_top_level);
            break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)nodeTag(parse_tree))));
        } break;
    }
}

#ifdef PGXC

/*
 * is_stmt_allowed_in_locked_mode
 *
 * Allow/Disallow a utility command while cluster is locked
 * A statement will be disallowed if it makes such changes
 * in catalog that are backed up by pg_dump except
 * CREATE NODE that has to be allowed because
 * a new node has to be created while the cluster is still
 * locked for backup
 */
static bool is_stmt_allowed_in_locked_mode(Node* parse_tree, const char* query_string)
{
#define ALLOW 1
#define DISALLOW 0

    switch (nodeTag(parse_tree)) {
        /* To allow creation of temp tables */
        case T_CreateStmt: /* CREATE TABLE */
        {
            CreateStmt* stmt = (CreateStmt*)parse_tree;

            /* Don't allow temp table in online scenes for metadata sync problem. */
            if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP &&
                !u_sess->attr.attr_sql.enable_online_ddl_waitlock)
                return ALLOW;
            return DISALLOW;
        } break;
        case T_CreateGroupStmt: {
            CreateGroupStmt* stmt = (CreateGroupStmt*)parse_tree;

            /* Enable create node group in logic and physical cluster online expansion. */
            if ((in_logic_cluster() && stmt->src_group_name) ||
                (u_sess->attr.attr_sql.enable_online_ddl_waitlock && superuser()))
                return ALLOW;
            return DISALLOW;
        } break;
        case T_ExecuteStmt:
            /*
             * Prepared statememts can only have
             * SELECT, INSERT, UPDATE, DELETE,
             * or VALUES statement, there is no
             * point stopping EXECUTE.
             */
        case T_CreateNodeStmt:
        case T_AlterNodeStmt:
            /*
             * This has to be allowed so that the new node
             * can be created, while the cluster is still
             * locked for backup
             */
            /* two cases: when a cluster is first deployed;     */
            /* when new nodes are added into cluster            */
            /* for latter, OM has lock DDL and backup           */
        case T_AlterGroupStmt:
        case T_AlterCoordinatorStmt:
        case T_DropNodeStmt:
            /*
             * This has to be allowed so that DROP NODE
             * can be issued to drop a node that has crashed.
             * Otherwise system would try to acquire a shared
             * advisory lock on the crashed node.
             */
        case T_TransactionStmt:
        case T_PlannedStmt:
        case T_ClosePortalStmt:
        case T_FetchStmt:
        case T_CopyStmt:
        case T_PrepareStmt:
            /*
             * Prepared statememts can only have
             * SELECT, INSERT, UPDATE, DELETE,
             * or VALUES statement, there is no
             * point stopping PREPARE.
             */
        case T_DeallocateStmt:
            /*
             * If prepare is allowed the deallocate should
             * be allowed also
             */
        case T_DoStmt:
        case T_NotifyStmt:
        case T_ListenStmt:
        case T_UnlistenStmt:
        case T_LoadStmt:
        case T_ClusterStmt:
        case T_ExplainStmt:
        case T_VariableSetStmt:
        case T_VariableShowStmt:
        case T_DiscardStmt:
        case T_LockStmt:
        case T_ConstraintsSetStmt:
        case T_CheckPointStmt:
        case T_BarrierStmt:
        case T_ReindexStmt:
        case T_RemoteQuery:
        case T_CleanConnStmt:
        case T_CreateFunctionStmt:  // @Temp Table. create function's lock check is moved in CreateFunction
            return ALLOW;

        default:
            return DISALLOW;
    }
    return DISALLOW;
}

/*
 * ExecUtilityWithMessage:
 * Execute the query on remote nodes in a transaction block.
 * If this fails on one of the nodes :
 * Add a context message containing the failed node names.
 * Rethrow the error with the message about the failed nodes.
 * If all are successful, just return.
 */
static void ExecUtilityWithMessage(const char* query_string, bool sent_to_remote, bool is_temp)
{
    PG_TRY();
    {
        ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, false, EXEC_ON_ALL_NODES, is_temp);
    }
    PG_CATCH();
    {

        /*
         * Some nodes failed. Add context about what all nodes the query
         * failed
         */
        ExecNodes* coord_success_nodes = NULL;
        ExecNodes* data_success_nodes = NULL;
        char* msg_failed_nodes = NULL;

        pgxc_all_success_nodes(&data_success_nodes, &coord_success_nodes, &msg_failed_nodes);
        if (msg_failed_nodes != NULL)
            errcontext("%s", msg_failed_nodes);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void exec_utility_with_message_parallel_ddl_mode(
    const char* query_string, bool sent_to_remote, bool is_temp, const char* first_exec_node, RemoteQueryExecType exec_type)
{
    PG_TRY();
    {
        ExecUtilityStmtOnNodes_ParallelDDLMode(
            query_string, NULL, sent_to_remote, false, exec_type, is_temp, first_exec_node);
    }
    PG_CATCH();
    {

        /*
         * Some nodes failed. Add context about what all nodes the query
         * failed
         */
        ExecNodes* coord_success_nodes = NULL;
        ExecNodes* data_success_nodes = NULL;
        char* msg_failed_nodes = NULL;

        pgxc_all_success_nodes(&data_success_nodes, &coord_success_nodes, &msg_failed_nodes);
        if (msg_failed_nodes != NULL)
            errcontext("%s", msg_failed_nodes);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Execute a Utility statement on nodes, including Coordinators
 * If the DDL is received from a remote Coordinator,
 * it is not possible to push down DDL to Datanodes
 * as it is taken in charge by the remote Coordinator.
 */
void ExecUtilityStmtOnNodes(const char* query_string, ExecNodes* nodes, bool sent_to_remote, bool force_auto_commit,
    RemoteQueryExecType exec_type, bool is_temp, Node* parse_tree)
{
    bool need_free_nodes = false;
    /*
     * @NodeGroup Support
     *
     * Binding necessary datanodes under exec_nodes to run utility, we put the logic here to
     * re-calculate the exec_nodes for target relation in some statements like VACUUM, GRANT,
     * as we don't want to put the similar logic in each swith-case branches in standard_ProcessUtility()
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && parse_tree && nodes == NULL && exec_type != EXEC_ON_COORDS) {
        nodes = assign_utility_stmt_exec_nodes(parse_tree);
        need_free_nodes = true;
    }

    /* single-user mode */
    if (!IsPostmasterEnvironment)
        return;

    /* gs_dump cn do not connect datanode */
    if (u_sess->attr.attr_common.application_name &&
        !strncmp(u_sess->attr.attr_common.application_name, "gs_dump", strlen("gs_dump")))
        return;

    /* Return if query is launched on no nodes */
    if (exec_type == EXEC_ON_NONE)
        return;

    /* Nothing to be done if this statement has been sent to the nodes */
    if (sent_to_remote)
        return;

    /*
     * If no Datanodes defined and the remote utility sent to DN, the query cannot
     * be launched
     */
    if ((exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES) && u_sess->pgxc_cxt.NumDataNodes == 0)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("No Datanode defined in cluster"),
                errhint("You need to define at least 1 Datanode with "
                        "CREATE NODE.")));

#ifdef ENABLE_MULTIPLE_NODES
    if (!IsConnFromCoord()) {
        RemoteQuery* step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->exec_nodes = nodes;
        step->sql_statement = pstrdup(query_string);
        step->force_autocommit = force_auto_commit;
        step->exec_type = exec_type;
        step->is_temp = is_temp;
        ExecRemoteUtility(step);
        pfree_ext(step->sql_statement);
        pfree_ext(step);
        if (need_free_nodes)
            FreeExecNodes(&nodes);
    }
#endif
}

/*
 * Execute a Utility statement on nodes, including Coordinators
 * If the DDL is received from a remote Coordinator,
 * it is not possible to push down DDL to Datanodes
 * as it is taken in charge by the remote Coordinator.
 */
void ExecUtilityStmtOnNodes_ParallelDDLMode(const char* query_string, ExecNodes* nodes, bool sent_to_remote,
    bool force_auto_commit, RemoteQueryExecType exec_type, bool is_temp, const char* first_exec_node, Node* parse_tree)
{
    /*
     * @NodeGroup Support
     *
     * Binding necessary datanodes under exec_nodes to run utility, we put the logic here to
     * re-calculate the exec_nodes for target relation in some statements like VACUUM, GRANT,
     * as we don't want to put the similar logic in each swith-case branches in standard_ProcessUtility()
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && parse_tree && nodes == NULL && exec_type != EXEC_ON_COORDS) {
        nodes = assign_utility_stmt_exec_nodes(parse_tree);
    }

    // single-user mode
    //
    if (!IsPostmasterEnvironment)
        return;

    /* gs_dump cn do not connect datanode */
    if (u_sess->attr.attr_common.application_name &&
        !strncmp(u_sess->attr.attr_common.application_name, "gs_dump", strlen("gs_dump")))
        return;

    /* Return if query is launched on no nodes */
    if (exec_type == EXEC_ON_NONE)
        return;

    /* Nothing to be done if this statement has been sent to the nodes */
    if (sent_to_remote)
        return;

    /*
     * If no Datanodes defined and the remote utility sent to DN, the query cannot
     * be launched
     */
    if ((exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES) && u_sess->pgxc_cxt.NumDataNodes == 0)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("No Datanode defined in cluster"),
                errhint("You need to define at least 1 Datanode with "
                        "CREATE NODE.")));

    if (!IsConnFromCoord()) {
        RemoteQuery* step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->exec_nodes = nodes;
        step->sql_statement = pstrdup(query_string);
        step->force_autocommit = force_auto_commit;
        step->exec_type = exec_type;
        step->is_temp = is_temp;
        ExecRemoteUtility_ParallelDDLMode(step, first_exec_node);
        pfree_ext(step->sql_statement);
        pfree_ext(step);
    }
}

static RemoteQueryExecType set_exec_type(Oid object_id, bool* is_temp)
{
    RemoteQueryExecType type;
    if ((*is_temp = IsTempTable(object_id)))
        type = EXEC_ON_DATANODES;
    else
        type = EXEC_ON_ALL_NODES;
    return type;
}

/*
 * ExecUtilityFindNodes
 *
 * Determine the list of nodes to launch query on.
 * This depends on temporary nature of object and object type.
 * Return also a flag indicating if relation is temporary.
 *
 * If object is a RULE, the object id sent is that of the object to which the
 * rule is applicable.
 */
static RemoteQueryExecType ExecUtilityFindNodes(ObjectType object_type, Oid object_id, bool* is_temp)
{
    RemoteQueryExecType exec_type;

    switch (object_type) {
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
            exec_type = set_exec_type(object_id, is_temp);
            break;

        /* Triggers are evaluated based on the relation they are defined on */
        case OBJECT_TABLE:
        case OBJECT_TRIGGER:
            /* Do the check on relation kind */
            exec_type = exec_utility_find_nodes_relkind(object_id, is_temp);
            break;

        /*
         * Views and rules, both permanent or temporary are created
         * on Coordinators only.
         */
        case OBJECT_RULE:
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
            /* Check if object is a temporary view */
            if ((*is_temp = IsTempTable(object_id)))
                exec_type = EXEC_ON_NONE;
            else
                exec_type = u_sess->attr.attr_common.IsInplaceUpgrade ? EXEC_ON_ALL_NODES : EXEC_ON_COORDS;
            break;

        case OBJECT_INDEX:
        case OBJECT_CONSTRAINT:
            // Temp table. For index or constraint, we should check if it's parent is temp object.
            // NOTICE: the argument object_id should be it's parent's oid, not itself's oid.
            exec_type = set_exec_type(object_id, is_temp);
            break;

        case OBJECT_COLUMN:
        case OBJECT_INTERNAL:
            /*
             * 1. temp view column, exec on current CN and exec_type is EXEC_ON_NONE;
             * 2. temp table column, exec on current CN and DNs, so exec_type is EXEC_ON_DATANODES;
             * 3. normal view column, exec on all CNs, so exec_type is EXEC_ON_COORDS;
             * 4. normal table column, exec on all CNs and DNs, so exec_type is EXEC_ON_ALL_NODES;
             */
            if ((*is_temp = IsTempTable(object_id))) {
                if (IsRelaionView(object_id))
                    exec_type = EXEC_ON_NONE;
                else
                    exec_type = EXEC_ON_DATANODES;
            } else {
                if (IsRelaionView(object_id))
                    exec_type = u_sess->attr.attr_common.IsInplaceUpgrade ? EXEC_ON_ALL_NODES : EXEC_ON_COORDS;
                else
                    exec_type = EXEC_ON_ALL_NODES;
            }
            break;

        default:
            *is_temp = false;
            exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    return exec_type;
}

/*
 * exec_utility_find_nodes_relkind
 *
 * Get node execution and temporary type
 * for given relation depending on its relkind
 */
static RemoteQueryExecType exec_utility_find_nodes_relkind(Oid rel_id, bool* is_temp)
{
    char relkind_str = get_rel_relkind(rel_id);
    RemoteQueryExecType exec_type;

    switch (relkind_str) {
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            *is_temp = IsTempTable(rel_id);
            exec_type = CHOOSE_EXEC_NODES(*is_temp);
            break;

        case RELKIND_RELATION:
            *is_temp = IsTempTable(rel_id);
            exec_type = CHOOSE_EXEC_NODES(*is_temp);
            break;

        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
            if ((*is_temp = IsTempTable(rel_id)))
                exec_type = EXEC_ON_NONE;
            else
                exec_type = u_sess->attr.attr_common.IsInplaceUpgrade ? EXEC_ON_ALL_NODES : EXEC_ON_COORDS;
            break;

        default:
            *is_temp = false;
            exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    return exec_type;
}
#endif

/* Execute a Utility statement on nodes, including Coordinators
 * If the DDL is received from a remote Coordinator,
 * it is not possible to push down DDL to Datanodes
 * as it is taken in charge by the remote Coordinator.
 */
HeapTuple* ExecRemoteVacuumStmt(VacuumStmt* stmt, const char* query_string, bool sent_to_remote, ANALYZE_RQTYPE arq_type,
    AnalyzeMode e_analyze_mode, Oid rel_id)
{
    ExecNodes* nodes = NULL;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (stmt->relation) {
            rel_id = RangeVarGetRelid(stmt->relation, NoLock, false);
        }
        nodes = RelidGetExecNodes(rel_id);
    }

    HeapTuple* result = NULL;
    // single-user mode
    //
    if (!IsPostmasterEnvironment) {
        FreeExecNodes(&nodes);
        return NULL;
    }

    /* Nothing to be done if this statement has been sent to the nodes */
    if (sent_to_remote) {
        FreeExecNodes(&nodes);
        return NULL;
    }

    /* If no Datanodes defined, the query cannot be launched */
    if (u_sess->pgxc_cxt.NumDataNodes == 0)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("No Datanode defined in cluster"),
                errhint("You need to define at least 1 Datanode with "
                        "CREATE NODE.")));

    if (!IsConnFromCoord()) {
        RemoteQuery* step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->exec_nodes = nodes;
        step->sql_statement = pstrdup(query_string);
        step->force_autocommit = true;
        step->exec_type = EXEC_ON_DATANODES;
        step->is_temp = false;
        result = RecvRemoteSampleMessage(stmt, step, arq_type, e_analyze_mode);
        pfree_ext(step->sql_statement);
        pfree_ext(step);
    }
    FreeExecNodes(&nodes);
    return result;
}

/*
 * UtilityReturnsTuples
 *		Return "true" if this utility statement will send output to the
 *		destination.
 *
 * Generally, there should be a case here for each case in ProcessUtility
 * where "dest" is passed on.
 */
bool UtilityReturnsTuples(Node* parse_tree)
{
    switch (nodeTag(parse_tree)) {
        case T_FetchStmt: {
            FetchStmt* stmt = (FetchStmt*)parse_tree;
            Portal portal;

            if (stmt->ismove)
                return false;
            portal = GetPortalByName(stmt->portalname);
            if (!PortalIsValid(portal))
                return false; /* not our business to raise error */
            return portal->tupDesc ? true : false;
        }

        case T_ExecuteStmt: {
            ExecuteStmt* stmt = (ExecuteStmt*)parse_tree;
            PreparedStatement *entry = NULL;

            entry = FetchPreparedStatement(stmt->name, false, false);
            if (entry == NULL)
                return false; /* not our business to raise error */
            if (entry->plansource->resultDesc)
                return true;
            return false;
        }

        case T_ExplainStmt:
            return true;

        case T_VariableShowStmt:
            return true;

        default:
            return false;
    }
}

/*
 * UtilityTupleDescriptor
 *		Fetch the actual output tuple descriptor for a utility statement
 *		for which UtilityReturnsTuples() previously returned "true".
 *
 * The returned descriptor is created in (or copied into) the current memory
 * context.
 */
TupleDesc UtilityTupleDescriptor(Node* parse_tree)
{
    switch (nodeTag(parse_tree)) {
        case T_FetchStmt: {
            FetchStmt* stmt = (FetchStmt*)parse_tree;
            Portal portal;

            if (stmt->ismove)
                return NULL;
            portal = GetPortalByName(stmt->portalname);
            if (!PortalIsValid(portal))
                return NULL; /* not our business to raise error */
            return CreateTupleDescCopy(portal->tupDesc);
        }

        case T_ExecuteStmt: {
            ExecuteStmt* stmt = (ExecuteStmt*)parse_tree;
            PreparedStatement *entry = NULL;

            entry = FetchPreparedStatement(stmt->name, false, true);
            if (entry == NULL)
                return NULL; /* not our business to raise error */
            return FetchPreparedStatementResultDesc(entry);
        }

        case T_ExplainStmt:
            return ExplainResultDesc((ExplainStmt*)parse_tree);

        case T_VariableShowStmt: {
            VariableShowStmt* n = (VariableShowStmt*)parse_tree;

            return GetPGVariableResultDesc(n->name);
        }

        default:
            return NULL;
    }
}

/*
 * QueryReturnsTuples
 *		Return "true" if this Query will send output to the destination.
 */
#ifdef NOT_USED
bool QueryReturnsTuples(Query* parse_tree)
{
    switch (parse_tree->commandType) {
        case CMD_SELECT:
            /* returns tuples ... unless it's DECLARE CURSOR */
            if (parse_tree->utilityStmt == NULL)
                return true;
            break;
        case CMD_MERGE:
            return false;
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
            /* the forms with RETURNING return tuples */
            if (parse_tree->returningList)
                return true;
            break;
        case CMD_UTILITY:
            return UtilityReturnsTuples(parse_tree->utilityStmt);
        case CMD_UNKNOWN:
        case CMD_NOTHING:
            /* probably shouldn't get here */
            break;
        default:
            break;
    }
    return false; /* default */
}
#endif

/*
 * UtilityContainsQuery
 *		Return the contained Query, or NULL if there is none
 *
 * Certain utility statements, such as EXPLAIN, contain a plannable Query.
 * This function encapsulates knowledge of exactly which ones do.
 * We assume it is invoked only on already-parse-analyzed statements
 * (else the contained parse_tree isn't a Query yet).
 *
 * In some cases (currently, only EXPLAIN of CREATE TABLE AS/SELECT INTO and
 * CREATE MATERIALIZED VIEW), potentially Query-containing utility statements
 * can be nested.  This function will drill down to a non-utility Query, or
 * return NULL if none.
 */
Query* UtilityContainsQuery(Node* parse_tree)
{
    Query* qry = NULL;

    switch (nodeTag(parse_tree)) {
        case T_ExplainStmt:
            qry = (Query*)((ExplainStmt*)parse_tree)->query;
            AssertEreport(IsA(qry, Query), MOD_EXECUTOR, "node type is not query");
            if (qry->commandType == CMD_UTILITY)
                return UtilityContainsQuery(qry->utilityStmt);
            return qry;

        case T_CreateTableAsStmt:
            qry = (Query*)((CreateTableAsStmt*)parse_tree)->query;
            AssertEreport(IsA(qry, Query), MOD_EXECUTOR, "node type is not query");
            if (qry->commandType == CMD_UTILITY)
                return UtilityContainsQuery(qry->utilityStmt);
            return qry;

        default:
            return NULL;
    }
}

/*
 * AlterObjectTypeCommandTag
 *		helper function for CreateCommandTag
 *
 * This covers most cases where ALTER is used with an ObjectType enum.
 */
static const char* AlterObjectTypeCommandTag(ObjectType obj_type)
{
    const char* tag = NULL;
    bool is_multiple_nodes = false;
#ifdef ENABLE_MULTIPLE_NODES
    is_multiple_nodes = true;
#endif   /* ENABLE_MULTIPLE_NODES */

    switch (obj_type) {
        case OBJECT_AGGREGATE:
            tag = "ALTER AGGREGATE";
            break;
        case OBJECT_ATTRIBUTE:
            tag = "ALTER TYPE";
            break;
        case OBJECT_CAST:
            tag = "ALTER CAST";
            break;
        case OBJECT_COLLATION:
            tag = "ALTER COLLATION";
            break;
        case OBJECT_COLUMN:
            tag = "ALTER TABLE";
            break;
        case OBJECT_CONSTRAINT:
            tag = "ALTER TABLE";
            break;
        case OBJECT_CONVERSION:
            tag = "ALTER CONVERSION";
            break;
        case OBJECT_DATABASE:
            tag = "ALTER DATABASE";
            break;
        case OBJECT_DOMAIN:
            tag = "ALTER DOMAIN";
            break;
        case OBJECT_EXTENSION:
            tag = "ALTER EXTENSION";
            break;
        case OBJECT_FDW:
            tag = "ALTER FOREIGN DATA WRAPPER";
            break;
        case OBJECT_FOREIGN_SERVER:
            tag = "ALTER SERVER";
            break;
        case OBJECT_FOREIGN_TABLE:
            tag = "ALTER FOREIGN TABLE";
            break;
        case OBJECT_STREAM:
            tag = "ALTER STREAM";
            if (!is_multiple_nodes) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Not supported for streaming engine in current version"),
                        errdetail("You should use the multiple nodes version")));
            }
            break;
        case OBJECT_FUNCTION:
            tag = "ALTER FUNCTION";
            break;
        case OBJECT_PACKAGE:
            tag = "ALTER PACKAGE";
            break;
        case OBJECT_INDEX:
            tag = "ALTER INDEX";
            break;
        case OBJECT_LANGUAGE:
            tag = "ALTER LANGUAGE";
            break;
        case OBJECT_LARGEOBJECT:
            tag = "ALTER LARGE OBJECT";
            break;
        case OBJECT_OPCLASS:
            tag = "ALTER OPERATOR CLASS";
            break;
        case OBJECT_OPERATOR:
            tag = "ALTER OPERATOR";
            break;
        case OBJECT_OPFAMILY:
            tag = "ALTER OPERATOR FAMILY";
            break;
        case OBJECT_RLSPOLICY:
            tag = "ALTER ROW LEVEL SECURITY POLICY";
            break;
        case OBJECT_PARTITION:
            tag = "ALTER TABLE";
            break;

        case OBJECT_PARTITION_INDEX:
            tag = "ALTER INDEX";
            break;

        case OBJECT_ROLE:
        case OBJECT_USER:
            tag = "ALTER ROLE";
            break;
        case OBJECT_RULE:
            tag = "ALTER RULE";
            break;
        case OBJECT_SCHEMA:
            tag = "ALTER SCHEMA";
            break;
        case OBJECT_SEQUENCE:
            tag = "ALTER SEQUENCE";
            break;
        case OBJECT_LARGE_SEQUENCE:
            tag = "ALTER LARGE SEQUENCE";
            break;
        case OBJECT_TABLE:
            tag = "ALTER TABLE";
            break;
        case OBJECT_TABLESPACE:
            tag = "ALTER TABLESPACE";
            break;
        case OBJECT_TRIGGER:
            tag = "ALTER TRIGGER";
            break;
        case OBJECT_TSCONFIGURATION:
            tag = "ALTER TEXT SEARCH CONFIGURATION";
            break;
        case OBJECT_TSDICTIONARY:
            tag = "ALTER TEXT SEARCH DICTIONARY";
            break;
        case OBJECT_TSPARSER:
            tag = "ALTER TEXT SEARCH PARSER";
            break;
        case OBJECT_TSTEMPLATE:
            tag = "ALTER TEXT SEARCH TEMPLATE";
            break;
        case OBJECT_TYPE:
            tag = "ALTER TYPE";
            break;
        case OBJECT_VIEW:
            tag = "ALTER VIEW";
            break;
        case OBJECT_CONTQUERY:
            tag = "ALTER CONTVIEW";
            if (!is_multiple_nodes) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Not supported for streaming engine in current version"),
                        errdetail("You should use the multiple nodes version")));
            }
            break;
        case OBJECT_MATVIEW:
            tag = "ALTER MATERIALIZED VIEW";
            break;
        case OBJECT_DATA_SOURCE:
            tag = "ALTER DATA SOURCE";
            break;
        case OBJECT_DIRECTORY:
            tag = "ALTER DIRECTORY";
            break;
        case OBJECT_SYNONYM:
            tag = "ALTER SYNONYM";
            break;
        case OBJECT_PUBLICATION:
            tag = "ALTER PUBLICATION";
            break;
        case OBJECT_SUBSCRIPTION:
            tag = "ALTER SUBSCRIPTION";
            break;
        default:
            tag = "?\?\?";
            break;
    }

    return tag;
}

/*
 * CreateCommandTag
 *		utility to get a string representation of the command operation,
 *		given either a raw (un-analyzed) parse_tree or a planned query.
 *
 * This must handle all command types, but since the vast majority
 * of 'em are utility commands, it seems sensible to keep it here.
 *
 * NB: all result strings must be shorter than COMPLETION_TAG_BUFSIZE.
 * Also, the result must point at a true constant (permanent storage).
 */
const char* CreateCommandTag(Node* parse_tree)
{
    const char* tag = NULL;
    bool is_multiple_nodes = false;
#ifdef ENABLE_MULTIPLE_NODES
    is_multiple_nodes = true;
#endif   /* ENABLE_MULTIPLE_NODES */

    switch (nodeTag(parse_tree)) {
            /* raw plannable queries */
        case T_InsertStmt:
            tag = "INSERT";
            break;

        case T_DeleteStmt:
            tag = "DELETE";
            break;

        case T_UpdateStmt:
            tag = "UPDATE";
            break;

        case T_MergeStmt:
            tag = "MERGE";
            break;

        case T_SelectStmt:
            tag = "SELECT";
            break;

            /* utility statements --- same whether raw or cooked */
        case T_TransactionStmt: {
            TransactionStmt* stmt = (TransactionStmt*)parse_tree;

            switch (stmt->kind) {
                case TRANS_STMT_BEGIN:
                    tag = "BEGIN";
                    break;

                case TRANS_STMT_START:
                    tag = "START TRANSACTION";
                    break;

                case TRANS_STMT_COMMIT:
                    tag = "COMMIT";
                    break;

                case TRANS_STMT_ROLLBACK:
                case TRANS_STMT_ROLLBACK_TO:
                    tag = "ROLLBACK";
                    break;

                case TRANS_STMT_SAVEPOINT:
                    tag = "SAVEPOINT";
                    break;

                case TRANS_STMT_RELEASE:
                    tag = "RELEASE";
                    break;

                case TRANS_STMT_PREPARE:
                    tag = "PREPARE TRANSACTION";
                    break;

                case TRANS_STMT_COMMIT_PREPARED:
                    tag = "COMMIT PREPARED";
                    break;

                case TRANS_STMT_ROLLBACK_PREPARED:
                    tag = "ROLLBACK PREPARED";
                    break;

                default:
                    tag = "?\?\?";
                    break;
            }
        } break;

        case T_DeclareCursorStmt:
            tag = "DECLARE CURSOR";
            break;

        case T_ClosePortalStmt: {
            ClosePortalStmt* stmt = (ClosePortalStmt*)parse_tree;

            if (stmt->portalname == NULL)
                tag = "CLOSE CURSOR ALL";
            else
                tag = "CLOSE CURSOR";
        } break;

        case T_FetchStmt: {
            FetchStmt* stmt = (FetchStmt*)parse_tree;

            tag = (stmt->ismove) ? "MOVE" : "FETCH";
        } break;

        case T_CreateDomainStmt:
            tag = "CREATE DOMAIN";
            break;

        case T_CreateWeakPasswordDictionaryStmt:
            tag = "CREATE WEAK PASSWORD DICTIONARY";
            break;

        case T_DropWeakPasswordDictionaryStmt:
            tag = "DROP WEAK PASSWORD DICTIONARY";
            break;

        case T_CreateSchemaStmt:
            tag = "CREATE SCHEMA";
            break;

        case T_AlterSchemaStmt:
            tag = "ALTER SCHEMA";
            break;

        case T_CreateStmt:
            tag = "CREATE TABLE";
            break;

        case T_CreateTableSpaceStmt:
            tag = "CREATE TABLESPACE";
            break;

        case T_DropTableSpaceStmt:
            tag = "DROP TABLESPACE";
            break;

        case T_AlterTableSpaceOptionsStmt:
            tag = "ALTER TABLESPACE";
            break;

        case T_CreateExtensionStmt:
            tag = "CREATE EXTENSION";
            break;

        case T_AlterExtensionStmt:
            tag = "ALTER EXTENSION";
            break;

        case T_AlterExtensionContentsStmt:
            tag = "ALTER EXTENSION";
            break;

        case T_CreateFdwStmt:
            tag = "CREATE FOREIGN DATA WRAPPER";
            break;

        case T_AlterFdwStmt:
            tag = "ALTER FOREIGN DATA WRAPPER";
            break;

        case T_CreateForeignServerStmt:
            tag = "CREATE SERVER";
            break;

        case T_AlterForeignServerStmt:
            tag = "ALTER SERVER";
            break;

        case T_CreateUserMappingStmt:
            tag = "CREATE USER MAPPING";
            break;

        case T_AlterUserMappingStmt:
            tag = "ALTER USER MAPPING";
            break;

        case T_DropUserMappingStmt:
            tag = "DROP USER MAPPING";
            break;

        case T_CreateSynonymStmt:
            tag = "CREATE SYNONYM";
            break;

        case T_DropSynonymStmt:
            tag = "DROP SYNONYM";
            break;

        case T_CreateDataSourceStmt:
            tag = "CREATE DATA SOURCE";
            break;

        case T_AlterDataSourceStmt:
            tag = "ALTER DATA SOURCE";
            break;

        case T_CreateRlsPolicyStmt:
            tag = "CREATE ROW LEVEL SECURITY POLICY";
            break;

        case T_AlterRlsPolicyStmt:
            tag = "ALTER ROW LEVEL SECURITY POLICY";
            break;

        case T_CreateForeignTableStmt:
            if (pg_strcasecmp(((CreateForeignTableStmt *)parse_tree)->servername, 
                STREAMING_SERVER) == 0) {
                tag = "CREATE STREAM";
                if (!is_multiple_nodes) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Not supported for streaming engine in current version"),
                            errdetail("You should use the multiple nodes version")));
                }
            } else {
                tag = "CREATE FOREIGN TABLE";
            }
            break;

        case T_DropStmt:
            switch (((DropStmt*)parse_tree)->removeType) {
                case OBJECT_TABLE:
                    tag = "DROP TABLE";
                    break;
                case OBJECT_SEQUENCE:
                    tag = "DROP SEQUENCE";
                    break;
                case OBJECT_LARGE_SEQUENCE:
                    tag = "DROP LARGE SEQUENCE";
                    break;
                case OBJECT_VIEW:
#ifdef ENABLE_MULTIPLE_NODES
                    if (range_var_list_include_streaming_object(((DropStmt*)parse_tree)->objects))
                        tag = "DROP CONTVIEW";
                    else
#endif   /* ENABLE_MULTIPLE_NODES */
                        tag = "DROP VIEW";
                    break;
                case OBJECT_CONTQUERY:
                    tag = "DROP CONTVIEW";
                    if (!is_multiple_nodes) {
                        ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Not supported for streaming engine in current version"),
                                errdetail("You should use the multiple nodes version")));
                    }
                    break;
                case OBJECT_MATVIEW:
                    tag = "DROP MATERIALIZED VIEW";
                    break;
                case OBJECT_INDEX:
                    tag = "DROP INDEX";
                    break;
                case OBJECT_TYPE:
                    tag = "DROP TYPE";
                    break;
                case OBJECT_DOMAIN:
                    tag = "DROP DOMAIN";
                    break;
                case OBJECT_COLLATION:
                    tag = "DROP COLLATION";
                    break;
                case OBJECT_CONVERSION:
                    tag = "DROP CONVERSION";
                    break;
                case OBJECT_DB4AI_MODEL:
                    tag = "DROP MODEL";
                    break;
                case OBJECT_SCHEMA:
                    tag = "DROP SCHEMA";
                    break;
                case OBJECT_TSPARSER:
                    tag = "DROP TEXT SEARCH PARSER";
                    break;
                case OBJECT_TSDICTIONARY:
                    tag = "DROP TEXT SEARCH DICTIONARY";
                    break;
                case OBJECT_TSTEMPLATE:
                    tag = "DROP TEXT SEARCH TEMPLATE";
                    break;
                case OBJECT_TSCONFIGURATION:
                    tag = "DROP TEXT SEARCH CONFIGURATION";
                    break;
                case OBJECT_FOREIGN_TABLE:
#ifdef ENABLE_MULTIPLE_NODES
                    if (range_var_list_include_streaming_object(((DropStmt*)parse_tree)->objects))
                        tag = "DROP STREAM";
                    else
#endif   /* ENABLE_MULTIPLE_NODES */
                        tag = "DROP FOREIGN TABLE";
                    break;
                case OBJECT_STREAM:
                    tag = "DROP STREAM";
                    if (!is_multiple_nodes) {
                        ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Not supported for streaming engine in current version"),
                                errdetail("You should use the multiple nodes version")));
                    }
                    break;
                case OBJECT_EXTENSION:
                    tag = "DROP EXTENSION";
                    break;
                case OBJECT_FUNCTION: {
                    DropStmt* dpStmt = (DropStmt*)parse_tree;
                    tag = (dpStmt->isProcedure) ? "DROP PROCEDURE" : "DROP FUNCTION";
                    break;
                }
                case OBJECT_PACKAGE: {
                    tag = "DROP PACKAGE";
                    break;
                }
                case OBJECT_PACKAGE_BODY: {
                    tag = "DROP PACKAGE BODY";
                    break;
                }
                case OBJECT_AGGREGATE:
                    tag = "DROP AGGREGATE";
                    break;
                case OBJECT_OPERATOR:
                    tag = "DROP OPERATOR";
                    break;
                case OBJECT_LANGUAGE:
                    tag = "DROP LANGUAGE";
                    break;
                case OBJECT_CAST:
                    tag = "DROP CAST";
                    break;
                case OBJECT_TRIGGER:
                    tag = "DROP TRIGGER";
                    break;
                case OBJECT_RULE:
                    tag = "DROP RULE";
                    break;
                case OBJECT_FDW:
                    tag = "DROP FOREIGN DATA WRAPPER";
                    break;
                case OBJECT_FOREIGN_SERVER:
                    tag = "DROP SERVER";
                    break;
                case OBJECT_OPCLASS:
                    tag = "DROP OPERATOR CLASS";
                    break;
                case OBJECT_OPFAMILY:
                    tag = "DROP OPERATOR FAMILY";
                    break;
                case OBJECT_RLSPOLICY:
                    tag = "DROP ROW LEVEL SECURITY POLICY";
                    break;
                case OBJECT_DATA_SOURCE:
                    tag = "DROP DATA SOURCE";
                    break;
                case OBJECT_GLOBAL_SETTING:
                    tag = "DROP CLIENT MASTER KEY";
                    break;
                case OBJECT_COLUMN_SETTING:
                    tag = "DROP COLUMN ENCRYPTION KEY";
                    break;
                case OBJECT_PUBLICATION:
                    tag = "DROP PUBLICATION";
                    break;
                default:
                    tag = "?\?\?";
                    break;
            }
            break;

        case T_TruncateStmt:
            tag = "TRUNCATE TABLE";
            break;

        case T_PurgeStmt:
            switch (((PurgeStmt*)parse_tree)->purtype) {
                case PURGE_TABLE:
                    tag = "PURGE TABLE";
                    break;
                case PURGE_INDEX:
                    tag = "PURGE INDEX";
                    break;
                case PURGE_TABLESPACE:
                    tag = "PURGE TABLESPACE";
                    break;
                case PURGE_RECYCLEBIN:
                    tag = "PURGE RECYCLEBIN";
                    break;
                default:
                    tag = "?\?\?";
                    break;
            }
            break;

        case T_CommentStmt:
            tag = "COMMENT";
            break;

        case T_SecLabelStmt:
            tag = "SECURITY LABEL";
            break;

        case T_CopyStmt:
            tag = "COPY";
            break;

        case T_RenameStmt:
            tag = AlterObjectTypeCommandTag(((RenameStmt*)parse_tree)->renameType);
            break;

        case T_AlterObjectSchemaStmt:
            tag = AlterObjectTypeCommandTag(((AlterObjectSchemaStmt*)parse_tree)->objectType);
            break;

        case T_AlterOwnerStmt:
            tag = AlterObjectTypeCommandTag(((AlterOwnerStmt*)parse_tree)->objectType);
            break;

        case T_AlterTableStmt:
            tag = AlterObjectTypeCommandTag(((AlterTableStmt*)parse_tree)->relkind);
            break;

        case T_AlterDomainStmt:
            tag = "ALTER DOMAIN";
            break;

        case T_AlterFunctionStmt:
            tag = "ALTER FUNCTION";
            break;

        case T_GrantStmt: {
            GrantStmt* stmt = (GrantStmt*)parse_tree;

            tag = (stmt->is_grant) ? "GRANT" : "REVOKE";
        } break;

        case T_GrantRoleStmt: {
            GrantRoleStmt* stmt = (GrantRoleStmt*)parse_tree;

            tag = (stmt->is_grant) ? "GRANT ROLE" : "REVOKE ROLE";
        } break;

        case T_GrantDbStmt: {
            GrantDbStmt* stmt = (GrantDbStmt*)parse_tree;

            tag = (stmt->is_grant) ? "GRANT" : "REVOKE";
        } break;

        case T_AlterDefaultPrivilegesStmt:
            tag = "ALTER DEFAULT PRIVILEGES";
            break;

        case T_DefineStmt:
            switch (((DefineStmt*)parse_tree)->kind) {
                case OBJECT_AGGREGATE:
                    tag = "CREATE AGGREGATE";
                    break;
                case OBJECT_OPERATOR:
                    tag = "CREATE OPERATOR";
                    break;
                case OBJECT_TYPE:
                    tag = "CREATE TYPE";
                    break;
                case OBJECT_TSPARSER:
                    tag = "CREATE TEXT SEARCH PARSER";
                    break;
                case OBJECT_TSDICTIONARY:
                    tag = "CREATE TEXT SEARCH DICTIONARY";
                    break;
                case OBJECT_TSTEMPLATE:
                    tag = "CREATE TEXT SEARCH TEMPLATE";
                    break;
                case OBJECT_TSCONFIGURATION:
                    tag = "CREATE TEXT SEARCH CONFIGURATION";
                    break;
                case OBJECT_COLLATION:
                    tag = "CREATE COLLATION";
                    break;
                default:
                    tag = "?\?\?";
                    break;
            }
            break;

        case T_CompositeTypeStmt:
            tag = "CREATE TYPE";
            break;
        
        case T_TableOfTypeStmt:
            tag = "CREATE TYPE";
            break;

        case T_CreateEnumStmt:
            tag = "CREATE TYPE";
            break;

        case T_CreateRangeStmt:
            tag = "CREATE TYPE";
            break;

        case T_AlterEnumStmt:
            tag = "ALTER TYPE";
            break;

        case T_ViewStmt:
#ifdef ENABLE_MULTIPLE_NODES
            if ((((ViewStmt*)parse_tree)->relkind) == OBJECT_CONTQUERY || 
                view_stmt_has_stream((ViewStmt*)parse_tree))
                tag = "CREATE CONTVIEW";
            else
#endif   /* ENABLE_MULTIPLE_NODES */
                tag = "CREATE VIEW";
            if ((((ViewStmt*)parse_tree)->relkind) == OBJECT_CONTQUERY) {
                if (!is_multiple_nodes) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Not supported for streaming engine in current version"),
                            errdetail("You should use the multiple nodes version")));
                }
            }
            break;

        case T_CreateFunctionStmt: {
            CreateFunctionStmt* cfstmt = (CreateFunctionStmt*)parse_tree;
            tag = (cfstmt->isProcedure) ? "CREATE PROCEDURE" : "CREATE FUNCTION";
            break;
        }
        case T_CreatePackageStmt: {
            tag = "CREATE PACKAGE";
            break;
        }
        case T_CreatePackageBodyStmt: {
            tag = "CREATE PACKAGE BODY";
            break;
        }
        case T_IndexStmt:
            tag = "CREATE INDEX";
            break;

        case T_RuleStmt:
            tag = "CREATE RULE";
            break;

        case T_CreateSeqStmt:
            if (((CreateSeqStmt*)parse_tree)->is_large) {
                tag = "CREATE LARGE SEQUENCE";
            } else {
                tag = "CREATE SEQUENCE";
            }
            break;

        case T_AlterSeqStmt:
            if (((AlterSeqStmt*)parse_tree)->is_large) {
                tag = "ALTER LARGE SEQUENCE";
            } else {
                tag = "ALTER SEQUENCE";
            }
            break;

        case T_DoStmt:
            tag = "ANONYMOUS BLOCK EXECUTE";
            break;

        case T_CreatedbStmt:
            tag = "CREATE DATABASE";
            break;

        case T_AlterDatabaseStmt:
            tag = "ALTER DATABASE";
            break;

        case T_AlterDatabaseSetStmt:
            tag = "ALTER DATABASE";
            break;

        case T_DropdbStmt:
            tag = "DROP DATABASE";
            break;

        case T_NotifyStmt:
            tag = "NOTIFY";
            break;

        case T_ListenStmt:
            tag = "LISTEN";
            break;

        case T_UnlistenStmt:
            tag = "UNLISTEN";
            break;

        case T_LoadStmt:
            tag = "LOAD";
            break;

        case T_ClusterStmt:
            tag = "CLUSTER";
            break;

        case T_VacuumStmt:
            if (((VacuumStmt*)parse_tree)->options & VACOPT_VACUUM) {
                tag = "VACUUM";
            } else if (((VacuumStmt*)parse_tree)->options & VACOPT_MERGE) {
                tag = "DELTA MERGE";
            } else if (((VacuumStmt*)parse_tree)->options & VACOPT_VERIFY) {
                tag = "ANALYZE VERIFY";
            } else {
                tag = "ANALYZE";
            }
            break;

        case T_ExplainStmt:
            tag = "EXPLAIN";
            break;

        case T_CreateTableAsStmt:
            switch (((CreateTableAsStmt *)parse_tree)->relkind)
                {
                    case OBJECT_TABLE:
                        if (((CreateTableAsStmt *)parse_tree)->is_select_into)
                            tag = "SELECT INTO";
                        else
                            tag = "CREATE TABLE AS";
                        break;
                    case OBJECT_MATVIEW:
                        tag = "CREATE MATERIALIZED VIEW";
                        break;
                    default:
                        tag = "???";
                }
                break;
            
        case T_RefreshMatViewStmt:
            tag = "REFRESH MATERIALIZED VIEW";
            break;

#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt:
            tag = "ALTER SYSTEM SET";
            break;
#endif
        case T_VariableSetStmt:
            switch (((VariableSetStmt*)parse_tree)->kind) {
                case VAR_SET_VALUE:
                case VAR_SET_CURRENT:
                case VAR_SET_DEFAULT:
                case VAR_SET_MULTI:
                case VAR_SET_ROLEPWD:
                    tag = "SET";
                    break;
                case VAR_RESET:
                case VAR_RESET_ALL:
                    tag = "RESET";
                    break;
                default:
                    tag = "?\?\?";
                    break;
            }
            break;

        case T_VariableShowStmt:
            tag = "SHOW";
            break;

        case T_DiscardStmt:
            switch (((DiscardStmt*)parse_tree)->target) {
                case DISCARD_ALL:
                    tag = "DISCARD ALL";
                    break;
                case DISCARD_PLANS:
                    tag = "DISCARD PLANS";
                    break;
                case DISCARD_TEMP:
                    tag = "DISCARD TEMP";
                    break;
                default:
                    tag = "?\?\?";
                    break;
            }
            break;

        case T_CreateTrigStmt:
            tag = "CREATE TRIGGER";
            break;

        case T_CreatePLangStmt:
            tag = "CREATE LANGUAGE";
            break;

        case T_CreateRoleStmt:
            tag = "CREATE ROLE";
            break;

        case T_AlterRoleStmt:
            tag = "ALTER ROLE";
            break;

        case T_AlterRoleSetStmt:
            tag = "ALTER ROLE";
            break;

        case T_DropRoleStmt:
            tag = "DROP ROLE";
            break;

        case T_DropOwnedStmt:
            tag = "DROP OWNED";
            break;

        case T_ReassignOwnedStmt:
            tag = "REASSIGN OWNED";
            break;

        case T_LockStmt:
            tag = "LOCK TABLE";
            break;

        case T_TimeCapsuleStmt:
            tag = "TimeCapsule Table";
            break;

        case T_ConstraintsSetStmt:
            tag = "SET CONSTRAINTS";
            break;

        case T_CheckPointStmt:
            tag = "CHECKPOINT";
            break;

#ifdef PGXC
        case T_BarrierStmt:
            tag = "BARRIER";
            break;

        case T_AlterNodeStmt:
        case T_AlterCoordinatorStmt:
            tag = "ALTER NODE";
            break;

        case T_CreateNodeStmt:
            tag = "CREATE NODE";
            break;

        case T_DropNodeStmt:
            tag = "DROP NODE";
            break;

        case T_CreateGroupStmt:
            tag = "CREATE NODE GROUP";
            break;

        case T_AlterGroupStmt:
            tag = "ALTER NODE GROUP";
            break;

        case T_DropGroupStmt:
            tag = "DROP NODE GROUP";
            break;

        case T_CreatePolicyLabelStmt:
            tag = "CREATE RESOURCE LABEL";
            break;

        case T_AlterPolicyLabelStmt:
            tag = "ALTER RESOURCE LABEL";
            break;

        case T_DropPolicyLabelStmt:
            tag = "DROP RESOURCE LABEL";
            break;

        case T_CreateAuditPolicyStmt:
            tag = "CREATE AUDIT POLICY";
            break;

        case T_AlterAuditPolicyStmt:
            tag = "ALTER AUDIT POLICY";
            break;

        case T_DropAuditPolicyStmt:
            tag = "DROP AUDIT POLICY";
            break;

        case T_CreateMaskingPolicyStmt:
            tag = "CREATE MASKING POLICY";
            break;

        case T_AlterMaskingPolicyStmt:
            tag = "ALTER MASKING POLICY";
            break;

        case T_DropMaskingPolicyStmt:
            tag = "DROP MASKING POLICY";
            break;

        case T_CreateResourcePoolStmt:
            tag = "CREATE RESOURCE POOL";
            break;

        case T_AlterResourcePoolStmt:
            tag = "ALTER RESOURCE POOL";
            break;

        case T_DropResourcePoolStmt:
            tag = "DROP RESOURCE POOL";
            break;

        case T_AlterGlobalConfigStmt:
            tag = "ALTER GLOBAL CONFIGURATION";
            break;

        case T_DropGlobalConfigStmt:
            tag = "Drop GLOBAL CONFIGURATION";
            break;

        case T_CreateWorkloadGroupStmt:
            tag = "CREATE WORKLOAD GROUP";
            break;

        case T_AlterWorkloadGroupStmt:
            tag = "ALTER WORKLOAD GROUP";
            break;

        case T_DropWorkloadGroupStmt:
            tag = "DROP WORKLOAD GROUP";
            break;

        case T_CreateAppWorkloadGroupMappingStmt:
            tag = "CREATE APP WORKLOAD GROUP MAPPING";
            break;

        case T_AlterAppWorkloadGroupMappingStmt:
            tag = "ALTER APP WORKLOAD GROUP MAPPING";
            break;

        case T_DropAppWorkloadGroupMappingStmt:
            tag = "DROP APP WORKLOAD GROUP MAPPING";
            break;
#endif

        case T_ReindexStmt:
            tag = "REINDEX";
            break;

        case T_CreateConversionStmt:
            tag = "CREATE CONVERSION";
            break;

        case T_CreateCastStmt:
            tag = "CREATE CAST";
            break;

        case T_CreateOpClassStmt:
            tag = "CREATE OPERATOR CLASS";
            break;

        case T_CreateOpFamilyStmt:
            tag = "CREATE OPERATOR FAMILY";
            break;

        case T_AlterOpFamilyStmt:
            tag = "ALTER OPERATOR FAMILY";
            break;

        case T_AlterTSDictionaryStmt:
            tag = "ALTER TEXT SEARCH DICTIONARY";
            break;

        case T_AlterTSConfigurationStmt:
            tag = "ALTER TEXT SEARCH CONFIGURATION";
            break;

        case T_PrepareStmt:
            tag = "PREPARE";
            break;

        case T_ExecuteStmt:
            tag = "EXECUTE";
            break;

        case T_DeallocateStmt: {
            DeallocateStmt* stmt = (DeallocateStmt*)parse_tree;

            if (stmt->name == NULL)
                tag = "DEALLOCATE ALL";
            else
                tag = "DEALLOCATE";
        } break;

            /* already-planned queries */
        case T_PlannedStmt: {
            PlannedStmt* stmt = (PlannedStmt*)parse_tree;

            switch (stmt->commandType) {
                case CMD_SELECT:

                    /*
                     * We take a little extra care here so that the result
                     * will be useful for complaints about read-only
                     * statements
                     */
                    if (stmt->utilityStmt != NULL) {
                        AssertEreport(
                            IsA(stmt->utilityStmt, DeclareCursorStmt), MOD_EXECUTOR, "not a cursor declare stmt");
                        tag = "DECLARE CURSOR";
                    } else if (stmt->rowMarks != NIL) {
                        /* not 100% but probably close enough */
                        switch (((PlanRowMark *)linitial(stmt->rowMarks))->markType) {
                            case ROW_MARK_EXCLUSIVE:
                                tag = "SELECT FOR UPDATE";
                                break;
                            case ROW_MARK_NOKEYEXCLUSIVE:
                                tag = "SELECT FOR NO KEY UPDATE";
                                break;
                            case ROW_MARK_SHARE:
                                tag = "SELECT FOR SHARE";
                                break;
                            case ROW_MARK_KEYSHARE:
                                tag = "SELECT FOR KEY SHARE";
                                break;
                            default:
                                tag = "SELECT";
                                break;
                        }
                    } else
                        tag = "SELECT";
                    break;
                case CMD_UPDATE:
                    tag = "UPDATE";
                    break;
                case CMD_INSERT:
                    tag = "INSERT";
                    break;
                case CMD_DELETE:
                    tag = "DELETE";
                    break;
                case CMD_MERGE:
                    tag = "MERGE";
                    break;
                default:
                    elog(WARNING, "unrecognized commandType: %d", (int)stmt->commandType);
                    tag = "?\?\?";
                    break;
            }
        } break;

            /* parsed-and-rewritten-but-not-planned queries */
        case T_Query: {
            Query* stmt = (Query*)parse_tree;

            switch (stmt->commandType) {
                case CMD_SELECT:

                    /*
                     * We take a little extra care here so that the result
                     * will be useful for complaints about read-only
                     * statements
                     */
                    if (stmt->utilityStmt != NULL) {
                        AssertEreport(
                            IsA(stmt->utilityStmt, DeclareCursorStmt), MOD_EXECUTOR, "not a cursor declare stmt");
                        tag = "DECLARE CURSOR";
                    } else if (stmt->rowMarks != NIL) {
                        /* not 100% but probably close enough */
                        switch (((RowMarkClause *)linitial(stmt->rowMarks))->strength) {
                            case LCS_FORKEYSHARE:
                                tag = "SELECT FOR KEY SHARE";
                                break;
                            case LCS_FORSHARE:
                                tag = "SELECT FOR SHARE";
                                break;
                            case LCS_FORNOKEYUPDATE:
                                tag = "SELECT FOR NO KEY UPDATE";
                                break;
                            case LCS_FORUPDATE:
                                tag = "SELECT FOR UPDATE";
                                break;
                            default:
                                tag = "?\?\?";
                                break;
                        }
                    } else
                        tag = "SELECT";
                    break;
                case CMD_UPDATE:
                    tag = "UPDATE";
                    break;
                case CMD_INSERT:
                    tag = "INSERT";
                    break;
                case CMD_DELETE:
                    tag = "DELETE";
                    break;
                case CMD_MERGE:
                    tag = "MERGE";
                    break;
                case CMD_UTILITY:
                    tag = CreateCommandTag(stmt->utilityStmt);
                    break;
                default:
                    elog(WARNING, "unrecognized commandType: %d", (int)stmt->commandType);
                    tag = "?\?\?";
                    break;
            }
        } break;

        case T_ExecDirectStmt:
            tag = "EXECUTE DIRECT";
            break;
        case T_CleanConnStmt:
            tag = "CLEAN CONNECTION";
            break;
        case T_CreateDirectoryStmt:
            tag = "CREATE DIRECTORY";
            break;
        case T_DropDirectoryStmt:
            tag = "DROP DIRECTORY";
            break;
        case T_CreateClientLogicGlobal:
            tag = "CREATE CLIENT MASTER KEY";
            break;
        case T_CreateClientLogicColumn:
            tag = "CREATE COLUMN ENCRYPTION KEY";
            break;
        case T_ShutdownStmt:
            tag = "SHUTDOWN";
            break;
        case T_CreateModelStmt:
            tag = "CREATE MODEL";
            break;
        case T_CreatePublicationStmt:
            tag = "CREATE PUBLICATION";
            break;

        case T_AlterPublicationStmt:
            tag = "ALTER PUBLICATION";
            break;

        case T_CreateSubscriptionStmt:
            tag = "CREATE SUBSCRIPTION";
            break;

        case T_AlterSubscriptionStmt:
            tag = "ALTER SUBSCRIPTION";
            break;

        case T_DropSubscriptionStmt:
            tag = "DROP SUBSCRIPTION";
            break;

        default:
            elog(WARNING, "unrecognized node type: %d", (int)nodeTag(parse_tree));
            tag = "?\?\?";
            break;
    }

    return tag;
}

/*
 * GetAlterTableCommandTag
 *		utility to get a string representation of the alter table
 *		command operation.
 */
const char* CreateAlterTableCommandTag(const AlterTableType subtype)
{
    const char* tag = NULL;

    switch (subtype) {
        case AT_AddColumn:
            tag = "ADD COLUMN";
            break;
        case AT_AddColumnRecurse:
            tag = "ADD COLUMN RECURSE";
            break;
        case AT_AddColumnToView:
            tag = "ADD COLUMN TO VIEW";
            break;
        case AT_AddPartition:
            tag = "ADD PARTITION";
            break;
        case AT_AddSubPartition:
            tag = "MODIFY PARTITION ADD SUBPARTITION";
            break;
        case AT_ColumnDefault:
            tag = "COLUMN DEFAULT";
            break;
        case AT_DropNotNull:
            tag = "DROP NOT NULL";
            break;
        case AT_SetNotNull:
            tag = "SET NOT NULL";
            break;
        case AT_SetStatistics:
            tag = "SET STATISTICS";
            break;
        case AT_SetOptions:
            tag = "SET OPTIONS";
            break;
        case AT_ResetOptions:
            tag = "RESET OPTIONS";
            break;
        case AT_SetStorage:
            tag = "SET STORAGE";
            break;
        case AT_DropColumn:
            tag = "DROP COLUMN";
            break;
        case AT_DropColumnRecurse:
            tag = "DROP COLUMN RECURSE";
            break;
        case AT_DropPartition:
            tag = "DROP PARTITION";
            break;
        case AT_DropSubPartition:
            tag = "DROP SUBPARTITION";
            break;
        case AT_AddIndex:
            tag = "ADD INDEX";
            break;
        case AT_ReAddIndex:
            tag = "RE ADD INDEX";
            break;
        case AT_AddConstraint:
            tag = "ADD CONSTRAINT";
            break;
        case AT_AddConstraintRecurse:
            tag = "ADD CONSTRAINT RECURSE";
            break;
        case AT_ValidateConstraint:
            tag = "VALIDATE CONSTRAINT";
            break;
        case AT_ValidateConstraintRecurse:
            tag = "VALIDATE CONSTRAINT RECURSE";
            break;
        case AT_ProcessedConstraint:
            tag = "PROCESSED CONSTRAINT";
            break;
        case AT_AddIndexConstraint:
            tag = "ADD INDEX CONSTRAINT";
            break;
        case AT_DropConstraint:
            tag = "DROP CONSTRAINT";
            break;
        case AT_DropConstraintRecurse:
            tag = "DROP CONSTRAINT RECURSE";
            break;
        case AT_AlterColumnType:
            tag = "ALTER COLUMN TYPE";
            break;
        case AT_AlterColumnGenericOptions:
            tag = "ALTER COLUMN GENERIC OPTIONS";
            break;
        case AT_ChangeOwner:
            tag = "CHANGE OWNER";
            break;
        case AT_ClusterOn:
            tag = "CLUSTER ON";
            break;
        case AT_DropCluster:
            tag = "DROP CLUSTER";
            break;
        case AT_AddOids:
            tag = "ADD OIDS";
            break;
        case AT_AddOidsRecurse:
            tag = "ADD OIDS RECURSE";
            break;
        case AT_DropOids:
            tag = "DROP OIDS";
            break;
        case AT_SetTableSpace:
            tag = "SET TABLE SPACE";
            break;
        case AT_SetPartitionTableSpace:
            tag = "SET PARTITION TABLE SPACE";
            break;
        case AT_SetRelOptions:
            tag = "SET REL OPTIONS";
            break;
        case AT_ResetRelOptions:
            tag = "RESET REL OPTIONS";
            break;
        case AT_ReplaceRelOptions:
            tag = "REPLACE REL OPTIONS";
            break;
        case AT_UnusableIndex:
            tag = "UNUSABLE INDEX";
            break;
        case AT_UnusableIndexPartition:
            tag = "UNUSABLE INDEX PARTITION";
            break;
        case AT_UnusableAllIndexOnPartition:
            tag = "UNUSABLE ALL INDEX ON PARTITION";
            break;
        case AT_RebuildIndex:
            tag = "REBUILD INDEX";
            break;
        case AT_RebuildIndexPartition:
            tag = "REBUILD INDEX PARTITION";
            break;
        case AT_RebuildAllIndexOnPartition:
            tag = "REBUILD ALL INDEX ON PARTITION";
            break;
        case AT_EnableTrig:
            tag = "ENABLE TRIGGER";
            break;
        case AT_EnableAlwaysTrig:
            tag = "ENABLE ALWAYS TRIGGER";
            break;
        case AT_EnableReplicaTrig:
            tag = "ENABLE REPLICA TRIGGER";
            break;
        case AT_DisableTrig:
            tag = "DISABLE TRIGGER";
            break;
        case AT_EnableTrigAll:
            tag = "ENABLE TRIGGER ALL";
            break;
        case AT_DisableTrigAll:
            tag = "DISABLE TRIGGER ALL";
            break;
        case AT_EnableTrigUser:
            tag = "ENABLE TRIGGER USER";
            break;
        case AT_DisableTrigUser:
            tag = "DISABLE TRIGGER USER";
            break;
        case AT_EnableRule:
            tag = "ENABLE RULE";
            break;
        case AT_EnableAlwaysRule:
            tag = "ENABLE ALWAYS RULE";
            break;
        case AT_EnableReplicaRule:
            tag = "ENABLE REPLICA RULE";
            break;
        case AT_DisableRule:
            tag = "DISABLE RULE";
            break;
        case AT_EnableRls:
            tag = "ENABLE ROW LEVEL SECURITY";
            break;
        case AT_DisableRls:
            tag = "DISABLE ROW LEVEL SECURITY";
            break;
        case AT_ForceRls:
            tag = "FORCE ROW LEVEL SECURITY";
            break;
        case AT_NoForceRls:
            tag = "NO FORCE ROW LEVEL SECURITY";
            break;
        case AT_EncryptionKeyRotation:
            tag = "ENCRYPTION KEY ROTATION";
        case AT_AddInherit:
            tag = "ADD INHERIT";
            break;
        case AT_DropInherit:
            tag = "DROP INHERIT";
            break;
        case AT_AddOf:
            tag = "ADD OF";
            break;
        case AT_DropOf:
            tag = "DROP OF";
            break;
        case AT_SET_COMPRESS:
            tag = "SET COMPRESS";
            break;
#ifdef PGXC
        case AT_DistributeBy:
            tag = "DISTRIBUTE BY";
            break;
        case AT_SubCluster:
            tag = "SUB CLUSTER";
            break;
        case AT_AddNodeList:
            tag = "ADD NODE LIST";
            break;
        case AT_DeleteNodeList:
            tag = "DELETE NODE LIST";
            break;
        case AT_UpdateSliceLike:
            tag = "UPDATE SLICE LIKE";
            break;
#endif
        case AT_GenericOptions:
            tag = "GENERIC OPTIONS";
            break;
        case AT_EnableRowMoveMent:
            tag = "ENABLE ROW MOVE MENT";
            break;
        case AT_DisableRowMoveMent:
            tag = "DISABLE ROW MOVE MENT";
            break;
        case AT_TruncatePartition:
            tag = "TRUNCATE PARTITION";
            break;
        case AT_ExchangePartition:
            tag = "EXCHANGE PARTITION";
            break;
        case AT_MergePartition:
            tag = "MERGE PARTITION";
            break;
        case AT_SplitPartition:
            tag = "SPLIT PARTITION";
            break;
        case AT_ReAddConstraint:
            tag = "RE ADD CONSTRAINT";
            break;

        default:
            tag = "?\?\?";
            break;
    }

    return tag;
}

/*
 * GetCommandLogLevel
 *		utility to get the minimum log_statement level for a command,
 *		given either a raw (un-analyzed) parse_tree or a planned query.
 *
 * This must handle all command types, but since the vast majority
 * of 'em are utility commands, it seems sensible to keep it here.
 */
LogStmtLevel GetCommandLogLevel(Node* parse_tree)
{
    LogStmtLevel lev;

    switch (nodeTag(parse_tree)) {
            /* raw plannable queries */
        case T_InsertStmt:
        case T_DeleteStmt:
        case T_UpdateStmt:
        case T_MergeStmt:
            lev = LOGSTMT_MOD;
            break;

        case T_SelectStmt:
            if (((SelectStmt*)parse_tree)->intoClause)
                lev = LOGSTMT_DDL; /* SELECT INTO */
            else
                lev = LOGSTMT_ALL;
            break;

            /* utility statements --- same whether raw or cooked */
        case T_TransactionStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_DeclareCursorStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ClosePortalStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_FetchStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_CreateSchemaStmt:
        case T_AlterSchemaStmt:
            lev = LOGSTMT_DDL;
            break;
            
        case T_CreateWeakPasswordDictionaryStmt:
        case T_DropWeakPasswordDictionaryStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateStmt:
        case T_CreateForeignTableStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateTableSpaceStmt:
        case T_DropTableSpaceStmt:
        case T_AlterTableSpaceOptionsStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateExtensionStmt:
        case T_AlterExtensionStmt:
        case T_AlterExtensionContentsStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateFdwStmt:
        case T_AlterFdwStmt:
        case T_CreateForeignServerStmt:
        case T_AlterForeignServerStmt:
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateDataSourceStmt:
        case T_AlterDataSourceStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateRlsPolicyStmt:
        case T_AlterRlsPolicyStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DropStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_TruncateStmt:
            lev = LOGSTMT_MOD;
            break;

        case T_CommentStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_SecLabelStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CopyStmt:
            if (((CopyStmt*)parse_tree)->is_from)
                lev = LOGSTMT_MOD;
            else
                lev = LOGSTMT_ALL;
            break;

        case T_PrepareStmt: {
            PrepareStmt* stmt = (PrepareStmt*)parse_tree;

            /* Look through a PREPARE to the contained stmt */
            lev = GetCommandLogLevel(stmt->query);
        } break;

        case T_ExecuteStmt: {
            ExecuteStmt* stmt = (ExecuteStmt*)parse_tree;
            PreparedStatement *ps = NULL;

            /* Look through an EXECUTE to the referenced stmt */
            ps = FetchPreparedStatement(stmt->name, false, false);
            if (ps != NULL)
                lev = GetCommandLogLevel(ps->plansource->raw_parse_tree);
            else
                lev = LOGSTMT_ALL;
        } break;

        case T_DeallocateStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_RenameStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterObjectSchemaStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterOwnerStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterTableStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterDomainStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_GrantStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_GrantRoleStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_GrantDbStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterDefaultPrivilegesStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DefineStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreatePackageStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreatePackageBodyStmt:
            lev = LOGSTMT_DDL;
            break;


        case T_CompositeTypeStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateEnumStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateRangeStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterEnumStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_ViewStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateFunctionStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterFunctionStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_IndexStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_RuleStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateSeqStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterSeqStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DoStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_CreatedbStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterDatabaseStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterDatabaseSetStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DropdbStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_NotifyStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ListenStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_UnlistenStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_LoadStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ClusterStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_VacuumStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ExplainStmt: {
            ExplainStmt* stmt = (ExplainStmt*)parse_tree;
            bool analyze = false;
            ListCell* lc = NULL;

            /* Look through an EXPLAIN ANALYZE to the contained stmt */
            foreach (lc, stmt->options) {
                DefElem* opt = (DefElem*)lfirst(lc);

                if (strcmp(opt->defname, "analyze") == 0)
                    analyze = defGetBoolean(opt);
                /* don't "break", as explain.c will use the last value */
            }
            if (analyze)
                return GetCommandLogLevel(stmt->query);

            /* Plain EXPLAIN isn't so interesting */
            lev = LOGSTMT_ALL;
        } break;

        case T_CreateTableAsStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_RefreshMatViewStmt:
#ifndef ENABLE_MULTIPLE_NODES
        case T_AlterSystemStmt:
#endif
            lev = LOGSTMT_DDL;
            break;

        case T_VariableSetStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_VariableShowStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_DiscardStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_CreateTrigStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreatePLangStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateDomainStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateRoleStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterRoleStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterRoleSetStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DropRoleStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_DropOwnedStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_ReassignOwnedStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_LockStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_TimeCapsuleStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ConstraintsSetStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_CheckPointStmt:
            lev = LOGSTMT_ALL;
            break;

        case T_ReindexStmt:
            lev = LOGSTMT_ALL; /* should this be DDL? */
            break;

        case T_CreateConversionStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateCastStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateOpClassStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateOpFamilyStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterOpFamilyStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterTSDictionaryStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_AlterTSConfigurationStmt:
            lev = LOGSTMT_DDL;
            break;

            /* already-planned queries */
        case T_PlannedStmt: {
            PlannedStmt* stmt = (PlannedStmt*)parse_tree;

            switch (stmt->commandType) {
                case CMD_SELECT:
                    lev = LOGSTMT_ALL;
                    break;

                case CMD_UPDATE:
                case CMD_INSERT:
                case CMD_DELETE:
                case CMD_MERGE:
                    lev = LOGSTMT_MOD;
                    break;

                default:
                    elog(WARNING, "unrecognized commandType: %d", (int)stmt->commandType);
                    lev = LOGSTMT_ALL;
                    break;
            }
        } break;

            /* parsed-and-rewritten-but-not-planned queries */
        case T_Query: {
            Query* stmt = (Query*)parse_tree;

            switch (stmt->commandType) {
                case CMD_SELECT:
                    lev = LOGSTMT_ALL;
                    break;

                case CMD_UPDATE:
                case CMD_INSERT:
                case CMD_DELETE:
                case CMD_MERGE:
                    lev = LOGSTMT_MOD;
                    break;

                case CMD_UTILITY:
                    lev = GetCommandLogLevel(stmt->utilityStmt);
                    break;

                default:
                    elog(WARNING, "unrecognized commandType: %d", (int)stmt->commandType);
                    lev = LOGSTMT_ALL;
                    break;
            }

        } break;
        case T_BarrierStmt:
        case T_CreateNodeStmt:
        case T_AlterNodeStmt:
        case T_DropNodeStmt:
        case T_CreateGroupStmt:
        case T_AlterGroupStmt:
        case T_DropGroupStmt:
        case T_CreatePolicyLabelStmt:
        case T_AlterPolicyLabelStmt:
        case T_DropPolicyLabelStmt:
        case T_CreateAuditPolicyStmt:
        case T_AlterAuditPolicyStmt:
        case T_DropAuditPolicyStmt:
        case T_CreateMaskingPolicyStmt:
        case T_AlterMaskingPolicyStmt:
        case T_DropMaskingPolicyStmt:
        case T_CreateResourcePoolStmt:
        case T_AlterResourcePoolStmt:
        case T_DropResourcePoolStmt:
        case T_AlterGlobalConfigStmt:
        case T_DropGlobalConfigStmt:
        case T_CreateWorkloadGroupStmt:
        case T_AlterWorkloadGroupStmt:
        case T_DropWorkloadGroupStmt:
        case T_CreateAppWorkloadGroupMappingStmt:
        case T_AlterAppWorkloadGroupMappingStmt:
        case T_DropAppWorkloadGroupMappingStmt:
        case T_CreatePublicationStmt:
        case T_AlterPublicationStmt:
        case T_CreateSubscriptionStmt:
        case T_AlterSubscriptionStmt:
        case T_DropSubscriptionStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_ExecDirectStmt:
            lev = LOGSTMT_ALL;
            break;

#ifdef PGXC
        case T_CleanConnStmt:
            lev = LOGSTMT_DDL;
            break;
#endif
        case T_CreateDirectoryStmt:
        case T_DropDirectoryStmt:
            lev = LOGSTMT_DDL;
            break;

        case T_CreateSynonymStmt:
        case T_DropSynonymStmt:
            lev = LOGSTMT_DDL;
            break;
        case T_CreateClientLogicGlobal:
        case T_CreateClientLogicColumn:
            lev = LOGSTMT_DDL;
            break;
        case T_ShutdownStmt:
            lev = LOGSTMT_ALL;
            break;
        case T_CreateModelStmt: // DB4AI
            lev = LOGSTMT_ALL;
            break;

        default:
            elog(WARNING, "unrecognized node type: %d", (int)nodeTag(parse_tree));
            lev = LOGSTMT_ALL;
            break;
    }

    return lev;
}

#ifdef PGXC
/*
 * GetCommentObjectId
 * Change to return the nodes to execute the utility on in future.
 *
 * Return Object ID of object commented
 * Note: This function uses portions of the code of CommentObject,
 * even if this code is duplicated this is done like this to facilitate
 * merges with PostgreSQL head.
 */
static RemoteQueryExecType get_nodes_4_comment_utility(CommentStmt* stmt, bool* is_temp, ExecNodes** exec_nodes)
{
    ObjectAddress address;
    Relation relation;
    RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES; /* By default execute on all nodes */
    Oid object_id;

    if (exec_nodes != NULL)
        *exec_nodes = NULL;

    if (stmt->objtype == OBJECT_DATABASE && list_length(stmt->objname) == 1) {
        char* database = strVal(linitial(stmt->objname));
        if (!OidIsValid(get_database_oid(database, true)))
            ereport(WARNING, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", database)));
        /* No clue, return the default one */
        return exec_type;
    }

    address =
        get_object_address(stmt->objtype, stmt->objname, stmt->objargs, &relation, ShareUpdateExclusiveLock, false);
    object_id = address.objectId;

    /*
     * If the object being commented is a rule, the nodes are decided by the
     * object to which rule is applicable, so get the that object's oid
     */
    if (stmt->objtype == OBJECT_RULE) {
        if (!relation || !OidIsValid(relation->rd_id)) {
            /* This should not happen, but prepare for the worst */
            char* rulename = strVal(llast(stmt->objname));
            ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("can not find relation for rule \"%s\" does not exist", rulename)));
            object_id = InvalidOid;
        } else {
            object_id = RelationGetRelid(relation);
            if (exec_nodes != NULL) {
                *exec_nodes = RelidGetExecNodes(object_id, false);
            }
        }
    }

    if (stmt->objtype == OBJECT_CONSTRAINT) {
        if (!relation || !OidIsValid(relation->rd_id)) {
            /* This should not happen, but prepare for the worst */
            char* constraintname = strVal(llast(stmt->objname));
            ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("can not find relation for constraint \"%s\" does not exist", constraintname)));
            object_id = InvalidOid;
        } else {
            object_id = RelationGetRelid(relation);
            if (exec_nodes != NULL) {
                *exec_nodes = RelidGetExecNodes(object_id, false);
            }
        }
    }

    if (relation != NULL)
        relation_close(relation, NoLock);

    /* Commented object may not have a valid object ID, so move to default */
    if (OidIsValid(object_id)) {
        exec_type = ExecUtilityFindNodes(stmt->objtype, object_id, is_temp);
        if (exec_nodes != NULL && (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)) {
            switch (stmt->objtype) {
                case OBJECT_TABLE:
                case OBJECT_FOREIGN_TABLE:
                case OBJECT_STREAM:
                case OBJECT_INDEX:
                case OBJECT_COLUMN:
                    *exec_nodes = RelidGetExecNodes(object_id, false);
                    break;
                case OBJECT_FUNCTION:
                    *exec_nodes = GetFunctionNodes(object_id);
                    break;
                default:
                    break;
            }
        }
    }
    return exec_type;
}

/*
 * get_nodes_4_rules_utility
 * Get the nodes to execute this RULE related utility statement.
 * A rule is expanded on Coordinator itself, and does not need any
 * existence on Datanode. In fact, if it were to exist on Datanode,
 * there is a possibility that it would expand again
 */
static RemoteQueryExecType get_nodes_4_rules_utility(RangeVar* relation, bool* is_temp)
{
    Oid rel_id = RangeVarGetRelid(relation, NoLock, true);
    RemoteQueryExecType exec_type;

    /* Skip if this Oid does not exist */
    if (!OidIsValid(rel_id))
        return EXEC_ON_NONE;

    /*
     * See if it's a temporary object, do we really need
     * to care about temporary objects here? What about the
     * temporary objects defined inside the rule?
     */
    exec_type = ExecUtilityFindNodes(OBJECT_RULE, rel_id, is_temp);
    return exec_type;
}

/*
 * TreatDropStmtOnCoord
 * Do a pre-treatment of Drop statement on a remote Coordinator
 */
static void drop_stmt_pre_treatment(
    DropStmt* stmt, const char* query_string, bool sent_to_remote, bool* is_temp, RemoteQueryExecType* exec_type)
{
    bool res_is_temp = false;
    RemoteQueryExecType res_exec_type = EXEC_ON_ALL_NODES;

#ifndef ENABLE_MULTIPLE_NODES
    switch (stmt->removeType) {
        case OBJECT_TABLE: {
            /*
             * Check whether the tables are in blockchain schema.
             */
            ListCell* cell = NULL;
            foreach (cell, stmt->objects) {
                RangeVar* rel = makeRangeVarFromNameList((List*)lfirst(cell));
                Oid rel_id = RangeVarGetRelid(rel, AccessShareLock, true);
                if (is_ledger_hist_table(rel_id)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for userchain table.")));
                }
                if (OidIsValid(rel_id)) {
                    UnlockRelationOid(rel_id, AccessShareLock);
                }
            }
        } break;
        case OBJECT_SCHEMA: {
            ListCell* cell = NULL;
            foreach (cell, stmt->objects) {
                List* objname = (List*)lfirst(cell);
                char* name = NameListToString(objname);

                if (get_namespace_oid(name, true) == PG_BLOCKCHAIN_NAMESPACE) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for blockchain schema.")));
                } else if (!u_sess->attr.attr_common.IsInplaceUpgrade &&
                    get_namespace_oid(name, true) == PG_SQLADVISOR_NAMESPACE) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for sqladvisor schema.")));
                } else if (!u_sess->attr.attr_common.IsInplaceUpgrade && IsPackageSchemaName(name)) {
                    ereport(ERROR,
                        (errmodule(MOD_COMMAND), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for %s schema.", name),
                            errdetail("Schemas in a package cannot be deleted."),
                            errcause("The schema in the package does not support object drop."),
                            erraction("N/A")));
                }
            }
        } break;
        default:
            break;
    }
#endif

    /* Nothing to do if not local Coordinator */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /*
     * Check for shared-cache-inval messages before trying to access the
     * relation.  This is needed to cover the case where the name
     * identifies a rel that has been dropped and recreated since the
     * start of our transaction: if we don't flush the old syscache entry,
     * then we'll latch onto that entry and suffer an error later.
     */
    AcceptInvalidationMessages();

    switch (stmt->removeType) {
        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_INDEX: {
            /*
             * Check the list of objects going to be dropped.
             * XC does not allow yet to mix drop of temporary and
             * non-temporary objects because this involves to rewrite
             * query to process for tables.
             */
            ListCell* cell = NULL;
            bool is_first = true;

            foreach (cell, stmt->objects) {
                RangeVar* rel = makeRangeVarFromNameList((List*)lfirst(cell));
                Oid rel_id;

                /*
                 * Do not print result at all, error is thrown
                 * after if necessary
                 * Notice : need get lock here to forbid parallel drop
                 */
                rel_id = RangeVarGetRelid(rel, AccessShareLock, true);

                /*
                 * In case this relation ID is incorrect throw
                 * a correct DROP error.
                 */
                if (!OidIsValid(rel_id) && !stmt->missing_ok)
                    DropTableThrowErrorExternal(rel, stmt->removeType, stmt->missing_ok);

                /* In case of DROP ... IF EXISTS bypass */
                if (!OidIsValid(rel_id) && stmt->missing_ok)
                    continue;

                if (is_ledger_hist_table(rel_id)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for userchain table.")));
                }

                if (is_first) {
                    res_exec_type = ExecUtilityFindNodes(stmt->removeType, rel_id, &res_is_temp);
                    is_first = false;
                } else {
                    RemoteQueryExecType exec_type_loc;
                    bool is_temp_loc = false;
                    exec_type_loc = ExecUtilityFindNodes(stmt->removeType, rel_id, &is_temp_loc);
                    if (exec_type_loc != res_exec_type || is_temp_loc != res_is_temp)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("DROP not supported for TEMP and non-TEMP objects"),
                                errdetail("You should separate TEMP and non-TEMP objects")));
                }
                UnlockRelationOid(rel_id, AccessShareLock);
            }
        } break;

        case OBJECT_SCHEMA: {
            ListCell* cell = NULL;
            bool has_temp = false;
            bool has_nontemp = false;

            foreach (cell, stmt->objects) {
                List* objname = (List*)lfirst(cell);
                char* name = NameListToString(objname);
                if (isTempNamespaceName(name) || isToastTempNamespaceName(name)) {
                    has_temp = true;
                    res_exec_type = EXEC_ON_DATANODES;
                } else {
                    has_nontemp = true;
                }

                if (get_namespace_oid(name, true) == PG_BLOCKCHAIN_NAMESPACE) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for blockchain schema.")));
                } else if (!u_sess->attr.attr_common.IsInplaceUpgrade &&
                    get_namespace_oid(name, true) == PG_SQLADVISOR_NAMESPACE) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for sqladvisor schema.")));
                } else if (!u_sess->attr.attr_common.IsInplaceUpgrade && IsPackageSchemaName(name)) {
                    ereport(ERROR,
                        (errmodule(MOD_COMMAND), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for %s schema.", name),
                            errdetail("Schemas in a package cannot be deleted."),
                            errcause("The schema in the package does not support object drop."),
                            erraction("N/A")));

                }

                if (has_temp && has_nontemp)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DROP not supported for TEMP and non-TEMP objects"),
                            errdetail("You should separate TEMP and non-TEMP objects")));
            }

            res_is_temp = has_temp;
            if (has_temp)
                res_exec_type = EXEC_ON_DATANODES;
            else
                res_exec_type = EXEC_ON_ALL_NODES;

            break;
        }
        /*
         * Those objects are dropped depending on the nature of the relationss
         * they are defined on. This evaluation uses the temporary behavior
         * and the relkind of the relation used.
         */
        case OBJECT_RULE:
        case OBJECT_TRIGGER: {
            List* objname = (List*)linitial(stmt->objects);
            Relation relation = NULL;

            get_object_address(stmt->removeType, objname, NIL, &relation, AccessExclusiveLock, stmt->missing_ok);

            /* Do nothing if no relation */
            if (relation && OidIsValid(relation->rd_id))
                res_exec_type = ExecUtilityFindNodes(stmt->removeType, relation->rd_id, &res_is_temp);
            else
                res_exec_type = EXEC_ON_NONE;

            /* Close relation if necessary */
            if (relation)
                relation_close(relation, NoLock);
        } break;
        /* Datanode do not have rls information */
        case OBJECT_RLSPOLICY:
            res_is_temp = false;
            res_exec_type = EXEC_ON_COORDS;
            break;
        default:
            res_is_temp = false;
            res_exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    /* Save results */
    *is_temp = res_is_temp;
    *exec_type = res_exec_type;
}
#endif

char* VariableBlackList[] = {"client_encoding"};

bool IsVariableinBlackList(const char* name)
{
    if (name == NULL) {
        return false;
    }

    bool isInBlackList = false;
    int blacklistLen = sizeof(VariableBlackList) / sizeof(char*);

    for (int i = 0; i < blacklistLen; i++) {
        char* blackname = VariableBlackList[i];
        AssertEreport(blackname, MOD_EXECUTOR, "the character string is NULL");

        if (0 == pg_strncasecmp(blackname, name, strlen(blackname))) {
            isInBlackList = true;
            break;
        }
    }

    return isInBlackList;
}

/*
 * Check if the object is in blacklist, if true, ALTER/DROP operation of the object is disabled.
 * Note that only prescribed extensions are able droppable.
 */
void CheckObjectInBlackList(ObjectType obj_type, const char* query_string)
{
    const char* tag = NULL;
    bool is_not_extended_feature = false;

    switch (obj_type) {
        case OBJECT_EXTENSION:
            /* Check black list in RemoveObjects */
            return;
#ifdef ENABLE_MULTIPLE_NODES
        case OBJECT_AGGREGATE:
            tag = "AGGREGATE";
            break;
#endif /* ENABLE_MULTIPLE_NODES */
        case OBJECT_OPERATOR:
            tag = "OPERATOR";
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case OBJECT_OPCLASS:
            tag = "OPERATOR CLASS";
            break;
#endif
        case OBJECT_OPFAMILY:
            tag = "OPERATOR FAMILY";
            break;
        case OBJECT_COLLATION:
            tag = "COLLATION";
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case OBJECT_RULE:
            tag = "RULE";
            break;
#endif
        case OBJECT_TSDICTIONARY:
        case OBJECT_TSCONFIGURATION:
            ts_check_feature_disable();
            return;
        case OBJECT_TSPARSER:
            is_not_extended_feature = true;
            tag = "TEXT SEARCH PARSER";
            break;
        case OBJECT_TSTEMPLATE:
            is_not_extended_feature = true;
            tag = "TEXT SEARCH TEMPLATE";
            break;
        case OBJECT_LANGUAGE:
            tag = "LANGUAGE";
            break;
        case OBJECT_DOMAIN:
            tag = "DOMAIN";
            break;
        case OBJECT_CONVERSION:
            tag = "CONVERSION";
            break;
        case OBJECT_FDW:
            tag = "FOREIGN DATA WRAPPER";
            break;
        default:
            return;
    }

    /* Enable DROP/ALTER operation of the above objects during inplace upgrade. */
    if (!u_sess->attr.attr_common.IsInplaceUpgrade) {
        if (is_not_extended_feature && !IsInitdb) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("%s is not yet supported.", tag)));
        } else if ((!g_instance.attr.attr_common.support_extended_features) &&
                   (!(obj_type == OBJECT_RULE && u_sess->attr.attr_sql.enable_cluster_resize))) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("%s is not yet supported.", tag)));
        }
    }
}

/*
 * CheckObjectInBlackList
 *
 * Check if the given extension is supported. If hashCheck is true,
 * the hash value of the sql script will be checked.
 */
bool CheckExtensionInWhiteList(const char* extension_name, uint32 hash_value, bool hash_check)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* 2902411162 hash for fastcheck, the sql file is much shorter than published version. */
    uint32 postgisHashHistory[POSTGIS_VERSION_NUM] = {2902411162, 2959454932};

    /* check for extension name */
    if (pg_strcasecmp(extension_name, "postgis") == 0) {
        /* PostGIS is disabled under 300 deployment */
        if (is_feature_disabled(POSTGIS_DOCKING)) {
            return false;
        }

        if (hash_check && !isSecurityMode) {
            /* check for postgis hashvalue */
            for (int i = 0; i < POSTGIS_VERSION_NUM; i++) {
                if (hash_value == postgisHashHistory[i]) {
                    return true;
                }         
            }
        } else {
            return true;
        }
    }

    return false;
#endif
    return true;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @hdfs
 * notify_other_cn_get_statistics
 *
 * We send hybridMessage to each other coordinator node. These nodes get statistics from
 * predefined data node represented in hybridMessage.
 */
static void notify_other_cn_get_statistics(const char* hybrid_message, bool sent_to_remote)
{
    ExecNodes* exec_nodes = NULL;
    int node_index;
    exec_nodes = makeNode(ExecNodes);
    for (node_index = 0; node_index < u_sess->pgxc_cxt.NumCoords; node_index++) {
        if (node_index != PGXCNodeGetNodeIdFromName(g_instance.attr.attr_common.PGXCNodeName, PGXC_NODE_COORDINATOR))
            exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, node_index);
    }
    ExecUtilityStmtOnNodes(hybrid_message, exec_nodes, sent_to_remote, true, EXEC_ON_COORDS, false);
    FreeExecNodes(&exec_nodes);

    /*
     * for VACUUM ANALYZE table where options=3, vacuum has set use_own_xacts
     * true and already poped ActiveSnapshot, so we don't want to pop again.
     */
    if (ActiveSnapshotSet()) {
        PopActiveSnapshot();
    }

    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
}
#endif

/*
 * @hdfs
 * AssembleHybridMessage
 *
 * Assembel hybridmessage. Copy query_string and scheduling_message into query_string_with_info.
 */
void AssembleHybridMessage(char** query_string_with_info, const char* query_string, const char* scheduling_message)
{
    unsigned int query_string_len = strlen(query_string);
    unsigned int scheduling_message_len = strlen(scheduling_message);
    unsigned int pos = 0;
    unsigned int alloc_len = 0;
    errno_t errorno = EOK;

    /* make Const node and set query_string_len identify how long does the query string command. */
    Const* n = makeNode(Const);
    n->constvalue = UInt32GetDatum(query_string_len);
    n->constisnull = false;
    n->constlen = sizeof(uint32);
    n->constbyval = true;
    char* query_len_const = nodeToString(n);

    /* the query string with info include 'h'+querylenConst+query_string+scheduling_message+'\0'. */
    alloc_len = 2 + strlen(query_len_const) + strlen(query_string) + strlen(scheduling_message);
    *query_string_with_info = (char*)MemoryContextAlloc(t_thrd.mem_cxt.msg_mem_cxt, alloc_len);

    const size_t head_len = 1;
    /* write hybirdmessage head into query_string_with_info */
    errorno = memcpy_s(*query_string_with_info + pos, head_len, "h", head_len);
    securec_check(errorno, "\0", "\0");

    pos += head_len;
    /* write serialized Const identify how long does the query_string into query_string_with_info */
    errorno = memcpy_s(*query_string_with_info + pos, strlen(query_len_const), query_len_const, strlen(query_len_const));
    securec_check(errorno, "\0", "\0");

    pos += strlen(query_len_const);
    /* write query_string into query_string_with_info. query_string is a executive sql sentence. */
    errorno = memcpy_s(*query_string_with_info + pos, query_string_len, query_string, query_string_len);
    securec_check(errorno, "\0", "\0");

    pos += query_string_len;
    /* write scheduling_message into query_string_with_info. */
    errorno = memcpy_s(*query_string_with_info + pos, scheduling_message_len, scheduling_message, scheduling_message_len);
    securec_check(errorno, "\0", "\0");

    pos += scheduling_message_len;
    const size_t end_len = 1;
    /* write terminator at the end of query_string_with_info */
    errorno = memcpy_s(*query_string_with_info + pos, end_len, "\0", end_len);
    securec_check(errorno, "\0", "\0");

    pfree_ext(n);
    pfree_ext(query_len_const);
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @hdfs
 * get_scheduling_message
 *
 * In this function we call CNSchedulingForAnalyze to get scheduling information. The exact count of file which
 * will be analyzed is stored in totalFileCnt. At the same time,  CNSchedulingForAnalyze function selects a data
 * node to execute analyze operation. The selected datanode number is stored in nodeNo.
 */
static char* get_scheduling_message(const Oid foreign_table_id, VacuumStmt* stmt)
{
    HDFSTableAnalyze* hdfs_table_analyze = makeNode(HDFSTableAnalyze);
    List* dn_task = NIL; /* Get scheduling information */

    /* get right scheduling messages for global stats. */
    dn_task = CNSchedulingForAnalyze(&stmt->totalFileCnt, &stmt->DnCnt, foreign_table_id, true);

    /* set default values.  */
    hdfs_table_analyze->DnCnt = 0;
    stmt->nodeNo = 0;
    stmt->hdfsforeignMapDnList = NIL;

    /*
     * There is a risk that dn_task can be null. We mush process this situation
     * It means that we call CNSchedulingForAnalyze failed.
     */
    if (dn_task != NIL) {
        bool first = true;
        ListCell* taskCell = NULL;

        if (!IS_OBS_CSV_TXT_FOREIGN_TABLE(foreign_table_id)) {
            SplitMap* task_map = NULL;
            foreach (taskCell, dn_task) {
                task_map = (SplitMap*)lfirst(taskCell);
                if (task_map->splits != NIL) {
                    /* we need to find the first dn which have filelist to get dndistinct for global stats. */
                    if (first) {
                        hdfs_table_analyze->DnCnt = stmt->DnCnt;
                        stmt->nodeNo = task_map->nodeId;
                        first = false;
                    }

                    /* get all nodeid which have filelist in order to get total reltuples from them later. */
                    stmt->hdfsforeignMapDnList = lappend_int(stmt->hdfsforeignMapDnList, task_map->nodeId);
                }
            }
        } else {
            DistFdwDataNodeTask* task_map = NULL;
            first = true;
            foreach (taskCell, dn_task) {
                task_map = (DistFdwDataNodeTask*)lfirst(taskCell);
                if (NIL != task_map->task) {
                    /* we need to find the first dn which have filelist to get dndistinct for global stats. */
                    if (first) {
                        hdfs_table_analyze->DnCnt = stmt->DnCnt;

                        Oid nodeOid = get_pgxc_nodeoid(task_map->dnName);
                        stmt->nodeNo = PGXCNodeGetNodeId(nodeOid, PGXC_NODE_DATANODE);
                        first = false;
                    }

                    /* get all nodeid which have filelist in order to get total reltuples from them later. */
                    stmt->hdfsforeignMapDnList = lappend_int(stmt->hdfsforeignMapDnList, stmt->nodeNo);
                }
            }
        }
    }

    /* dn_task can be null when we call CNSchedulingForAnalyze failed */
    hdfs_table_analyze->DnWorkFlow = dn_task;

    return nodeToString(hdfs_table_analyze);
}
#endif

/**
 * @Description: attatch sample rate to the query_string for global stats
 * @out query_string_with_info - the query string with analyze command and schedulingMessag
 * @in stmt - the statment for analyze or vacuum command
 * @in query_string - the query string for analyze or vacuum command
 * @in rel_id - the relation oid for analyze or vacuum
 * @in has_var - the flag identify do analyze for one relation or all database
 * @in e_analyze_mode - identify hdfs table or normal table
 * @in rel_id - the hdfs foreign table oid. others, it is invalid for other tables
 * @in foreign_tbl_schedul_message - identify the schedul info for hdfs foreign table
 *
 * @return: void
 */
static void attatch_global_info(char** query_string_with_info, VacuumStmt* stmt, const char* query_string, bool has_var,
    AnalyzeMode e_analyze_mode, Oid rel_id, char* foreign_tbl_schedul_message)
{
    SplitMap* task_map = makeNode(SplitMap);
    HDFSTableAnalyze* hdfs_table_analyze = NULL;
    char* global_info = NULL;

    if (!stmt->isForeignTables || stmt->isPgFdwForeignTables) {
        hdfs_table_analyze = makeNode(HDFSTableAnalyze);

        // stmt->sampleRate, DNs get the sample rate for sending sample rows to CN.
        if (e_analyze_mode == ANALYZENORMAL) {
            hdfs_table_analyze->isHdfsStore = false;
            hdfs_table_analyze->sampleRate[0] = stmt->pstGlobalStatEx[0].sampleRate;
        } else {
            hdfs_table_analyze->isHdfsStore = true;
            for (int i = 0; i < ANALYZE_MODE_MAX_NUM - 1; i++)
                hdfs_table_analyze->sampleRate[i] = stmt->pstGlobalStatEx[i].sampleRate;
        }

        task_map->locatorType = LOCATOR_TYPE_NONE;
        task_map->splits = NIL;
        hdfs_table_analyze->DnCnt = 0;
        hdfs_table_analyze->DnWorkFlow = lappend(hdfs_table_analyze->DnWorkFlow, task_map);
        hdfs_table_analyze->isHdfsForeignTbl = false;
    } else {
        /*
         * Call scheduler to get datanode which will execute analyze operation
         * and file list.
         */
        if (foreign_tbl_schedul_message != NULL) {
            hdfs_table_analyze = (HDFSTableAnalyze*)stringToNode(foreign_tbl_schedul_message);
        } else {
            /* make a default dn task for do analyze foreign tables. */ 
            hdfs_table_analyze = makeNode(HDFSTableAnalyze);
            task_map->locatorType = LOCATOR_TYPE_NONE;
            task_map->splits = NIL;
            hdfs_table_analyze->DnCnt = 0;
            hdfs_table_analyze->DnWorkFlow = lappend(hdfs_table_analyze->DnWorkFlow, task_map);
        }

        hdfs_table_analyze->isHdfsStore = false;
        hdfs_table_analyze->sampleRate[0] = stmt->pstGlobalStatEx[0].sampleRate;
        hdfs_table_analyze->isHdfsForeignTbl = true;
    }

    /* orgCnNodeNo is the ID of CN which do global analyze, the other CNs fetch stats from it */
    hdfs_table_analyze->orgCnNodeNo =
        PGXCNodeGetNodeIdFromName(g_instance.attr.attr_common.PGXCNodeName, PGXC_NODE_COORDINATOR);
    hdfs_table_analyze->sampleTableRequired = stmt->sampleTableRequired;
    hdfs_table_analyze->tmpSampleTblNameList = stmt->tmpSampleTblNameList;
    hdfs_table_analyze->disttype = stmt->disttype;
    hdfs_table_analyze->memUsage.work_mem = stmt->memUsage.work_mem;
    hdfs_table_analyze->memUsage.max_mem = stmt->memUsage.max_mem;
    global_info = nodeToString(hdfs_table_analyze);

    if (has_var) {
        AssembleHybridMessage(query_string_with_info, query_string, global_info);
    } else {
        AssertEreport(query_string != NULL, MOD_EXECUTOR, "Invalid NULL value");

        if (!stmt->isForeignTables || stmt->isPgFdwForeignTables) {
            StringInfo stringptr = makeStringInfo();
            /*
             * If the query comes from JDBC connecion, there is without ';'
             * at the end of query_string.
             */
            const char* position = NULL;
            position = strrchr(query_string, ';');
            if (position == NULL) {
                /* We don't find ';' at the end of query_string. copy query_string to stringptr. */
                appendBinaryStringInfo(stringptr, query_string, strlen(query_string));
            } else {
                bool hasSemicolon = true;
                position += 1;
                /* Find the char ';' is the end of query_string or not. */
                while (*position != '\0') {
                    if (!((*position == ' ') || (*position == '\n') || (*position == '\t') || (*position == '\r') ||
                            (*position == '\f') || (*position == '\v'))) {
                        /* We have find ';', but it is not the end of query_string. copy query_string to stringptr. */
                        appendBinaryStringInfo(stringptr, query_string, strlen(query_string));
                        hasSemicolon = false;
                        break;
                    }
                    ++position;
                }

                if (hasSemicolon) {
                    /* Find ';' at the end of query_string. copy query_string to stringptr except ';'. */
                    appendBinaryStringInfo(stringptr, query_string, strlen(query_string) - 1);
                }
            }

            appendStringInfoSpaces(stringptr, 1);
            appendStringInfoString(stringptr, quote_identifier(stmt->relation->schemaname));
            appendStringInfoChar(stringptr, '.');
            appendStringInfoString(stringptr, quote_identifier(stmt->relation->relname));
            appendStringInfoChar(stringptr, ';');
            AssembleHybridMessage(query_string_with_info, stringptr->data, global_info);
            pfree_ext(stringptr->data);
            pfree_ext(stringptr);
        } else {
            /*
             * Assemble ForeignTableDesc and put it into workList.
             * We will use items in workList to apply analzye operation
             */
            ForeignTableDesc* foreignTableDesc = makeNode(ForeignTableDesc);
            foreignTableDesc->tableName = (char*)quote_identifier(stmt->relation->relname);
            foreignTableDesc->tableOid = rel_id;
            foreignTableDesc->schemaName = (char*)quote_identifier(stmt->relation->schemaname);

            /* Get hybridmessage and send it to data node */
            *query_string_with_info = get_hybrid_message(foreignTableDesc, stmt, global_info);
            pfree_ext(foreignTableDesc);
        }
    }

    pfree_ext(task_map);
    pfree_ext(hdfs_table_analyze);
    pfree_ext(global_info);
}

/*
 * @hdfs
 * get_hybrid_message
 *
 * This function uses parameter table_desc generating hybridmessage which will be sent
 * to data node.
 */
static char* get_hybrid_message(ForeignTableDesc* table_desc, VacuumStmt* stmt, char* foreign_tbl_schedul_message)
{
    char* table_name = table_desc->tableName;
    char* schema_name = table_desc->schemaName;
    size_t table_len = strlen(table_name);
    size_t schema_len = strlen(schema_name);
    const char* analyze_key_words = "analyze \0";
    const char* verbose_key_words = "verbose \0";
    unsigned int analyze_key_words_len = strlen(analyze_key_words);
    unsigned int verbose_key_words_len = strlen(verbose_key_words);
    char* query_string = NULL;
    int pos = 0;
    char* scheduling_message = NULL;
    errno_t errorno = EOK;

    /* get right scheduling messages. */
    scheduling_message = foreign_tbl_schedul_message;

    /*
     * If SQL sentence includes "verbose" command, we write verbose keyword into query_string.
     */
    if (stmt->options & VACOPT_VERBOSE) {
        query_string =
            (char*)palloc(analyze_key_words_len + verbose_key_words_len + table_len + schema_len + 3);
        errorno = memcpy_s(query_string, analyze_key_words_len, analyze_key_words, analyze_key_words_len);
        securec_check(errorno, "\0", "\0");

        pos += analyze_key_words_len;
        errorno = memcpy_s(query_string + pos, verbose_key_words_len, verbose_key_words, verbose_key_words_len);
        securec_check(errorno, "\0", "\0");

        pos += verbose_key_words_len;
        errorno = memcpy_s(query_string + pos, schema_len, schema_name, schema_len);
        securec_check(errorno, "\0", "\0");

        const size_t spilt_len = 1;
        pos += schema_len;
        errorno = memcpy_s(query_string + pos, spilt_len, ".", spilt_len);
        securec_check(errorno, "\0", "\0");

        pos += spilt_len;
        errorno = memcpy_s(query_string + pos, table_len, table_name, table_len);
        securec_check(errorno, "\0", "\0");

        pos += table_len;
        errorno = memcpy_s(query_string + pos, spilt_len, ";", spilt_len);
        securec_check(errorno, "\0", "\0");

        pos += spilt_len;
        query_string[pos] = '\0';
    } else {
        query_string = (char*)palloc(analyze_key_words_len + table_len + schema_len + 3);
        errorno = memcpy_s(query_string + pos, analyze_key_words_len, analyze_key_words, analyze_key_words_len);
        securec_check(errorno, "\0", "\0");

        pos += analyze_key_words_len;
        errorno = memcpy_s(query_string + pos, schema_len, schema_name, schema_len);
        securec_check(errorno, "\0", "\0");

        const size_t spilt_len = 1;
        pos += schema_len;
        errorno = memcpy_s(query_string + pos, spilt_len, ".", spilt_len);
        securec_check(errorno, "\0", "\0");

        pos += spilt_len;
        errorno = memcpy_s(query_string + pos, table_len, table_name, table_len);
        securec_check(errorno, "\0", "\0");

        pos += table_len;
        errorno = memcpy_s(query_string + pos, spilt_len, ";", spilt_len);
        securec_check(errorno, "\0", "\0");

        pos += spilt_len;

        query_string[pos] = '\0';
    }

    /* Write query_string and schedulingMesage into query_string_with_info. */
    char* query_string_with_info = NULL;
    AssembleHybridMessage(&query_string_with_info, query_string, scheduling_message);

    /* Free memory */
    pfree_ext(query_string);

    return query_string_with_info;
}

/*
 * @hdfs
 * IsForeignTableAnalyze
 *
 * A table is foreign table or not. If it is a foreign tabel, does it support analyze operation?
 */
bool IsHDFSForeignTableAnalyzable(VacuumStmt* stmt)
{
    Oid foreign_table_id = 0;
    bool ret_value = false;

    stmt->isPgFdwForeignTables = false;
#ifdef ENABLE_MOT
    stmt->isMOTForeignTable = false;
#endif

    if (stmt->isForeignTables == true) {
        /* "analyze foreign tables;" command. Analyze all foreign tables */
        ret_value = true;
    } else if (!stmt->isForeignTables && !stmt->relation) {
        /* "analyze [verbose]" analyze MPP local table/tables */
        ret_value = false;
    } else {
        foreign_table_id = RangeVarGetRelidExtended(stmt->relation, NoLock, false, false, false, true, NULL, NULL);

        /*
         * A MPP local table or foreign table? If it is a foreign table, support analyze or not?
         * False for MPP Local table and foreign table which does not support analyze operation.
         */
        ret_value = IsHDFSTableAnalyze(foreign_table_id);
        stmt->isForeignTables = ret_value;

        if (IsPGFDWTableAnalyze(foreign_table_id)) {
            stmt->isPgFdwForeignTables = true;
        }

#ifdef ENABLE_MOT
        if (IsMOTForeignTable(foreign_table_id)) {
            stmt->isMOTForeignTable = true;
        }
#endif
    }

    return ret_value;
}

#ifdef ENABLE_MULTIPLE_NODES
// we degrade the lock level for VACUUM FULL statement.
// we should wait until all the current transactions finish,
// so make sure the order of executing VACUUM FULL is right.
static void cn_do_vacuum_full_mpp_table(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    Oid relation_id = InvalidOid;
    ExecNodes* exec_nodes = NULL;
    RemoteQueryExecType exec_type = EXEC_ON_DATANODES;

    // step 1: the other CN nodes work;
    if (stmt->relation) {
        relation_id = RangeVarGetRelid(stmt->relation, NoLock, false);
        if (!IsTempTable(relation_id))
            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, true, EXEC_ON_COORDS, false);
    }

    // step 2: myself CN node works;
    // this function will commit the previous transaction:
    // 1. PopActiveSnapshot()
    // 2. CommitTransactionCommand()
    vacuum(stmt, InvalidOid, true, NULL, is_top_level);
    // 3. StartTransactionCommand()
    PushActiveSnapshot(GetTransactionSnapshot());
    // Though we have held lockForBackup in share mode before, but CommitTransaction has release lock
    // So We must hold lockForBackup in share mode in order to prevent global backup
    // during vacuum full. Because 'select pgxc_lock_for_backup' will run on all coordinator,
    // so we can only hold lockForBackup in local coordinator.
    //
    pgxc_lock_for_utility_stmt(NULL, false);
    if (!superuser() && in_logic_cluster()) {
        /*
         * In logic cluster mode, superuser and system DBA can execute Vacuum Full
         * on all nodes; logic cluster users can execute Vacuum Full on its node group;
         * other users can't execute Vacuum Full in DN nodes.
         */
        Oid curr_group_oid = get_current_lcgroup_oid();
        if (OidIsValid(curr_group_oid)) {
            exec_nodes = GetNodeGroupExecNodes(curr_group_oid);
        } else {
            exec_type = EXEC_ON_NONE;
            ereport(NOTICE,
                (errmsg("Vacuum Full do not run in DNs because User \"%s\" don't "
                        "attach to any logic cluster.",
                    GetUserNameFromId(GetUserId()))));
        }
    }

    // step 3: all the DN nodes work;
    query_string = ConstructMesageWithMemInfo(query_string, stmt->memUsage);
    ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, true, exec_type, false, (Node*)stmt);
    FreeExecNodes(&exec_nodes);
    // it's important that fetching statistic info from one datanode
    // must be with a new transaction. because if the followings,
    // step 1: CN xid1 <1000> : we notify all dn to do VACUUM FULL.
    // step 2: DN xid2 <1001> : start transaction;
    //                          Do VACUUM FULL;
    //                          commit transaction;
    // step 3: CN xid1 <1000> : query from one datanode, but xid2=1001 is the futhre event.
    //         so CN get the previous statisttic info instead of the newest.
    // we cannot accept this sutiation, because info is out of date.
    // so that start a new transaction to do fetching from one datanode.
    PopActiveSnapshot();
    CommitTransactionCommand();
    StartTransactionCommand();
}

/*
 * Code below is from ExecUtilityStmtOnNodes(), just difference
 * is step->combine_type, means that don't care any result from DN.
 */
static void send_delta_merge_req(const char* query_string)
{
    RemoteQuery* step = makeNode(RemoteQuery);

    step->combine_type = COMBINE_TYPE_NONE;
    step->exec_nodes = NULL;
    step->sql_statement = pstrdup(query_string);
    step->force_autocommit = true;
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = false;
    ExecRemoteUtility(step);

    pfree_ext(step->sql_statement);
    pfree_ext(step);
}

/*
 * if enable_upgrade_merge_lock_mode is true, block all insert/delete/
 * update on all CNs when deltamerge the same table under ExclusiveLock.
 */
static void delta_merge_with_exclusive_lock(VacuumStmt* stmt)
{
    /* t_thrd.vacuum_cxt.vac_context is needed by get_rel_oids() */
    int i = 0;
    const int retry_times = 3;
    t_thrd.vacuum_cxt.vac_context = AllocSetContextCreate(t_thrd.mem_cxt.portal_mem_cxt,
        "Vacuum Deltamerge",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    List* dfsRels = get_rel_oids(InvalidOid, stmt);

    PopActiveSnapshot();
    CommitTransactionCommand();

    ListCell* lc = NULL;
    foreach (lc, dfsRels) {
        vacuum_object* vo = NULL;
        Relation rel = NULL;
        StringInfo query = NULL;
        char* schema_name = NULL;

        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        vo = (vacuum_object*)lfirst(lc);
        LOCKMODE lockmode = vo->is_tsdb_deltamerge ? ShareUpdateExclusiveLock : AccessExclusiveLock;

        /*
         * try to lock table on local CN with no long wait for three times,
         * if the lock is unavailable, just skip the current relation.
         */
        for (i = 0; i < retry_times; i++) {
            if (ConditionalLockRelationOid(vo->tab_oid, lockmode)) {

                rel = try_relation_open(vo->tab_oid, NoLock);
                break;
            }

            /*
             * except the last time, we sleep for a while(30s, 60s) and then try to
             * lock the relation again.
             */
            if (i + 1 < retry_times)
                sleep((i + 1) * 30);
        }

        /* for deltamerge, if the relation is moved before or is locked, just skip without error report. */
        if (rel == NULL) {
            elog(LOG, "delta merge skip relation %d, because the relation can not be open.", vo->tab_oid);
            PopActiveSnapshot();
            CommitTransactionCommand();
            continue;
        }

        schema_name = get_namespace_name(rel->rd_rel->relnamespace);
        if (schema_name == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_SCHEMA_NAME), errmsg("Invalid schema oid: %u", rel->rd_rel->relnamespace)));
        }

        query = makeStringInfo();
        appendStringInfo(query, "vacuum deltamerge \"%s\".\"%s\"", schema_name, rel->rd_rel->relname.data);
        elog(DEBUG1, "%s under ExclusiveLock mode.", query->data);

        /*
         * "deltamerge" on other remote CNs do nothing except locking table.
         * NOTE: deltamerge on all CNs is in ONE transaction. it will block
         *       insert/delete/update on all CNs.
         */
        ExecUtilityStmtOnNodes(query->data, NULL, false, false, EXEC_ON_COORDS, false);

        /*
         * do real job on all DNs.
         * NOTE: deltamerge on DNs is in local transaction.
         */
        send_delta_merge_req(query->data);

        relation_close(rel, NoLock);

        /*
         * Be carefull to use this function, only in the following condition:
         * 1. the relation can not be in opening state;
         * 2. the pointer of the rel cache can not be referenced by other place;
         */
        RelationForgetRelation(RelationGetRelid(rel));

        pfree_ext(query->data);
        pfree_ext(query);

        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    MemoryContextDelete(t_thrd.vacuum_cxt.vac_context);
    t_thrd.vacuum_cxt.vac_context = NULL;
}
#endif

/*
 * @Description: Check whether only one temp relation for analyze
 * @Return true if analyze one temp relation or false
 */
bool IsAnalyzeTempRel(VacuumStmt* stmt)
{
    bool is_local_tmp_table = false;

    /* in case that t_thrd.vacuum_cxt.vac_context would be invalid */
    if (!MemoryContextIsValid(t_thrd.vacuum_cxt.vac_context))
        t_thrd.vacuum_cxt.vac_context = NULL;

    if (stmt->options & VACOPT_ANALYZE) {
        if (stmt->relation) {
            Oid rel_id = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
            Relation rel = relation_open(rel_id, AccessShareLock);

            if (RelationIsLocalTemp(rel)) {
                is_local_tmp_table = true;
            }

            relation_close(rel, NoLock);
        }
    }
    return is_local_tmp_table;
}

#ifdef ENABLE_MULTIPLE_NODES
static void cn_do_vacuum_mpp_table(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    ExecNodes* exec_nodes = NULL;
    RemoteQueryExecType exec_type = EXEC_ON_DATANODES;
    bool is_vacuum = false;

    if (stmt->relation == NULL && !superuser() && in_logic_cluster()) {
        /*
         * In logic cluster mode, superuser and system DBA can execute Vacuum Full
         * on all nodes; logic cluster users can execute Vacuum on its node group;
         * other users can't execute Vacuum in DN nodes.
         */
        Oid curr_group_oid = get_current_lcgroup_oid();
        if (OidIsValid(curr_group_oid)) {
            exec_nodes = GetNodeGroupExecNodes(curr_group_oid);
        } else {
            exec_type = EXEC_ON_NONE;
            ereport(NOTICE,
                (errmsg("Vacuum do not run in DNs because User \"%s\" don't "
                        "attach to any logic cluster.",
                    GetUserNameFromId(GetUserId()))));
        }
    }

    // Step 0: Notify gtm if it is a vacuum
    is_vacuum = (stmt->options & VACOPT_VACUUM) && (!(stmt->options & VACOPT_FULL));

    // Step 1: Execute query_string on all datanode
    if (stmt->relation == NULL || (!ISMATMAP(stmt->relation->relname) && !ISMLOG(stmt->relation->relname))) {
        ExecUtilityStmtOnNodes(query_string, exec_nodes, sent_to_remote, true, exec_type, false, (Node*)stmt);
    }

    FreeExecNodes(&exec_nodes);

    // We don't use multi transaction for analyze local temp relation
    // But still use multip tratinsactics for andalyze normal relatheions
    //
    if (!IS_ONLY_ANALYZE_TMPTABLE) {
        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    } else {
        CommandCounterIncrement();
    }

    // Step 2: Do local vacuum
    vacuum(stmt, InvalidOid, true, NULL, is_top_level);
}

static List* get_analyzable_matviews(Relation pg_class)
{
    Oid rel_oid;
    TableScanDesc scan;
    HeapTuple tuple;
    ScanKeyData key;
    List* oid_list = NIL;

    ScanKeyInit(&key, Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_MATVIEW));
    scan = tableam_scan_begin(pg_class, SnapshotNow, 1, &key);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class class_form = (Form_pg_class)GETSTRUCT(tuple);

        if (class_form->relkind == RELKIND_MATVIEW) {
            rel_oid = HeapTupleGetOid(tuple);

            if (class_form->relnamespace == CSTORE_NAMESPACE || IsToastNamespace(class_form->relnamespace) ||
                isOtherTempNamespace(class_form->relnamespace) ||
                (isAnyTempNamespace(class_form->relnamespace) && !checkGroup(rel_oid, true))) {
                continue;
            }

            /* ignore system tables, inheritors */
            if (rel_oid < FirstNormalObjectId || IsInheritor(rel_oid)) {
                continue;
            }

            if (!superuser() && in_logic_cluster()) {
                Oid curr_group_oid = get_current_lcgroup_oid();
                Oid table_group_oid = get_pgxc_class_groupoid(rel_oid);
                if (curr_group_oid != table_group_oid)
                    continue;
            }

            if (ISMATMAP(class_form->relname.data) || ISMLOG(class_form->relname.data)) {
                continue;
            }

            oid_list = lappend_oid(oid_list, rel_oid);
        }
    }

    tableam_scan_end(scan);

    return oid_list;
}

/*
 * The SQL command is just "analyze" without table specified, so
 * we must find all the plain tables from pg_class.
 */
static List* get_analyzable_relations(bool is_foreign_tables)
{
    Relation pgclass;
    TableScanDesc scan;
    HeapTuple tuple;
    ScanKeyData key;
    Oid rel_oid;
    List* oid_list = NIL;

    pgclass = heap_open(RelationRelationId, AccessShareLock);
    if (!is_foreign_tables) {
        /* Process all plain relations listed in pg_class */
        ScanKeyInit(&key, Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));
        scan = heap_beginscan(pgclass, SnapshotNow, 1, &key);
    } else {
        ScanKeyInit(&key, Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_FOREIGN_TABLE));
        scan = heap_beginscan(pgclass, SnapshotNow, 1, &key);
    }

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class class_form = (Form_pg_class)GETSTRUCT(tuple);

        if ((!is_foreign_tables && (class_form->relkind == RELKIND_RELATION || class_form->relkind == RELKIND_MATVIEW)) ||
            (is_foreign_tables && (class_form->relkind == RELKIND_FOREIGN_TABLE))) {
            rel_oid = HeapTupleGetOid(tuple);

            /* we should do some restriction for non-hdfs foreign table before do analyze. */
            if (!is_foreign_tables) {
                /* when vacuum/vacuum full/analyze the total database,
                we skip some collect of relations, for example in CSTORE namespace,
                skip other temp namespace, and our own invalid temp namespace. */
                if (class_form->relnamespace == CSTORE_NAMESPACE || IsToastNamespace(class_form->relnamespace) ||
                    isOtherTempNamespace(class_form->relnamespace) ||
                    (isAnyTempNamespace(class_form->relnamespace) && !checkGroup(rel_oid, true))) {
                    continue;
                }

                /* ignore system tables, inheritors */
                if (rel_oid < FirstNormalObjectId || IsInheritor(rel_oid))
                    continue;
            } else {
                /*
                 * we should be sure every foreign table in pg_class is hdfs foreign table
                 * if we do analyze command for hdfs foreign table.
                 */
                if (!IsHDFSTableAnalyze(rel_oid) && !IsPGFDWTableAnalyze(rel_oid))
                    continue;
            }

            if (!superuser() && in_logic_cluster()) {
                Oid curr_group_oid = get_current_lcgroup_oid();
                Oid table_group_oid = get_pgxc_class_groupoid(rel_oid);
                if (curr_group_oid != table_group_oid)
                    continue;
            }

            if ((strncmp(class_form->relname.data, MATMAPNAME, MATMAPLEN) == 0) ||
                (strncmp(class_form->relname.data, MLOGNAME, MLOGLEN) == 0)) {
                continue;
            }

            oid_list = lappend_oid(oid_list, rel_oid);
        }
    }
    heap_endscan(scan);

    /* get matview oids then concat with oid_list */
    List *matview_oids = get_analyzable_matviews(pgclass);
    oid_list = list_concat_unique_oid(oid_list, matview_oids);

    heap_close(pgclass, AccessShareLock);
    return oid_list;
}

static void do_global_analyze_pgfdw_Rel(
    VacuumStmt* stmt, Oid rel_id, const char* query_string, bool has_var, bool sent_to_remote, bool is_top_level)
{
    Assert(!IS_PGXC_DATANODE);

    FdwRoutine* fdw_routine = GetFdwRoutineByRelId(rel_id);
    bool ret_value = false;
    PGFDWTableAnalyze* info = (PGFDWTableAnalyze*)palloc0(sizeof(PGFDWTableAnalyze));

    info->relid = rel_id;

    volatile bool use_own_xacts = true;

    if (use_own_xacts) {
        if (ActiveSnapshotSet())
            PopActiveSnapshot();

        /* matches the StartTransaction in PostgresMain() */
        CommitTransactionCommand();
    }

    if (use_own_xacts) {
        StartTransactionCommand();
        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    if (NULL != fdw_routine->AnalyzeForeignTable) {
        ret_value = fdw_routine->AnalyzeForeignTable(NULL, NULL, NULL, (void*)info, false);
    }

    FetchGlobalPgfdwStatistics(stmt, has_var, info);

    if (use_own_xacts) {
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    if (use_own_xacts) {
        /*
         * This matches the CommitTransaction waiting for us in
         * PostgresMain().
         */
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    if (has_var) {
        char* query_string_with_info = NULL;

        stmt->orgCnNodeNo = PGXCNodeGetNodeIdFromName(g_instance.attr.attr_common.PGXCNodeName, PGXC_NODE_COORDINATOR);
        attatch_global_info(&query_string_with_info, stmt, query_string, has_var, ANALYZENORMAL, rel_id, NULL);

        if (!IsConnFromCoord()) {
            notify_other_cn_get_statistics(query_string_with_info, sent_to_remote);
        }

        if (query_string_with_info != NULL) {
            pfree_ext(query_string_with_info);
            query_string_with_info = NULL;
        }
    }
}

/**
 * @global stats
 * @Description: do analyze for one hdfs rel
 * @in stmt - the statment for analyze or vacuum command
 * @in rel_id - the relation oid for analyze or vacuum
 * @in deltarelid - the relation oid for delta table
 * @in query_string - the query string for analyze or vacuum command
 * @in is_replication - the flag identify the relation which analyze or vacuum is replication or not
 * @in has_var - the flag identify do analyze for one relation or all database
 * @in attnum - how many attribute num of the relation
 * @in sent_to_remote - identify if this statement has been sent to the nodes
 * @in is_top_level - is_top_level should be passed down from ProcessUtility
 * @in global_stats_context - the global stats context which create in caller, we will switch to it when we malloc
 * stadndistinct.
 * @return: void
 *
 * step 1: get estimate total rows from DNs
 * step 2: compute sample rate
 * step 3: get sample rows from DNs
 * step 4: get real total tuples from pg_class in DNs except replication table
 * step 5: get stadndistinct from DN1
 * step 6: compute statistics and update in pg_statistics
 */
static void do_global_analyze_hdfs_rel(VacuumStmt* stmt, Oid rel_id, Oid deltarelid, const char* query_string,
    bool is_replication, bool has_var, int attnum, bool sent_to_remote, bool is_top_level,
    MemoryContext global_stats_context, int* table_num)
{
    VacuumStmt* delta_stmt = NULL;
    char* query_string_with_info = NULL;
    MemoryContext old_context = NULL;
    int one_tuple_size1 = 0;
    int one_tuple_size2 = 0;

    if (!check_analyze_permission(rel_id)) {
        (*table_num)--;
        return;
    }

    /* get one_tuple_size for analyze memory control on cn */
    if (*table_num == 1) {
        Relation rel1 = relation_open(rel_id, AccessShareLock);
        one_tuple_size1 = GetOneTupleSize(stmt, rel1);
        relation_close(rel1, AccessShareLock);

        Relation rel2 = relation_open(deltarelid, AccessShareLock);
        one_tuple_size2 = GetOneTupleSize(stmt, rel2);
        relation_close(rel2, AccessShareLock);
    }

    /*
     * get delta relation information and make new VacuumStmt for
     * get reltuples and dndistinct from dn for delta table.
     */
    Relation delta_rel = relation_open(deltarelid, AccessShareLock);
    delta_stmt = makeNode(VacuumStmt);
    delta_stmt->relation = makeNode(RangeVar);
    delta_stmt->relation->relname = pstrdup(RelationGetRelationName(delta_rel));
    delta_stmt->relation->schemaname = get_namespace_name(CSTORE_NAMESPACE);
    delta_stmt->options = stmt->options;
    delta_stmt->flags = stmt->flags;
    delta_stmt->va_cols = (List*)copyObject(stmt->va_cols);
    relation_close(delta_rel, AccessShareLock);

    /*
     * if it is a REPLICATION table, we set stmt->totalRowCnts = 0, then the sample rate=0, DNs send 0 rows to CN.
     * then CN do not compute stats and just fetch stats from DN1. same for System Relation
     */
    if (is_replication) {
        stmt->nodeNo = 0;
        /* set inital value of totalRowCnts, because we will get real total row count from DN1 for replication later. */
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].totalRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].totalRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].totalRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].topRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].topRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].topRowCnts = 0;
        /* memory released after both main and delta finish */
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].topMemSize =
            DEFAULT_SAMPLE_ROWCNT * (one_tuple_size1 + one_tuple_size2) / 1024;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].topMemSize = 0;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].topMemSize = 0;
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].num_samples = 0;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].num_samples = 0;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].num_samples = 0;
        /* identify the current table is replication. */
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].isReplication = true;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].isReplication = true;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].isReplication = true;
    } else {
        /* Set inital sampleRate, it means CN get estimate total row count from DN If sampleRate is -1. */
        global_stats_set_samplerate(ANALYZEMAIN, stmt, NULL);
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].exec_query = true;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].exec_query = true;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].exec_query = true;
        stmt->sampleTableRequired = false;

        /*
         * Create temp table on coordinator for analyze with relative-estimation or
         * absolute-estimate with debuging.
         */
        analyze_tmptbl_debug_cn(rel_id, InvalidOid, stmt, false);
        analyze_tmptbl_debug_cn(deltarelid, rel_id, stmt, true);

        /* Attatch global info to the query_string for get estimate total rows. */
        attatch_global_info(&query_string_with_info, stmt, query_string, has_var, ANALYZEMAIN, InvalidOid);

        /*
         * step 1: get estimate total rows from DNs
         * we set sampleRate is -1 identify the action.
         * if sampleRate more or equal 0, it means CN should get sample rows from DN.
         */
        DEBUG_START_TIMER;
        (void)ExecRemoteVacuumStmt(stmt, query_string_with_info, sent_to_remote, ARQ_TYPE_TOTALROWCNTS, ANALYZEMAIN);
        DEBUG_STOP_TIMER(
            "Fetch estimate totalRowCount for table: %s, %s", stmt->relation->relname, delta_stmt->relation->relname);
    }

    /* workload client manager */
    if (*table_num > 0) {
        UtilityDesc desc;
        errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
        securec_check(rc, "\0", "\0");
        AdaptMem* mem_usage = &stmt->memUsage;
        bool need_sort = false;

        /* cn row count, no more than DEFAULT_SAMPLE_ROWCNT */
        int cnRowCnts1 = (int)Min(DEFAULT_SAMPLE_ROWCNT, stmt->pstGlobalStatEx[ANALYZEMAIN - 1].totalRowCnts);
        int cnRowCnts2 = (int)Min(DEFAULT_SAMPLE_ROWCNT, stmt->pstGlobalStatEx[ANALYZEDELTA - 1].totalRowCnts);
        /* dn memsize (top) */
        int top_mem_size = stmt->pstGlobalStatEx[ANALYZEMAIN - 1].topMemSize;

        if (stmt->options & VACOPT_FULL) {
            Relation rel = relation_open(rel_id, AccessShareLock);
            if (rel->rd_rel->relhasclusterkey) {
                SetSortMemInfo(&desc,
                    cnRowCnts1 + cnRowCnts2,
                    Max(one_tuple_size1, one_tuple_size2),
                    true,
                    false,
                    u_sess->attr.attr_memory.work_mem);
                need_sort = true;
            }
            relation_close(rel, AccessShareLock);
        }

        /* Get top row count of cn/dn */
        top_mem_size = Max(top_mem_size, (cnRowCnts1 * one_tuple_size1 + cnRowCnts2 * one_tuple_size2) / 1024);
        desc.query_mem[0] = (int)Max(desc.query_mem[0], BIG_MEM_RATIO * top_mem_size);
        /* Adjust assigned query mem if it's too small */
        desc.assigned_mem = Max(Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L), desc.assigned_mem);
        desc.cost = (double)(desc.query_mem[0] / PAGE_SIZE) * u_sess->attr.attr_sql.seq_page_cost;

        if (*table_num > 1) {
            desc.query_mem[0] = Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L);
            /* Adjust assigned query mem if it's too small */
            desc.assigned_mem = Max(Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L), desc.assigned_mem);
            desc.cost = g_instance.cost_cxt.disable_cost;
        }
        if (desc.query_mem[1] == 0)
            desc.query_mem[1] = desc.query_mem[0];

        WLMInitQueryPlan((QueryDesc*)&desc, false);
        dywlm_client_manager((QueryDesc*)&desc, false);
        if (need_sort)
            AdjustIdxMemInfo(mem_usage, &desc);

        /* set 0 for only send to wlm at first time */
        *table_num = 0;
    }

    /*
     * step 2: compute sample rate for normal table.
     * compute sample rate for HDFS table(include dfs/delta/complex table).
     */
    (void)compute_sample_size(stmt, 0, NULL, rel_id, ANALYZEMAIN - 1);
    (void)compute_sample_size(stmt, 0, NULL, deltarelid, ANALYZEDELTA - 1);
    (void)compute_sample_size(stmt, 0, NULL, rel_id, ANALYZECOMPLEX - 1);
    elog(DEBUG1,
        "Step 2: Compute sample rate[%.12f][%.12f][%.12f] for DFS table.",
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].sampleRate,
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].sampleRate,
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].sampleRate);

    /* Attatch global info to the query_string. for get sample. */
    attatch_global_info(&query_string_with_info, stmt, query_string, has_var, ANALYZEMAIN, InvalidOid);
    elog(DEBUG1,
        "Analyzing [%s], oid is [%d], the messege is [%s]",
        stmt->relation->relname,
        rel_id,
        query_string_with_info);

    /*
     * We only send query string to all datanodes in two cases:
     * 1. for replication table;
     * 2. there is no split map of all datanodes for hdfs foreign tables.
     */
    if (stmt->pstGlobalStatEx[ANALYZEMAIN - 1].isReplication) {
        ExecUtilityStmtOnNodes(query_string_with_info, NULL, sent_to_remote, true, EXEC_ON_DATANODES, false);
    } else {
        /* step 3: get sample rows and real total rows from DNs */
        DEBUG_START_TIMER;
        stmt->sampleRows = ExecRemoteVacuumStmt(stmt, query_string_with_info, sent_to_remote, ARQ_TYPE_SAMPLE, ANALYZEMAIN);
        DEBUG_STOP_TIMER(
            "Get sample rows from all DNs for table: %s, %s", stmt->relation->relname, delta_stmt->relation->relname);
    }

    PopActiveSnapshot();
    /*
     * It will swich CurrentMemoryContext to t_thrd.top_mem_cxt after call CommitTransactionCommand(),
     * and swich to u_sess->top_transaction_mem_cxt after call StartTransactionCommand().
     */
    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    /* step 4: get stadndistinct from DN1. */
    /* current memctx is u_sess->top_transaction_mem_cxt, we will malloc memory for stadndistinct, so we must switch to
     * global_stats_context. */
    old_context = MemoryContextSwitchTo(global_stats_context);

    DEBUG_START_TIMER;
    /* get dndistinct from dn1 for delta table. */
    delta_stmt->tableidx = ANALYZEDELTA - 1;
    /* get some statistic info which have collected for delta table. */
    delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].totalRowCnts =
        stmt->pstGlobalStatEx[delta_stmt->tableidx].totalRowCnts;
    delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].eAnalyzeMode = ANALYZEDELTA;
    delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].isReplication =
        stmt->pstGlobalStatEx[delta_stmt->tableidx].isReplication;
    /* set inital value of dndistinct. */
    set_dndistinct_coors(delta_stmt, attnum);

    /* fetch dndistinct from dn1 and copy to stmt using for vacuum later. */
    FetchGlobalStatistics(delta_stmt, deltarelid, stmt->relation, is_replication);
    stmt->pstGlobalStatEx[ANALYZEDELTA - 1].eAnalyzeMode = ANALYZEDELTA;
    stmt->pstGlobalStatEx[delta_stmt->tableidx].totalRowCnts =
        delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].totalRowCnts;
    stmt->pstGlobalStatEx[delta_stmt->tableidx].dndistinct = delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].dndistinct;
    stmt->pstGlobalStatEx[delta_stmt->tableidx].correlations =
        delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].correlations;
    stmt->pstGlobalStatEx[delta_stmt->tableidx].attnum = delta_stmt->pstGlobalStatEx[delta_stmt->tableidx].attnum;

    /* get dndistinct from dn1 for dfs table. */
    stmt->tableidx = ANALYZEMAIN - 1;
    stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode = ANALYZEMAIN;
    set_dndistinct_coors(stmt, attnum);
    /* there is no tuples, we need not go on continue. */
    FetchGlobalStatistics(stmt, rel_id, NULL, is_replication);

    /* get dndistinct from dn1 for complex table. */
    stmt->tableidx = ANALYZECOMPLEX - 1;
    stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode = ANALYZECOMPLEX;
    set_dndistinct_coors(stmt, attnum);
    /* there is no tuples, we need not go on continue. */
    FetchGlobalStatistics(stmt, rel_id, NULL, is_replication);
    DEBUG_STOP_TIMER(
        "Fetch statistics from DN1 for table: %s, %s", stmt->relation->relname, delta_stmt->relation->relname);

    /* malloc sample memory for complex table and copy sample rows from dfs table and delta table. */
    if (!stmt->sampleTableRequired)
        set_complex_sample(stmt);

    (void)MemoryContextSwitchTo(old_context);
    /* step 5: compute statistics and update in pg_statistics. */
    vacuum(stmt, InvalidOid, true, NULL, is_top_level);

    /*
     * for VACUUM ANALYZE table where options=3, vacuum has set use_own_xacts true and already poped ActiveSnapshot,
     * so we don't want to pop again.
     */
    if (ActiveSnapshotSet())
        PopActiveSnapshot();

    CommitTransactionCommand();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (stmt->tmpSampleTblNameList && log_min_messages > DEBUG1) {
        dropSampleTable(get_sample_tblname(ANALYZEMAIN, stmt->tmpSampleTblNameList));
        dropSampleTable(get_sample_tblname(ANALYZEDELTA, stmt->tmpSampleTblNameList));

        /*
         * Drop table is twophase-transaction, if dn fault at this time,
         * current transaction don't be cleanned immadiately. So we should commit the transaction and
         * Start a new transaction.
         */
        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    /* Notify the other CN nodes if do analyze/vacuum one relation and the relation is not temporary. */
    if (has_var && !IsTempTable(rel_id)) {
        notify_other_cn_get_statistics(query_string_with_info, sent_to_remote);
    }

    if (query_string_with_info != NULL) {
        pfree_ext(query_string_with_info);
        query_string_with_info = NULL;
    }
    return;
}

/**
 * @global stats
 * @Description: do analyze for one rel
 * @in stmt - the statment for analyze or vacuum command
 * @in rel_id - the relation oid for analyze or vacuum
 * @in query_string - the query string for analyze or vacuum command
 * @in is_replication - the flag identify the relation which analyze or vacuum is replication or not
 * @in has_var - the flag identify do analyze for one relation or all database
 * @in attnum - how many attribute num of the relation
 * @in sent_to_remote - identify if this statement has been sent to the nodes
 * @in is_top_level - is_top_level should be passed down from ProcessUtility
 * @return: void
 *
 * step 1: get estimate total rows from DNs
 * step 2: compute sample rate
 * step 3: get sample rows from DNs
 * step 4: get real total tuples from pg_class in DNs except replication table
 * step 5: get stadndistinct from DN1
 * step 6: compute statistics and update in pg_statistics
 */
static void do_global_analyze_rel(VacuumStmt* stmt, Oid rel_id, const char* query_string, bool is_replication, bool has_var,
    int attnum, bool sent_to_remote, bool is_top_level, int* table_num)
{
    char* query_string_with_info = NULL;
    char* foreign_tbl_schedul_message = NULL;
    stmt->tableidx = ANALYZENORMAL;
    int one_tuple_size = 0;

    if (!check_analyze_permission(rel_id)) {
        (*table_num)--;
        return;
    }

    /* get one_tuple_size for analyze memory control on cn */
    if (*table_num == 1) {
        Relation rel = relation_open(rel_id, AccessShareLock);
        one_tuple_size = GetOneTupleSize(stmt, rel);
        relation_close(rel, AccessShareLock);
    }

    /*
     * Call scheduler to get datanode which will execute analyze operation
     * and file list.
     */
    if (stmt->isForeignTables && !stmt->isPgFdwForeignTables)
        foreign_tbl_schedul_message = get_scheduling_message(rel_id, stmt);

    /*
     * if it is a REPLICATION table, we set stmt->totalRowCnts = 0, then the sample rate=0, DNs send 0 rows to CN.
     * then CN do not compute stats and just fetch stats from DN1. same for System Relation
     */
    if (is_replication) {
        stmt->nodeNo = stmt->isForeignTables ? stmt->nodeNo : 0;
        /* set inital value of totalRowCnts, because we will get real total row count from DN1 for replication later. */
        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZENORMAL].topRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize = DEFAULT_SAMPLE_ROWCNT * one_tuple_size / 1024;
        stmt->num_samples = 0;
        /* identify the current table is replication. */
        stmt->pstGlobalStatEx[ANALYZENORMAL].isReplication = true;
    } else {
        /* Set inital sampleRate, it means CN get estimate total row count from DN If sampleRate is -1. */
        global_stats_set_samplerate(ANALYZENORMAL, stmt, NULL);
        stmt->pstGlobalStatEx[ANALYZENORMAL].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZENORMAL].exec_query = true;
        stmt->sampleTableRequired = false;

        analyze_tmptbl_debug_cn(rel_id, InvalidOid, stmt, true);

        /* Attatch global info to the query_string for get estimate total rows. */
        attatch_global_info(
            &query_string_with_info, stmt, query_string, has_var, ANALYZENORMAL, rel_id, foreign_tbl_schedul_message);

        /*
         * Step 1: get estimated total rows from DNs
         *
         * We set sampleRate to -1 identify the action.
         * if sampleRate more or equal 0, it means CN should get sample rows from DN.
         */
        DEBUG_START_TIMER;
        (void)ExecRemoteVacuumStmt(stmt, query_string_with_info, sent_to_remote, ARQ_TYPE_TOTALROWCNTS, ANALYZENORMAL);
        DEBUG_STOP_TIMER("Fetch estimate totalRowCount for table: %s", stmt->relation->relname);

        elog(ES_LOGLEVEL,
            "Step 1 > table: %s, estimated total row count: [%.2lf]",
            stmt->relation->relname,
            stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts);

        /* The memory of query_string_with_info can be freed because it is no use. */
        if (query_string_with_info != NULL) {
            pfree_ext(query_string_with_info);
            query_string_with_info = NULL;
        }
    }

    /* workload client manager */
    if (*table_num > 0) {
        UtilityDesc desc;
        errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
        securec_check(rc, "\0", "\0");
        AdaptMem* mem_usage = &stmt->memUsage;
        bool need_sort = false;
        /* cn row count, no more than DEFAULT_SAMPLE_ROWCNT */
        int cn_row_cnts = (int)Min(DEFAULT_SAMPLE_ROWCNT, stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts);
        /* dn mem size (top) */
        int top_mem_size = stmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize;
        bool use_tenant = false;
        int available_mem = 0;
        int max_mem = 0;
        dywlm_client_get_memory_info(&max_mem, &available_mem, &use_tenant);

        if (stmt->options & VACOPT_FULL) {
            Relation rel = relation_open(rel_id, AccessShareLock);
            if (rel->rd_rel->relhasclusterkey) {
                SetSortMemInfo(&desc, cn_row_cnts, one_tuple_size, true, false, u_sess->attr.attr_memory.work_mem);
                need_sort = true;
            }
            /* when rel has index, we give a rough estimation */
            List* index_ids = RelationGetIndexList(rel);
            if (index_ids != NIL) {
                if (desc.assigned_mem == 0) {
                    desc.assigned_mem = max_mem;
                }
                desc.cost = g_instance.cost_cxt.disable_cost;
                desc.query_mem[0] = Max(STATEMENT_MIN_MEM * 1024, desc.query_mem[0]);
                need_sort = true;
                list_free_ext(index_ids);
            }
            relation_close(rel, AccessShareLock);
        }

        /* Get top row count of cn/dn */
        top_mem_size = Max(top_mem_size, cn_row_cnts * one_tuple_size / 1024);
        desc.query_mem[0] = (int)Max(BIG_MEM_RATIO * top_mem_size, desc.query_mem[0]);
        /* Adjust assigned query mem if it's too small */
        desc.assigned_mem = Max(Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L), desc.assigned_mem);
        desc.cost = Max((double)(desc.query_mem[0] / PAGE_SIZE) * u_sess->attr.attr_sql.seq_page_cost, desc.cost);

        if (*table_num > 1) {
            desc.query_mem[0] = Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L);
            /* Adjust assigned query mem if it's too small */
            desc.assigned_mem = Max(Max(desc.query_mem[0], STATEMENT_MIN_MEM * 1024L), desc.assigned_mem);
            desc.cost = g_instance.cost_cxt.disable_cost;
        }
        /* We use assigned variable if query_mem is set */
        if (VALID_QUERY_MEM()) {
            desc.query_mem[0] = Max(desc.query_mem[0], max_mem);
            desc.query_mem[1] = Max(desc.query_mem[1], max_mem);
        }
        if (desc.query_mem[1] == 0)
            desc.query_mem[1] = desc.query_mem[0];

        WLMInitQueryPlan((QueryDesc*)&desc, false);
        dywlm_client_manager((QueryDesc*)&desc, false);
        if (need_sort)
            AdjustIdxMemInfo(mem_usage, &desc);
        /* set 0 for only send to wlm at first time */
        *table_num = 0;
    }

    /* get memory for stadndistinct. */
    set_dndistinct_coors(stmt, attnum);

    /* step 2: compute sample rate for normal table. */
    (void)compute_sample_size(stmt, 0, NULL, rel_id, ANALYZENORMAL);
    elog(DEBUG1,
        "Step 2: Compute sample rate[%.12f] for normal table: %s.",
        stmt->pstGlobalStatEx[0].sampleRate,
        stmt->relation->relname);

    elog(ES_LOGLEVEL,
        "Step 2 > table: %s, compute sample rate [%.12f]",
        stmt->relation->relname,
        stmt->pstGlobalStatEx[ANALYZENORMAL].sampleRate);

    /* Attatch global info to the query_string. for get sample. */
    attatch_global_info(&query_string_with_info, stmt, query_string, has_var, ANALYZENORMAL, rel_id, foreign_tbl_schedul_message);
    elog(DEBUG1,
        "Analyzing [%s], oid is [%d], the messege is [%s]",
        stmt->relation->relname,
        rel_id,
        query_string_with_info);

    /*
     * We only send query string to all datanodes in two cases:
     * 1. for replication table;
     * 2. there is no split map of all datanodes for hdfs foreign tables.
     */
    if (stmt->pstGlobalStatEx[stmt->tableidx].isReplication ||
        (stmt->isForeignTables && (stmt->hdfsforeignMapDnList == NIL))) {
        bool analyze_force_autocommit = IS_ONLY_ANALYZE_TMPTABLE ? false : true;
        ExecUtilityStmtOnNodes(
            query_string_with_info, NULL, sent_to_remote, analyze_force_autocommit, EXEC_ON_DATANODES, false, (Node*)stmt);
    } else {
        DEBUG_START_TIMER;
        /* step 3: get sample rows and real total rows from DNs */
        stmt->sampleRows =
            ExecRemoteVacuumStmt(stmt, query_string_with_info, sent_to_remote, ARQ_TYPE_SAMPLE, ANALYZENORMAL);
        DEBUG_STOP_TIMER("Get sample rows from all DNs for table: %s", stmt->relation->relname);
    }

    if (stmt->isForeignTables && !stmt->isPgFdwForeignTables)
        pfree_ext(foreign_tbl_schedul_message);

    if (!IS_ONLY_ANALYZE_TMPTABLE) {
        if (ActiveSnapshotSet())
            PopActiveSnapshot();

        /*
         * It will switch CurrentMemoryContext to t_thrd.top_mem_cxt after call CommitTransactionCommand(),
         * and switch to u_sess->top_transaction_mem_cxt after call StartTransactionCommand().
         */
        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    } else
        CommandCounterIncrement();

    /* step 4: get stadndistinct from DN1. */
    stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode = ANALYZENORMAL;
    DEBUG_START_TIMER;
    FetchGlobalStatistics(stmt, rel_id, NULL, is_replication);
    DEBUG_STOP_TIMER("Fetch statistics from DN1 for table: %s", stmt->relation->relname);

    /*
     * Both FetchGlobalStatistics and vacuum refreshes relpages.
     * To prevent inconsistent update, we add CommandCounterIncrement in between.
     * Refactor it if necessary.
     */
    CommandCounterIncrement();

    /* step 5: compute statistics and update in pg_statistics. */
    vacuum(stmt, InvalidOid, true, NULL, is_top_level);

    if (!IS_ONLY_ANALYZE_TMPTABLE) {
        /*
         * for VACUUM ANALYZE table where options=3, vacuum has set use_own_xacts
         * true and already poped ActiveSnapshot, so we don't want to pop again.
         */
        if (ActiveSnapshotSet())
            PopActiveSnapshot();

        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    } else
        CommandCounterIncrement();

    if (stmt->tmpSampleTblNameList && log_min_messages > DEBUG1) {
        dropSampleTable(get_sample_tblname(ANALYZENORMAL, stmt->tmpSampleTblNameList));

        /*
         * Drop table is twophase-transaction, if dn fault at this time,
         * current transaction don't be cleanned immadiately. So we should commit the transaction and
         * Start a new transaction.
         */
        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    }

    /* Notify the other CN nodes if do analyze/vacuum one relation and the relation is not temporary. */
    if (has_var && !IsTempTable(rel_id)) {
        notify_other_cn_get_statistics(query_string_with_info, sent_to_remote);
    }

    if (query_string_with_info != NULL) {
        pfree_ext(query_string_with_info);
        query_string_with_info = NULL;
    }
    
    /*
     * Other CNs have already gotten statistics. Now, We can resume the time counter which was stoped before
     * update statistics on local CN.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsAutoVacuumWorkerProcess())
        resume_sig_alarm(true);

    return;
}

/* Process analyze command from gsql in CN. */
static void do_global_analyze_mpp_table(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    List* oid_list = NIL;
    ListCell* cur = NULL;
    Oid rel_oid = InvalidOid;
    bool has_var = false;
    char* query_string_with_info = NULL;
    MemoryContext old_context = NULL;
    MemoryContext global_stats_context = NULL;

    PgxcNodeGetOids(NULL, NULL, NULL, (int*)&stmt->DnCnt, false);

    /* Set flag has_var which identify do analyze for one relation or all database. */
    if (stmt->relation == NULL) {
        has_var = false;
        oid_list = get_analyzable_relations(stmt->isForeignTables);
    } else {
        rel_oid = RangeVarGetRelid(stmt->relation, NoLock, false);

        /* Do not analyze matmap or mlog table. */
        if (ISMATMAP(stmt->relation->relname) || ISMLOG(stmt->relation->relname)) {
            ereport(WARNING,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support Analyze Matmap or Mlog table.")));
        } else {
            oid_list = lappend_oid(oid_list, rel_oid);
        }

        has_var = true;
    }

    /* Create new memcontext for analyze/vacuum. */
    global_stats_context = AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,
        "global_stats_context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /*
     * Switch to global_stats_context because it only used to get sample from all datanodes
     * and we want to put this memory into this memory context.
     */
    old_context = MemoryContextSwitchTo(global_stats_context);

    /* set table_num for wlm */
    int table_num = 0;
    if (IS_PGXC_COORDINATOR && ENABLE_WORKLOAD_CONTROL && !IsAnyAutoVacuumProcess())
        table_num = list_length(oid_list);

    /* Loop to process each selected relation. */
    foreach (cur, oid_list) {
        /*
         * Check if the table is REPLICATION for global analyze,
         * if it is a REPLICATION table, just fetch stats from DN1
         */
        int attnum = 0;
        bool is_replication = false;
        Oid rel_id = lfirst_oid(cur);
        DistributeBy* distributeby = NULL;
        Relation rel = try_relation_open(rel_id, AccessShareLock);
        if (!rel) {
            table_num--;
            elog(DEBUG1, "Skip analyze table for cannot get AccessShareLock");
            continue;
        }

        if (RelationIsView(rel)) {
            elog(LOG, "View is an unanalyzable object");
            relation_close(rel, AccessShareLock);
            continue;
        }

        if (RelationIsContquery(rel)) {
            elog(LOG, "Contquery is an unanalyzable object");
            relation_close(rel, AccessShareLock);
            continue;
        }

        if (RelationIsSequnce(rel)) {
            elog(LOG, "Sequnce is an unanalyzable object");
            relation_close(rel, AccessShareLock);
            continue;
        }

        attnum = rel->rd_att->natts;
        /* No need to send analyze/vacuum command to dn for non-ordinary table and non-toast table. */
        if (rel_id < FirstNormalObjectId ||
            (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
                rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE && rel->rd_rel->relkind != RELKIND_MATVIEW)) {
            /* workload client manager */
            if (table_num > 0) {
                UtilityDesc desc;
                errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
                securec_check(rc, "\0", "\0");

                if (table_num == 1) {
                    int one_tuple_size = GetOneTupleSize(stmt, rel);
                    desc.query_mem[0] = (int)(BIG_MEM_RATIO * DEFAULT_SAMPLE_ROWCNT * one_tuple_size / 1024L);
                    desc.cost = (double)(desc.query_mem[0] / PAGE_SIZE) * u_sess->attr.attr_sql.seq_page_cost;
                } else {
                    /* not use work_mem for analyze all database, because it may be different in cn/dn */
                    desc.query_mem[0] = STATEMENT_MIN_MEM * 1024L;
                    desc.cost = g_instance.cost_cxt.disable_cost;
                }

                relation_close(rel, AccessShareLock);
                desc.query_mem[1] = desc.query_mem[0];
                WLMInitQueryPlan((QueryDesc*)&desc, false);
                dywlm_client_manager((QueryDesc*)&desc, false);
                /* set 0 for only send to wlm at first time */
                table_num = 0;
            } else
                relation_close(rel, AccessShareLock);

            stmt->tableidx = ANALYZENORMAL;
            set_dndistinct_coors(stmt, attnum);

            /* All the coordinator and datanodes work */
            ExecUtilityStmtOnNodes(query_string, NULL, sent_to_remote, true, EXEC_ON_ALL_NODES, false, (Node*)stmt);

            vacuum(stmt, InvalidOid, true, NULL, is_top_level);
            continue;
        }

        /* Check the relation which analyze or vacuum is replication or not.  */
        if (!IsSystemRelation(rel)) {
            distributeby = getTableDistribution(rel_id);
            stmt->disttype = distributeby->disttype;
            is_replication = (distributeby->disttype == DISTTYPE_REPLICATION);
        }

        /* It should get relname and schemaname for every relation if do analyze for all database */
        if (!has_var) {
            stmt->relation = makeNode(RangeVar);
            stmt->relation->relname = pstrdup(RelationGetRelationName(rel));
        }

        PG_TRY();
        {
            stmt->relation->schemaname = get_namespace_name(RelationGetNamespace(rel));

            /* do analyze for HDFS table. */
            if (!(stmt->isForeignTables && IsHDFSTableAnalyze(rel_id)) && RelationIsDfsStore(rel)) {
                Oid deltaRelId = rel->rd_rel->reldeltarelid;

                relation_close(rel, AccessShareLock);
                do_global_analyze_hdfs_rel(stmt,
                    rel_id,
                    deltaRelId,
                    query_string,
                    is_replication,
                    has_var,
                    attnum,
                    sent_to_remote,
                    is_top_level,
                    global_stats_context,
                    &table_num);
            } else if (IsPGFDWTableAnalyze(rel_id)) {
                relation_close(rel, AccessShareLock);
                /* do analyze for openGauss foreign table. */
                do_global_analyze_pgfdw_Rel(stmt, rel_id, query_string, has_var, sent_to_remote, is_top_level);
            } else {
                /* do analyze for one relation of normal or hdfs foreign table. */
                relation_close(rel, AccessShareLock);
                do_global_analyze_rel(
                    stmt, rel_id, query_string, is_replication, has_var, attnum, sent_to_remote, is_top_level, &table_num);
            }

            stmt->relation->schemaname = NULL;
        }
        PG_CATCH();
        {
            stmt->relation->schemaname = NULL;
            PG_RE_THROW();
        }
        PG_END_TRY();

        /*
         * Analyzing a big table or analyzing database may consume a lot of memeory,
         * so we reset the memory after analyze each relation in order to avoid memory increase.
         */
        MemoryContextReset(global_stats_context);

        /*
         * We have finished analyze for one relation, so we should switch to global_stats_context
         * in order to do analyze for next relation.
         */
        (void)MemoryContextSwitchTo(global_stats_context);
        stmt->tmpSampleTblNameList = NIL;
    }

    /* Notify the other CN get statistic if analyze for all database. */
    if (!has_var) {
        /*
         * We should construct the new query string include the PGXCNodeId of the current CN,
         * in order to tell other CN get statistic, because other CN need to know which CN is orignal.
         * the AnalyzeMode is no use because it will send analyze command with no table name to other CN.
         */
        attatch_global_info(&query_string_with_info, stmt, query_string, true, ANALYZENORMAL, InvalidOid);
        notify_other_cn_get_statistics(query_string_with_info, sent_to_remote);

        if (query_string_with_info != NULL) {
            pfree_ext(query_string_with_info);
            query_string_with_info = NULL;
        }
    }

    (void)MemoryContextSwitchTo(old_context);
    MemoryContextDelete(global_stats_context);
}

/*
 * do_vacuum_mpp_table_local_cn
 *
 * @Description: do vacuum/analyze where come from client in local coordinator.
 * @in stmt - the statment for analyze or vacuum command
 * @in query_string - the query string for analyze or vacuum command
 * @in is_top_level - true if executing a "top level" (interactively issued) command
 * @in sent_to_remote - identify if this statement has been sent to the nodes
 */
static void do_vacuum_mpp_table_local_cn(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    /* clean empty hdfs directories of value partition just on main CN */
    if (stmt->options & VACOPT_HDFSDIRECTORY) {
        vacuum(stmt, InvalidOid, true, NULL, is_top_level);
        return;
    }

    /*
     * Now VACUUM FULL statement has different executing order from
     * normal VACUUM statement. Degrading lock level results in this different.
     */
    if (stmt->options & VACOPT_VERIFY) {
        if (stmt->relation != NULL) {
            DoGlobalVerifyMppTable(stmt, query_string, sent_to_remote);
        } else {
            DoGlobalVerifyDatabase(stmt, query_string, sent_to_remote);
        }
    } else if (stmt->options & VACOPT_ANALYZE)
        do_global_analyze_mpp_table(stmt, query_string, is_top_level, sent_to_remote);
    else if (stmt->options & VACOPT_FULL)
        cn_do_vacuum_full_mpp_table(stmt, query_string, is_top_level, sent_to_remote);
    else if (stmt->options & VACOPT_MERGE)
        delta_merge_with_exclusive_lock(stmt);
    else
        cn_do_vacuum_mpp_table(stmt, query_string, is_top_level, sent_to_remote);
}
#endif

/*
 * DoVacuumMppTableOtherCN
 *
 * @Description: do vacuum/analyze where come from original cn in other coordinator.
 * @in stmt - the statment for analyze or vacuum command
 * @in query_string - the query string for analyze or vacuum command
 * @in is_top_level - true if executing a "top level" (interactively issued) command
 * @in sent_to_remote - identify if this statement has been sent to the nodes
 */
static void do_vacuum_mpp_table_other_node(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    /* clean empty hdfs directories of value partition just on "main" CN */
    if (stmt->options & VACOPT_HDFSDIRECTORY)
        return;

    /* Do deltamerge on other remote CNs. */
    if (stmt->options & VACOPT_MERGE) {
#ifdef ENABLE_MULTIPLE_NODES
        if (Tsdb::IsDeltaMergeStmt(stmt)) {
            if (!g_instance.attr.attr_common.enable_tsdb) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOG),
                    errmsg("Can't DoDeltaMerge when 'enable_tsdb' is off"),
                    errdetail("When the guc is off, it is forbidden to DoDeltaMerge"),
                    errcause("Unexpected error"),
                    erraction("Turn on the 'enable_tsdb'."),
                    errmodule(MOD_VACUUM)));
            }
            Tsdb::DoDeltaMerge(stmt->relation);
        } else {
            begin_delta_merge(stmt);
        }
#else   /* ENABLE_MULTIPLE_NODES */
        begin_delta_merge(stmt);
#endif  /* ENABLE_MULTIPLE_NODES */
        return;
    }
    if (stmt->options & VACOPT_VERIFY) {
        DoVerifyTableOtherNode(stmt, sent_to_remote);
        return;
    }

    /* Some coordinator node told me to fetch local table/tables statistics information. */
    if (IS_PGXC_COORDINATOR && (stmt->options & VACOPT_ANALYZE))
        FetchGlobalStatistics(stmt, InvalidOid, NULL);

    /* If we only need to estimate rows, vacuum operation should be skipped */
    if (NEED_EST_TOTAL_ROWS_DN(stmt))
        stmt->options &= ~(VACOPT_VACUUM | VACOPT_FULL | VACOPT_FREEZE);

    if (!stmt->isPgFdwForeignTables) {
        vacuum(stmt, InvalidOid, true, NULL, is_top_level);
    }
}

void ClearVacuumStmt(VacuumStmt* stmt)
{
    if (stmt->relation && (stmt->options & VACOPT_ANALYZE)) {
        // The memory context of schemaname has been reset,
        // so there is no memory leak here
        //
        stmt->relation->schemaname = NULL;
    }
}

void ClearCreateSeqStmtUUID(CreateSeqStmt* stmt)
{
    stmt->uuid = INVALIDSEQUUID;
}

void ClearCreateStmtUUIDS(CreateStmt* stmt)
{
    stmt->uuids = NIL;
}

/*
 * @hdfs
 * DoVacuumMappTable
 *
 * We capsule orignal VacuumStmt processing code in this function.
 * Processe MPP local table/tables here.
 */
void DoVacuumMppTable(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote)
{
    elog_node_display(ES_LOGLEVEL, "[DoVacuumMppTable]", stmt->va_cols, true);

    char* cmdname = NULL;

    if (stmt->options & VACOPT_MERGE) {
        cmdname = "DELTA MERGE";
    } else if (stmt->options & VACOPT_VACUUM) {
        cmdname = "VACUUM";
    } else if (stmt->options & VACOPT_VERIFY) {
        cmdname = "VERIFY";
    } else {
        cmdname = "ANALYZE";
    }

    /* It is no means to vacuum a foreign table */
    if (stmt->relation != NULL && (stmt->isForeignTables || stmt->isPgFdwForeignTables) 
        && (stmt->options & VACOPT_VACUUM)) {
        ereport(WARNING, (errmsg("skipping \"%s\" --- cannot vacuum a foreign table", stmt->relation->relname)));
        return;
    }

    /* Template0 is not allowed to vacuum. Keep it unchanged */
    if (stmt->relation != NULL && stmt->relation->catalogname != NULL &&
        strcmp(stmt->relation->catalogname, "template0") == 0) {
        ereport(WARNING,
            (errmsg("skipping \"%s\" --- cannot vacuum database template0",
            stmt->relation->catalogname)));
        return;
    }

    if (stmt->relation != NULL && (stmt->options & VACOPT_FULL)){
        Oid relId = RangeVarGetRelid(stmt->relation, NoLock, false);
        Relation targRel = heap_open(relId, AccessShareLock);
        bool isSegmentTable = targRel->storage_type == SEGMENT_PAGE;
        heap_close(targRel, AccessShareLock);
        if (isSegmentTable) {
            ereport(INFO, (errmsg("skipping segment table \"%s\" --- please use gs_space_shrink "
                "to recycle segment space.", stmt->relation->relname)));
            return;
        }
    }

    /* we choose to allow this during "read only" transactions */
    PreventCommandDuringRecovery(cmdname);

#ifdef PGXC
    /*
     * We have to run the command on nodes before Coordinator
     * because vacuum() pops active snapshot and we can not
     * send it to nodes.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
#ifdef ENABLE_MULTIPLE_NODES
        // Analyze only temp relation can run inside a transaction block
        // other commands can't be allowed in DoVacuumMppTable
        stmt->isAnalyzeTmpTable = IsAnalyzeTempRel(stmt);
        if ((!IS_ONLY_ANALYZE_TMPTABLE) && !Tsdb::IsDeltaMergeStmt(stmt)) {
            PreventTransactionChain(is_top_level, cmdname);
            /*
             * With jdbc terminal, the cid of vacuum statement should be zero;
             * Cause CN will set the vacuum statement blockstate to TBLOCK_STARTED(running single-query transaction)
             */
            if (GetCurrentCommandId(false) > 0 && ((stmt)->options & VACOPT_VACUUM))
                ereport(ERROR,
                    (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        errmsg("%s can not run inside a transaction block", cmdname)));
        }

        /* Do vacuum/analyze in local CN. */
        do_vacuum_mpp_table_local_cn(stmt, query_string, is_top_level, sent_to_remote);
#endif
    } else {
        /* For single node case, need to judge temp rel as it never did before */
        if (IS_SINGLE_NODE) {
            stmt->isAnalyzeTmpTable = IsAnalyzeTempRel(stmt);
        }
        /* For single node case, keep the same behavior as PGXC */
        if (IS_SINGLE_NODE && !IS_ONLY_ANALYZE_TMPTABLE) {
            PreventTransactionChain(is_top_level, cmdname);
            if (GetCurrentCommandId(false) > 0 && ((stmt)->options & VACOPT_VACUUM))
                ereport(ERROR,
                    (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                        errmsg("%s can not run inside a transaction block", cmdname)));
        }

        /* Do vacuum/analyze on other node include CN and DN. */
        do_vacuum_mpp_table_other_node(stmt, query_string, is_top_level, sent_to_remote);

        /*
         * There is plan cache in store procedure, some data structure
         * must be rest, because memcontext has been reset. We can't use
         * those pointer when get stmt from plan cache.
         */
        ClearVacuumStmt(stmt);
    }
#endif
}

/*
 * @hdfs
 * IsSupportForeignTableAnalyze
 *
 * Use foreign_table_id to judge if a foreign table support analysis operation.
 */
bool IsHDFSTableAnalyze(Oid foreign_table_id)
{
    HeapTuple tuple = NULL;
    Form_pg_class class_form = NULL;
    bool ret_value = false;
    /*
     * Find the tuple in pg_class, using syscache for the lookup.
     */
    tuple = SearchSysCacheCopyWithLogLevel(RELOID, ObjectIdGetDatum(foreign_table_id), LOG);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", foreign_table_id)));
    }
    class_form = (Form_pg_class)GETSTRUCT(tuple);

    /*
     * Judge if the relation is a foreign table
     */
    if (RELKIND_FOREIGN_TABLE != class_form->relkind) {
        ret_value = false;
    } else {
        /*
         * Judge if a foreign supports analysis opertation. If a foreign table
         * implements AnalyzeForeignTable, we think it supports foreign table
         * analyze operation.
         */
        FdwRoutine* table_fdw_routine = GetFdwRoutineByRelId(foreign_table_id);
        if (table_fdw_routine->AnalyzeForeignTable == NULL) {
            /*
             * Foreign table does not support analyze operation.
             */
            ereport(WARNING, (errmsg("The operation foreign table doesn't support analyze command.")));
            ret_value = false;
        } else {
            if (isObsOrHdfsTableFormTblOid(foreign_table_id) || IS_OBS_CSV_TXT_FOREIGN_TABLE(foreign_table_id))
                ret_value = true;
            else if (IS_LOGFDW_FOREIGN_TABLE(foreign_table_id)) {
                ret_value = true;
            } else
                ret_value = false;
        }
    }

    return ret_value;
}

/*
 * @pgfdw
 * IsPGFDWTableAnalyze
 *
 * Use foreignTableId to judge if is a gc_fdw foreign table support analysis operation.
 */
bool IsPGFDWTableAnalyze(Oid foreignTableId)
{
    HeapTuple tuple = NULL;
    Form_pg_class classForm = NULL;

    /*
     * Find the tuple in pg_class, using syscache for the lookup.
     */
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(foreignTableId));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", foreignTableId)));
    }
    classForm = (Form_pg_class)GETSTRUCT(tuple);

    /*
     * Judge if the relation is a foreign table
     */
    if (RELKIND_FOREIGN_TABLE == classForm->relkind && IS_POSTGRESFDW_FOREIGN_TABLE(foreignTableId)) {
        return true;
    }

    return false;
}

#ifdef ENABLE_MOT
bool IsMOTForeignTable(Oid foreignTableId)
{
    HeapTuple tuple = NULL;
    Form_pg_class classForm = NULL;

    /*
     * Find the tuple in pg_class, using syscache for the lookup.
     */
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(foreignTableId));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                 errmsg("cache lookup failed for relation %u", foreignTableId)));
    }

    classForm = (Form_pg_class) GETSTRUCT(tuple);

    /*
     * Judge if the relation is a foreign table
     */
    if (RELKIND_FOREIGN_TABLE == classForm->relkind && isMOTFromTblOid(foreignTableId)) {
        return true;
    }

    return false;
}
#endif

/*
 * @global stats
 * @Description: set sample rate in local which compute by total row count.
 * @in e_analyze_mode - identify which type the table, normal talbe/dfs table/delta table
 * @in stmt - the statment for analyze or vacuum command
 * @in newSampleRate - the estimate total row count which get from dn
 * @return: void
 */
void global_stats_set_samplerate(AnalyzeMode e_analyze_mode, VacuumStmt* stmt, const double* newSampleRate)
{
    /* Set inital sampleRate, it means CN get estimate total row count from DN If sampleRate is -1. */
    if (e_analyze_mode == ANALYZENORMAL) {
        stmt->pstGlobalStatEx[0].sampleRate = ((NULL == newSampleRate) ? GET_ESTTOTALROWCNTS_FLAG : newSampleRate[0]);
    } else {
        for (int i = 0; i < ANALYZE_MODE_MAX_NUM - 1; i++) {
            stmt->pstGlobalStatEx[i].sampleRate =
                ((NULL == newSampleRate) ? GET_ESTTOTALROWCNTS_FLAG : newSampleRate[i]);
        }
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @global stats
 * @Description: initial stadndistinct and coorelations which using for get the value from dn1 and set in local's
 * pg_statistic.
 * @in stmt - the statment for analyze or vacuum command
 * @in attnum - the num of attribute for current table which do analyze
 * @return: void
 */
static void set_dndistinct_coors(VacuumStmt* stmt, int attnum)
{
    stmt->pstGlobalStatEx[stmt->tableidx].attnum = attnum;
    stmt->pstGlobalStatEx[stmt->tableidx].dndistinct = (double*)palloc0(sizeof(double) * attnum);
    stmt->pstGlobalStatEx[stmt->tableidx].correlations = (double*)palloc0(sizeof(double) * attnum);

    for (int i = 0; i < attnum; i++) {
        stmt->pstGlobalStatEx[stmt->tableidx].dndistinct[i] = 1;
        stmt->pstGlobalStatEx[stmt->tableidx].correlations[i] = 1;
    }
}
#endif

/*
 * @global stats
 * @Description: for a relation to be analyzed, check the permission. If no permisision, skip it.
 * @in rel_id - oid of the relation to be checked
 * @return: whether has permission
 */
bool check_analyze_permission(Oid rel_id)
{

    Relation rel = try_relation_open(rel_id, ShareUpdateExclusiveLock);

    /* if the rel is invalid, just return false and outter level will skip it */
    if (!rel) {
        elog(DEBUG1, "Skip analyze table for cannot get ShareUpdateExclusiveLock");
        return false;
    }

    /*
     * If rel is in read only mode(none redistribution scenario), we skip analyze
     * the relation.
     */
    if (!u_sess->attr.attr_sql.enable_cluster_resize && RelationInClusterResizingReadOnly(rel)) {
        ereport(WARNING,
            (errmsg(
                "skipping \"%s\" --- only none read-only mode can do analyze command", RelationGetRelationName(rel))));
        relation_close(rel, ShareUpdateExclusiveLock);
        return false;
    }

    AclResult aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(RelationGetRelid(rel), GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !rel->rd_rel->relisshared) ||
                (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        /*
         * we may have checked the first two situations before, just double check to
         * make sure no miss in every cases.
         */
        if (rel->rd_rel->relisshared)
            ereport(WARNING,
                (errmsg("skipping \"%s\" --- only system admin can analyze it", RelationGetRelationName(rel))));
        else if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
            ereport(WARNING,
                (errmsg("skipping \"%s\" --- only system admin or database owner can analyze it",
                    RelationGetRelationName(rel))));
        else
            ereport(WARNING,
                (errmsg(
                    "skipping \"%s\" --- only table or database owner can analyze it", RelationGetRelationName(rel))));

        relation_close(rel, ShareUpdateExclusiveLock);
        return false;
    }

    relation_close(rel, ShareUpdateExclusiveLock);
    return true;
}

/*
 * @NodeGroup Support
 * @Description: Find ExecNodes for given rel_id(for index case) we return the base
 *		table's execnodes
 *
 * @Return: return the rel_id's execnodes
 */
ExecNodes* RelidGetExecNodes(Oid rel_id, bool isutility)
{
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    int nmembers = 0;
    Oid* members = NULL;
    char relkind = get_rel_relkind(rel_id);
    Oid group_oid = InvalidOid;

    if (rel_id > FirstNormalObjectId) {
        if (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW) {
            /* For relation, go to pgxc_class to fetch group list directly */
            nmembers = get_pgxc_classnodes(rel_id, &members);

            /* Binding group_oid for none system table */
            group_oid = get_pgxc_class_groupoid(rel_id);
        } else if (relkind == RELKIND_INDEX || relkind == RELKIND_GLOBAL_INDEX) {
            /*
             * For index, there is enry in pgxc_class, so we first get index's
             * base rel_id and then fetch group list from pgxc_class
             */
            Oid base_relid = IndexGetRelation(rel_id, false);
            nmembers = get_pgxc_classnodes(base_relid, &members);

            /* Binding group_oid for none system table */
            group_oid = get_pgxc_class_groupoid(base_relid);
        } else if (relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_STREAM) {
            if (in_logic_cluster() && is_pgxc_class_table(rel_id)) {
                /* For relation, go to pgxc_class to fetch group list directly */
                nmembers = get_pgxc_classnodes(rel_id, &members);

                /* Binding group_oid for none system table */
                group_oid = get_pgxc_class_groupoid(rel_id);
            }
        }
    } else {
        group_oid = ng_get_installation_group_oid();
        nmembers = get_pgxc_groupmembers(group_oid, &members);
        AssertEreport(nmembers > 0 && members != NULL, MOD_EXECUTOR, "Invalid group member value");
    }

    /*
     * Notice!!
     * In cluster resizing stage we need special processing logics in utility statement.
     *
     * So, as normal, when target node group's status is marked as 'installation' or
     * 'redistribution', we have to issue a full-DN request.
     */
    if (isutility && OidIsValid(group_oid)) {
        char* group_name = get_pgxc_groupname(group_oid);
        if (need_full_dn_execution(group_name)) {
            exec_nodes->nodeList = GetAllDataNodes();
        } else {
            if (IsLogicClusterRedistributed(group_name) &&
                ((relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_STREAM)
                || !RelationIsDeleteDeltaTable(get_rel_name(rel_id)))) {
                int dst_nmembers = 0;
                Oid* dst_members = NULL;

                Oid destgroup_oid = PgxcGroupGetRedistDestGroupOid();
                if (OidIsValid(destgroup_oid))
                    dst_nmembers = get_pgxc_groupmembers(destgroup_oid, &dst_members);
                if (dst_nmembers > nmembers) {
                    nmembers = dst_nmembers;
                    pfree_ext(members);
                    members = dst_members;
                } else if (dst_members != NULL) {
                    pfree_ext(dst_members);
                }
            }

            /* Creating executing-node list */
            exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
        }
    } else {
        /* Creating executing-node list */
        exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
    }

    Distribution* distribution = ng_convert_to_distribution(exec_nodes->nodeList);
    distribution->group_oid = group_oid;
    ng_set_distribution(&exec_nodes->distribution, distribution);

    pfree_ext(members);

    return exec_nodes;
}

/*
 * @NodeGroup Support
 * @Description: Determine the list objects is in same node group
 *
 * @Return: 'true' in same group, 'false' in different group
 */
bool ObjectsInSameNodeGroup(List* objects, NodeTag stmttype)
{
    ListCell* cell = NULL;
    Oid first_ngroup_oid = InvalidOid;
    Oid table_oid = InvalidOid;
    Oid group_oid = InvalidOid;

    if (objects == NIL || list_length(objects) == 1) {
        return true;
    }

    /* Scanning dropping list to error-out un-allowed cases */
    foreach (cell, objects) {
        RangeVar* rel = NULL;
        if (stmttype == T_DropStmt) {
            /* In DROP Table statements, we dereference list as name list */
            rel = makeRangeVarFromNameList((List*)lfirst(cell));
        } else if (stmttype == T_TruncateStmt) {
            /* In TRUNCATE table statements, we dereference list as range var */
            rel = (RangeVar*)lfirst(cell);
        }

        table_oid = RangeVarGetRelid(rel, NoLock, true);
        /* table_oid is InvalidOid means it is dropping a none-exists table */
        if (table_oid == InvalidOid) {
            continue;
        }

        /*
         * Only check relation and materialized view, for index, view is not associated with
         * nodegroup they belongs to its base table
         */
        char relkind = get_rel_relkind(table_oid);
        if ((relkind != RELKIND_RELATION) && (relkind != RELKIND_MATVIEW)) {
            continue;
        }

        /* Fetching group_oid for given relation */
        group_oid = get_pgxc_class_groupoid(table_oid);
        AssertEreport(group_oid != InvalidOid, MOD_EXECUTOR, "group OID is invalid");

        if (first_ngroup_oid == InvalidOid) {
            first_ngroup_oid = group_oid;

            /*
             * In 1st round of dropping list iteration we don't have
             * to process following verification.
             */
            continue;
        }

        if (first_ngroup_oid != group_oid) {
            return false;
        }
    }

    return true;
}

/*
 * @NodeGroup Support
 * @Description: Return correct ExecNodes with given parser node
 *
 * @Return: execnodes of target table in ReIndex, Grant, Lock statements
 */
static ExecNodes* assign_utility_stmt_exec_nodes(Node* parse_tree)
{
    ExecNodes* nodes = NULL;
    Oid rel_id = InvalidOid;

    AssertEreport(parse_tree, MOD_EXECUTOR, "parser tree is NULL");
    AssertEreport(IS_PGXC_COORDINATOR && !IsConnFromCoord(), MOD_EXECUTOR, "the node is not a CN node");

    /* Do special processing for nodegroup support */
    switch (nodeTag(parse_tree)) {
        case T_ReindexStmt: {
            ReindexStmt* stmt = (ReindexStmt*)parse_tree;
            if (stmt->relation) {
                rel_id = RangeVarGetRelid(stmt->relation, NoLock, false);
                nodes = RelidGetExecNodes(rel_id);
            }
            break;
        }
        case T_GrantStmt: {
            GrantStmt* stmt = (GrantStmt*)parse_tree;
            if (stmt->objects && stmt->objtype == ACL_OBJECT_RELATION && stmt->targtype == ACL_TARGET_OBJECT) {
                RangeVar* relvar = (RangeVar*)list_nth(stmt->objects, 0);
                Oid relation_id = RangeVarGetRelid(relvar, NoLock, false);
                nodes = RelidGetExecNodes(relation_id);
            }
            break;
        }
        case T_ClusterStmt: {
            ClusterStmt* stmt = (ClusterStmt*)parse_tree;
            if (stmt->relation) {
                Oid relation_id = RangeVarGetRelid(stmt->relation, NoLock, false);
                nodes = RelidGetExecNodes(relation_id);
            }
            break;
        }
        case T_LockStmt: {
            LockStmt* stmt = (LockStmt*)parse_tree;
            if (stmt->relations) {
                RangeVar* relvar = (RangeVar*)list_nth(stmt->relations, 0);
                Oid relation_id = RangeVarGetRelid(relvar, NoLock, false);
                nodes = RelidGetExecNodes(relation_id);
            }
            break;
        }
        case T_VacuumStmt: {
            VacuumStmt* stmt = (VacuumStmt*)parse_tree;
            if (stmt->relation) {
                Oid relation_id = RangeVarGetRelid(stmt->relation, NoLock, false);
                nodes = RelidGetExecNodes(relation_id);
            } else if (stmt->options & VACOPT_VERIFY) {
                nodes = RelidGetExecNodes(stmt->curVerifyRel);
            }
            break;
        }
        case T_RenameStmt: {
            RenameStmt* stmt = (RenameStmt*)parse_tree;
            if (stmt->relation) {
                Oid relation_id = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
                nodes = RelidGetExecNodes(relation_id);
            } else if (stmt->renameType == OBJECT_FUNCTION) {
                Oid funcid = LookupFuncNameTypeNames(stmt->object, stmt->objarg, false);

                nodes = GetFunctionNodes(funcid);
            }
            break;
        }
        case T_AlterObjectSchemaStmt: {
            AlterObjectSchemaStmt* stmt = (AlterObjectSchemaStmt*)parse_tree;
            if (stmt->relation) {
                rel_id = RangeVarGetRelid(stmt->relation, NoLock, true);
                nodes = RelidGetExecNodes(rel_id);
            } else if (stmt->objectType == OBJECT_FUNCTION) {
                Oid funcid = LookupFuncNameTypeNames(stmt->object, stmt->objarg, false);

                nodes = GetFunctionNodes(funcid);
            }

            break;
        }
        case T_CreateTrigStmt: {
            CreateTrigStmt* stmt = (CreateTrigStmt*)parse_tree;
            if (stmt->relation) {
                /*
                 * Notice : When support create or replace trigger in future,
                 * we may need adjust the missing_ok parameter here.
                 */
                rel_id = RangeVarGetRelidExtended(stmt->relation, NoLock, false, false, false, true, NULL, NULL);
                nodes = RelidGetExecNodes(rel_id);
            }
            break;
        }
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized nodes %s in node group utility execution", nodeTagToString(parse_tree->type))));
        }
    }

    return nodes;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Description: Create temp table on coordinator for analyze with relative-estimation or
 *			absolute-estimate with debuging.
 *
 * Parameters:
 * 	@in relid: relation oid
 * 	@in stmt: the statment for analyze or vacuum command
 *	@in iscommit: commit and start a new transaction if non-temp table or delta table.
 *
 * Returns: void
 */
static void analyze_tmptbl_debug_cn(Oid relid, Oid main_relid, VacuumStmt* stmt, bool iscommit)
{
    /*
     * Don't create temp sample table for u_sess->cmd_cxt.default_statistics_target is absolute-estimate
     * or analyze for hdfs foreign table or temp table.
     */
    if (NULL == stmt || stmt->isAnalyzeTmpTable || DISTTYPE_HASH != stmt->disttype ||
        (default_statistics_target >= 0 && log_min_messages > DEBUG1)) {
        return;
    }

    MemoryContext oldcontext = CurrentMemoryContext;
    char* tableName = buildTempSampleTable(relid, main_relid, TempSmpleTblType_Table);

    if (tableName != NULL) {
        stmt->tmpSampleTblNameList = lappend(stmt->tmpSampleTblNameList, makeString(tableName));
    }

    if (iscommit) {
        /* Create temp table using a single transaction seperate with VACUUM ANALYZE. */
        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
    } else {
        CommandCounterIncrement();
    }

    (void)MemoryContextSwitchTo(oldcontext);
}
#endif

/*
 * Return if we need a full_datanodes execution with given group,
 * for example group_name is installation group or redistribution group
 * in cluster resizing stage.
 */
static bool need_full_dn_execution(const char* group_name)
{
    if (in_logic_cluster()) {
        return false;
    }
    const char* redistribution_group_name = PgxcGroupGetInRedistributionGroup();
    const char* installation_group_name = PgxcGroupGetInstallationGroup();

    if ((installation_group_name && strcmp(group_name, installation_group_name) == 0) ||
        (redistribution_group_name && strcmp(group_name, redistribution_group_name) == 0)) {
        return true;
    }

    Oid group_oid = get_pgxc_groupoid(group_name, false);
    const char* group_parent_name = get_pgxc_groupparent(group_oid);
    if (group_parent_name != NULL) {
        if ((installation_group_name && strcmp(group_parent_name, installation_group_name) == 0) ||
            (redistribution_group_name && strcmp(group_parent_name, redistribution_group_name) == 0)) {
            return true;
        }
    }
    return false;
}

/*
 * EstIdxMemInfo
 *	According to resource usage and cardinality estimation, assign query
 *	max/min mem, assigned statement mem in utilityDesc struct
 *
 * We use this function to evaluate mem in three cases:
 *	1. index case: create index/ reindex
 *	2. cluster case: now only btree is needed for sort
 *	3. vacuum full case: now only cstore table with pck is needed for sort
 *
 * Parameters:
 *	@in rel: input relation
 *	@in relation: parser struct of relation, containing nspname and relname
 *	@out desc: set meminfo in this struct
 *	@in info: index info, or NULL when we do cluster operation
 *	@in accessMethod: access method of index, or NULL when we do cluster operation
 *
 * Returns: void
 */
void EstIdxMemInfo(Relation rel, RangeVar* relation, UtilityDesc* desc, IndexInfo* info, const char* access_method)
{
    VacuumStmt* stmt = makeNode(VacuumStmt);
    char* query_string_with_info = NULL;
    StringInfoData s;
    bool vectorized = false;
    AnalyzeMode mode = ANALYZENORMAL;

    /* skip system table */
    if (rel->rd_id < FirstNormalObjectId)
        return;

    /* Only the following 4 kinds of indices may have sort op to consume memory */
    if (info != NULL && access_method && strncmp(access_method, "btree", strlen("btree")) &&
        strncmp(access_method, "cbtree", strlen("cbtree")) && strncmp(access_method, "hash", strlen("hash")) &&
        strncmp(access_method, "psort", strlen("psort")))
        return;

    /* for hdfs table, we should set to get estimated rows from complex table */
    if (RelationIsPAXFormat(rel))
        mode = ANALYZEMAIN;

    /* Set inital sampleRate, it means CN get estimate total row count from DN If sampleRate is -1. */
    global_stats_set_samplerate(mode, stmt, NULL);
    if (mode == ANALYZENORMAL) {
        stmt->pstGlobalStatEx[ANALYZENORMAL].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZENORMAL].exec_query = true;
    } else {
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].isReplication = false;
        stmt->pstGlobalStatEx[ANALYZEMAIN - 1].exec_query = true;
        stmt->pstGlobalStatEx[ANALYZEDELTA - 1].exec_query = true;
        stmt->pstGlobalStatEx[ANALYZECOMPLEX - 1].exec_query = true;
    }
    stmt->sampleTableRequired = false;

    const char* schemaname = NULL;
    const char* relname = NULL;
    if (relation != NULL) {
        if (relation->schemaname != NULL)
            schemaname = quote_identifier((const char*)relation->schemaname);
        else
            schemaname = quote_identifier((const char*)get_namespace_name(get_rel_namespace(rel->rd_id)));
        relname = quote_identifier((const char*)relation->relname);
    } else {
        schemaname = quote_identifier((const char*)get_namespace_name(get_rel_namespace(rel->rd_id)));
        relname = quote_identifier((const char*)get_rel_name(rel->rd_id));
    }

    initStringInfo(&s);
    appendStringInfo(&s, "analyze %s.%s;", schemaname, relname);

    /* Attatch global info to the query_string for get estimate total rows. */
    attatch_global_info(&query_string_with_info, stmt, s.data, true, mode, rel->rd_id, NULL);

    (void)ExecRemoteVacuumStmt(stmt, query_string_with_info, false, ARQ_TYPE_TOTALROWCNTS, mode, rel->rd_id);

    /* For hdfs table, we only create index on main table */
    double maxRowCnt = (mode == ANALYZENORMAL) ? stmt->pstGlobalStatEx[ANALYZENORMAL].topRowCnts :
                                                 stmt->pstGlobalStatEx[ANALYZEMAIN - 1].topRowCnts;
    double sortRowCnt = maxRowCnt;
    int32 width = 0;

    /* For partition table, we assume each partition is the same size */
    if (RELATION_IS_PARTITIONED(rel)) {
        if (rel->partMap && rel->partMap->type == PART_TYPE_RANGE) {
            int sumtotal = getPartitionNumber(rel->partMap);
            if (sumtotal > 0) {
                sortRowCnt /= sumtotal;
            }
        }
    }

    /* only psort index use partial vectorized sort */
    if ((access_method != NULL && strncmp(access_method, "psort", strlen("psort")) == 0) ||
        rel->rd_rel->relhasclusterkey) {
        sortRowCnt = Min(sortRowCnt, RelationGetPartialClusterRows(rel));
        vectorized = true;
    }

    if (info != NULL) {
        if (access_method != NULL && strncmp(access_method, "hash", strlen("hash")) == 0)
            width = sizeof(Datum);
        else
            width = getIdxDataWidth(rel, info, vectorized);
    } else
        width = get_relation_data_width(rel->rd_id, InvalidOid, NULL, rel->rd_rel->relhasclusterkey);

    SetSortMemInfo(
        desc, (int)sortRowCnt, width, vectorized, (info != NULL), u_sess->attr.attr_memory.maintenance_work_mem);
}

/*
 * SetSortMemInfo
 *	According to resource usage and cardinality estimation, assign query
 *	max/min mem, assigned statement mem in utilityDesc struct
 *
 * Parameters:
 *	@out desc: set meminfo in this struct
 *	@in row_count: estimated sort row count
 *	@in width: estimated tuple width
 *	@in vectorized: whether vertorized sort will be used for sort
 *	@in index_sort: if sort tuple is from index
 *	@in default_size: default size used when resource management is unavailable
 *	@in copy_rel: input relation for copy, or NULL for non-copy case
 *
 * Returns: void
 */
void SetSortMemInfo(
    UtilityDesc* desc, int row_count, int width, bool vectorized, bool index_sort, Size default_size, Relation copy_rel)
{
    Path sort_path; /* dummy for result of cost_sort */
    OpMemInfo mem_info;
    bool use_tenant = false;
    int max_mem = 0;
    int available_mem = 0;

    dywlm_client_get_memory_info(&max_mem, &available_mem, &use_tenant);

    /*
     * When database is startup, the require of resource mem will be deferred,
     * so give a default size in case no resource info available
     */
    if (max_mem == 0)
        max_mem = available_mem = default_size;

    if (copy_rel != NULL) {
        bool is_dfs_table = RelationIsPAXFormat(copy_rel);
        Oid rel_oid = copy_rel->rd_id;
        cost_insert(&sort_path, vectorized, 0.0, row_count, width, 0.0, available_mem, 1, rel_oid, is_dfs_table, &mem_info);
    } else {
        cost_sort(&sort_path, NIL, 0.0, row_count, width, 0.0, available_mem, -1.0, vectorized, 1, &mem_info, index_sort);
    }

    desc->cost = sort_path.total_cost + u_sess->attr.attr_sql.seq_page_cost * cost_page_size(row_count, width);
    desc->query_mem[0] = (int)ceil(Min(mem_info.maxMem, MAX_OP_MEM * 2));
    if (desc->query_mem[0] > STATEMENT_MIN_MEM * 1024L) {
        desc->query_mem[1] = (int)Max(STATEMENT_MIN_MEM * 1024L, desc->query_mem[0] * DECREASED_MIN_CMP_GAP);
    } else
        desc->query_mem[1] = desc->query_mem[0];
    /* We use assigned variable if query_mem is set */
    if (VALID_QUERY_MEM()) {
        desc->query_mem[0] = Max(desc->query_mem[0], max_mem);
        desc->query_mem[1] = Max(desc->query_mem[1], max_mem);
    }
    desc->min_mem = (int)(mem_info.minMem);
    desc->assigned_mem = max_mem;

    /* cut query_mem if exceeds max mem */
    if (desc->query_mem[0] > desc->assigned_mem)
        desc->query_mem[0] = desc->assigned_mem;
    if (desc->query_mem[1] > desc->assigned_mem)
        desc->query_mem[1] = desc->assigned_mem;
}

/*
 * AdjustIdxMemInfo
 *	Assign the final decided mem info into IndexStmt, so it can then pass
 *	down to datanode for memory control
 *
 * Parameters:
 *	@out mem_info: adaptive mem struct to assign mem info
 *	@in desc: Utility desc struct that has mem info to assign
 *
 * Returns: void
 */
void AdjustIdxMemInfo(AdaptMem* mem_info, UtilityDesc* desc)
{
    mem_info->work_mem = Min(desc->query_mem[0], desc->assigned_mem);
    mem_info->max_mem = desc->assigned_mem;

    /* Adjust operator mem if it's close to max or min mem */
    if (mem_info->work_mem < MIN_OP_MEM)
        mem_info->work_mem = MIN_OP_MEM;
    else if (mem_info->work_mem < mem_info->max_mem)
        mem_info->work_mem = (int)Min(mem_info->work_mem * BIG_MEM_RATIO, mem_info->max_mem);
    else if (mem_info->work_mem == desc->min_mem)
        mem_info->work_mem = (int)(mem_info->work_mem * SMALL_MEM_RATIO);

    /* reset mem info for CN execution */
    desc->query_mem[0] = 0;
    desc->query_mem[1] = 0;
}

/*
 * constructMesageWithMemInfo
 *	if index mem info is adaptive, we should pass it down to datanode.
 *	This function is used to combine mem info to the message.
 *
 * Parameters:
 *	@in query_string: original query string
 *	@in mem_info: adapt mem struct that contains mem info
 *
 * Returns: combined message
 */
char* ConstructMesageWithMemInfo(const char* query_string, AdaptMem mem_info)
{
    char* final_string = (char*)query_string;

    /* if index build mem is adaptive, we should pass it to datanode */
    if (mem_info.work_mem > 0) {
        List* l = NIL;
        l = lappend_int(l, mem_info.work_mem);
        l = lappend_int(l, mem_info.max_mem);
        char* op_string = nodeToString(l);

        AssembleHybridMessage(&final_string, query_string, op_string);
        pfree_ext(op_string);
        list_free_ext(l);
        query_string = final_string;
    }

    return final_string;
}

static bool IsAllTempObjectsInVacuumStmt(Node* parsetree)
{
    bool isTemp = false;
    VacuumStmt* stmt = (VacuumStmt*)parsetree;
    if (((stmt->options & VACOPT_ANALYZE) || (stmt->options & VACOPT_VACUUM)) && stmt->relation != NULL) {
        Oid relationid = RangeVarGetRelid(stmt->relation, NoLock, false);
        LOCKMODE lockmode = NEED_EST_TOTAL_ROWS_DN(stmt) ? AccessShareLock : ShareUpdateExclusiveLock;
        Relation onerel = try_relation_open(relationid, lockmode);
        if (onerel != NULL && onerel->rd_rel != NULL && onerel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
            isTemp = true;
        }
        if (onerel != NULL) {
            relation_close(onerel, lockmode);
        }
    }
    return isTemp;
}

static void SendCopySqlToClient(StringInfo copy_sql)
{
    StringInfoData buf;

    /* Send a RowDescription message */
    pq_beginmessage(&buf, 'T');
    pq_sendint16(&buf, 1); /* 1 fields */

    /* first field */
    pq_sendstring(&buf, "LOAD TRANSFORM TO COPY RESULT"); /* col name */
    pq_sendint32(&buf, 0);           /* table oid */
    pq_sendint16(&buf, 0);           /* attnum */
    pq_sendint32(&buf, TEXTOID);     /* type oid */
    pq_sendint16(&buf, UINT16_MAX);  /* typlen */
    pq_sendint32(&buf, 0);           /* typmod */
    pq_sendint16(&buf, 0);           /* format code */
    pq_endmessage_noblock(&buf);

    /* Send a DataRow message */
    pq_beginmessage(&buf, 'D');
    pq_sendint16(&buf, 1);             /* # of columns */
    pq_sendint32(&buf, copy_sql->len); /* col1 len */
    pq_sendbytes(&buf, (char *)copy_sql->data, copy_sql->len);
    pq_endmessage_noblock(&buf);

    /* Send CommandComplete and ReadyForQuery messages */
    EndCommand_noblock("LOAD", DestTuplestore);
}

static void TransformLoadGetRelation(LoadStmt* stmt, StringInfo buf)
{
    RangeVar* relation = stmt->relation;

    if (relation->schemaname && relation->schemaname[0]) {
        appendStringInfo(buf, "%s.", quote_identifier(relation->schemaname));
    }
    appendStringInfo(buf, "%s ", quote_identifier(relation->relname));
}

static void TransformLoadGetFiledList(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;
    ListCell* field_option = NULL;
    int special_filed = 0;

    foreach (option, stmt->rel_options) {
        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "fields_list") == 0) {
            List* field_list = (List *)def->arg;
            appendStringInfo(buf, "(");
            foreach (field_option, field_list) {
                SqlLoadColExpr* coltem = (SqlLoadColExpr *)lfirst(field_option);
                special_filed += ((coltem->const_info != NULL) || (coltem->sequence_info != NULL) ||
                                         (coltem->is_filler == true) ||
                                         ((coltem->scalar_spec != NULL) &&
                                          ((SqlLoadScalarSpec *)coltem->scalar_spec)->position_info != NULL))
                                     ? 1
                                     : 0;
                appendStringInfo(buf, " %s,", coltem->colname);
            }
            buf->data[buf->len - 1] = ' ';
            appendStringInfo(buf, ") ");
            stmt->is_only_special_filed = (special_filed == list_length(field_list));
            break;
        }
    }
}

static void TransformLoadType(LoadStmt* stmt, StringInfo buf)
{
    switch (stmt->load_type) {
        case LOAD_DATA_APPEND: {
            // COPY's default mode, no need to handle
            break;
        }
        case LOAD_DATA_REPLACE:
        case LOAD_DATA_TRUNCATE: {
            appendStringInfo(buf, "TRUNCATE TABLE ");
            TransformLoadGetRelation(stmt, buf);
            appendStringInfo(buf, "; ");
            break;
        }
        case LOAD_DATA_INSERT: {
            appendStringInfo(buf, "SELECT 'has_data_in_table' FROM ");
            TransformLoadGetRelation(stmt, buf);
            appendStringInfo(buf, "LIMIT 1; ");
            break;
        }
        default: {
            elog(ERROR, "load type(%d) is not supported", stmt->load_type);
            break;
        }
    }
}

static char * TransformPreLoadOptionsData(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;

    foreach (option, stmt->pre_load_options) {
        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "data") == 0) {
            char* data = defGetString(def);
            return data;
        }
    }

    return NULL;
}

static void TransformLoadDatafile(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;
    char* filename = NULL;

    appendStringInfo(buf, "FROM ");

    // has datafile in options
    filename = TransformPreLoadOptionsData(stmt, buf);
    if (filename == NULL) {
        foreach (option, stmt->load_options) {
            DefElem* def = (DefElem*)lfirst(option);
            if (strcmp(def->defname, "infile") == 0) {
                filename = defGetString(def);
                break;
            }
        }
    }

    if (filename != NULL) {
        appendStringInfo(buf, "\'%s\' LOAD ", filename);
    }
    else {
        appendStringInfo(buf, "STDIN LOAD ");
    }
}

static void TransformFieldsListScalar(SqlLoadColExpr* coltem, StringInfo transformbuf, StringInfo formatterbuf)
{
    SqlLoadScalarSpec* scalarSpec = (SqlLoadScalarSpec *)coltem->scalar_spec;
    if (scalarSpec->nullif_col != NULL) {
        appendStringInfo(transformbuf, "%s AS nullif(trim(%s), \'\'),", quote_identifier(coltem->colname),
                                                                        quote_identifier(scalarSpec->nullif_col));
    }
    if (scalarSpec->sqlstr != NULL) {
        appendStringInfo(transformbuf, "%s", quote_identifier(coltem->colname));
        A_Const* colexprVal = (A_Const *)scalarSpec->sqlstr;
        appendStringInfo(transformbuf, " AS %s,", colexprVal->val.val.str);
    }
    if (scalarSpec->position_info) {
        appendStringInfo(formatterbuf, "%s", quote_identifier(coltem->colname));
        SqlLoadColPosInfo* posInfo = (SqlLoadColPosInfo *)scalarSpec->position_info;
        int offset = posInfo->start - 1;
        int length = posInfo->end - posInfo->start + 1;
        appendStringInfo(formatterbuf, "(%d, %d),", offset, length);
    }
}

static void TransformFieldsListSeque(const char *schemaname, const char *relname,
                                     SqlLoadColExpr* coltem, StringInfo sequencebuf)
{
    SqlLoadSequInfo* sequenceInfo = (SqlLoadSequInfo *)coltem->sequence_info;
    int64 start_num = sequenceInfo->start;
    int64 step_num = sequenceInfo->step;

    if (start_num == LOADER_SEQUENCE_COUNT_FLAG) {
        start_num = getCopySequenceCountval(schemaname, relname) + step_num;
    } else if (start_num == LOADER_SEQUENCE_MAX_FLAG) {
        start_num = getCopySequenceMaxval(schemaname, relname, coltem->colname) + step_num;
    }

    appendStringInfo(sequencebuf, "%s", quote_identifier(coltem->colname));
    appendStringInfo(sequencebuf, "(%ld, %ld),", start_num, step_num);
}

static void TransformFieldsListAppInfo(StringInfo fieldDataBuf, int dataBufCnt, StringInfo buf)
{
    if (dataBufCnt < LOADER_COL_BUF_CNT) {
        return;
    }

    StringInfo constantbuf = &fieldDataBuf[0];
    StringInfo formatterbuf = &fieldDataBuf[1];
    StringInfo transformbuf = &fieldDataBuf[2];
    StringInfo sequencebuf = &fieldDataBuf[3];
    StringInfo fillerbuf = &fieldDataBuf[4];

    if (constantbuf->len) {
        constantbuf->data[constantbuf->len - 1] = '\0';
        appendStringInfo(buf, "CONSTANT(%s) ", constantbuf->data);
    }

    if (formatterbuf->len) {
        formatterbuf->data[formatterbuf->len - 1] = '\0';
        appendStringInfo(buf, "fixed FORMATTER(%s) ", formatterbuf->data);
    }

    if (sequencebuf->len) {
        sequencebuf->data[sequencebuf->len - 1] = '\0';
        appendStringInfo(buf, "SEQUENCE(%s) ", sequencebuf->data);
    }
    if (fillerbuf->len) {
        fillerbuf->data[fillerbuf->len - 1] = '\0';
        appendStringInfo(buf, "FILLER(%s) ", fillerbuf->data);
    }
    if (transformbuf->len) {
        transformbuf->data[transformbuf->len - 1] = '\0';
        appendStringInfo(buf, "TRANSFORM(%s) ", transformbuf->data);
    }
}

static void TransformFieldsListOpt(LoadStmt* stmt, DefElem* def, StringInfo buf)
{
    List* field_list = (List *)def->arg;
    ListCell* option = NULL;
    StringInfoData fieldDataBuf[LOADER_COL_BUF_CNT];
    RangeVar* relation = stmt->relation;
    const char *schemaname = NULL;
    const char *relname = quote_identifier(relation->relname);
    if (relation->schemaname && relation->schemaname[0]) {
        schemaname = quote_identifier(relation->schemaname);
    }

    for (int i = 0; i < LOADER_COL_BUF_CNT; i++) {
        initStringInfo(&fieldDataBuf[i]);
    }

    StringInfo constantbuf = &fieldDataBuf[0];
    StringInfo formatterbuf = &fieldDataBuf[1];
    StringInfo transformbuf = &fieldDataBuf[2];
    StringInfo sequencebuf = &fieldDataBuf[3];
    StringInfo fillerbuf = &fieldDataBuf[4];

    foreach (option, field_list) {
        SqlLoadColExpr* coltem = (SqlLoadColExpr *)lfirst(option);
        if (coltem->const_info != NULL) {
            appendStringInfo(constantbuf, "%s", quote_identifier(coltem->colname));
            A_Const* constVal = (A_Const *)coltem->const_info;
            appendStringInfo(constantbuf, " \'%s\',", constVal->val.val.str);
        }
        else if (coltem->sequence_info != NULL) {
            TransformFieldsListSeque(schemaname, relname, coltem, sequencebuf);
        }else if (coltem->is_filler == true) {
            appendStringInfo(fillerbuf, " %s,", quote_identifier(coltem->colname));
        }
        else if (coltem->scalar_spec != NULL) {
            TransformFieldsListScalar(coltem, transformbuf, formatterbuf);
        }
    }

    TransformFieldsListAppInfo(fieldDataBuf, LOADER_COL_BUF_CNT, buf);
}

static void TransformLoadOptions(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;

    foreach (option, stmt->load_options) {

        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "characterset") == 0) {
            char* encoding = defGetString(def);
            if (pg_strcasecmp(encoding, "AL32UTF8") == 0) {
                encoding = "utf8";
            }
            if (pg_strcasecmp(encoding, "zhs16gbk") == 0) {
                encoding = "gbk";
            }
            if (pg_strcasecmp(encoding, "zhs32gb18030") == 0) {
                encoding = "gb18030";
            }
            appendStringInfo(buf, "ENCODING \'%s\' ", encoding);
            break;
        }
    }
}

static void TransformLoadRelationOptionsWhen(List *when_list, StringInfo buf)
{
    ListCell* lc = NULL;
    bool is_frist = true;

    foreach (lc, when_list) {
        LoadWhenExpr *when = (LoadWhenExpr *)lfirst(lc);
        CheckCopyWhenExprOptions(when);

        if (is_frist == true) {
            appendStringInfo(buf, "WHEN ");
            is_frist = false;
        } else {
            appendStringInfo(buf, "AND ");
        }

        if (when->whentype == 0) {
            appendStringInfo(buf, "(%d-%d) ", when->start, when->end);
            appendStringInfo(buf, "%s ", when->oper);
            appendStringInfo(buf, "\'%s\' ", when->val);
        } else {
            appendStringInfo(buf, "%s ", quote_identifier(when->attname));
            appendStringInfo(buf, "%s ", when->oper);
            appendStringInfo(buf, "\'%s\' ", when->val);
        }
    }
}

static void TransformLoadRelationOptions(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;
    bool fields_csv = false;
    bool optionally_enclosed_by = false;

    foreach (option, stmt->rel_options) {

        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "when_expr") == 0) {
            List *when_list = (List*)(def->arg);
            TransformLoadRelationOptionsWhen(when_list, buf);
        }
        else if (strcmp(def->defname, "fields_csv") == 0) {
            if (stmt->is_only_special_filed == false) {
                appendStringInfo(buf, "csv ");
                fields_csv = true;
            }
        } else if (strcmp(def->defname, "trailing_nullcols") == 0) {
            appendStringInfo(buf, "FILL_MISSING_FIELDS 'multi' ");
        } else if (strcmp(def->defname, "fields_terminated_by") == 0) {
            if (stmt->is_only_special_filed == false) {
                char* terminated_by = defGetString(def);
                appendStringInfo(buf, "DELIMITER \'%s\' ", terminated_by);
            }
        } else if (strcmp(def->defname, "optionally_enclosed_by") == 0) {
            if (stmt->is_only_special_filed == false) {
                char *quote = defGetString(def);
                appendStringInfo(buf, "QUOTE \'%s\' ", quote);
                optionally_enclosed_by = true;
            }
        } else if (strcmp(def->defname, "fields_list") == 0) {
            TransformFieldsListOpt(stmt, def, buf);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", def->defname)));
        }
    }

    if (optionally_enclosed_by && !fields_csv && stmt->is_only_special_filed == false) {
        appendStringInfo(buf, "csv ");
    }
}

static bool LoadDataHasWhenExpr(LoadStmt* stmt)
{
    ListCell* option = NULL;

    foreach (option, stmt->rel_options) {
        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "when_expr") == 0) {
            return list_length((List*)(def->arg)) != 0;
        }
    }

    return false;
}


static void TransformPreLoadOptions(LoadStmt* stmt, StringInfo buf)
{
    ListCell* option = NULL;

    int64 errors = 0;

    foreach (option, stmt->pre_load_options) {
        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "errors") == 0) {
            errors = defGetInt64(def);
            if (errors < 0) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("ERRORS=%ld in OPTIONS should be >= 0", errors)));
            }
            else if (errors > 0) {
                appendStringInfo(buf, "LOG ERRORS DATA REJECT LIMIT \'%ld\' ", errors);
            }
        }
    }

    if (errors == 0 && LoadDataHasWhenExpr(stmt)) {
        appendStringInfo(buf, "LOG ERRORS DATA ");
    }

    foreach (option, stmt->pre_load_options) {
        DefElem* def = (DefElem*)lfirst(option);
        if (strcmp(def->defname, "skip") == 0) {
            int64 skip = defGetInt64(def);
            if (skip < 0) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("SKIP=%ld in OPTIONS should be >= 0", skip)));
            }
            appendStringInfo(buf, "SKIP %ld ", skip);
        }
    }
}

static void TransformLoadDataToCopy(LoadStmt* stmt)
{
    StringInfoData bufData;
    StringInfo buf = &bufData;

    initStringInfo(buf);

    TransformLoadType(stmt, buf);

    appendStringInfo(buf, "\\COPY ");

    TransformLoadGetRelation(stmt, buf);

    TransformLoadGetFiledList(stmt, buf);

    TransformLoadDatafile(stmt, buf);

    TransformPreLoadOptions(stmt, buf);

    TransformLoadOptions(stmt, buf);

    TransformLoadRelationOptions(stmt, buf);

    appendStringInfo(buf, "IGNORE_EXTRA_DATA;");

    elog(LOG, "load data is recv type: %d tablename:%s\ncopy sql:%s",
        stmt->type, stmt->relation->relname, buf->data);

    SendCopySqlToClient(buf);
}

static int64 getCopySequenceCountval(const char *nspname, const char *relname)
{
    StringInfoData buf;
    int ret;
    bool isnull;
    Datum attval;
    if (relname == NULL) {
        return 0;
    }
    initStringInfo(&buf);
    if (nspname == NULL) {
        appendStringInfo(&buf, "select cast(count(*) as bigint) from %s", quote_identifier(relname));
    } else {
        appendStringInfo(&buf, "select cast(count(*) as bigint) from %s.%s", quote_identifier(nspname),
                         quote_identifier(relname));
    }
    if ((ret = SPI_connect()) < 0) {
        /* internal error */
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI connect failure - returned %d", ret)));
    }
    ret = SPI_execute(buf.data, true, INT_MAX);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errcode(ERRCODE_SPI_EXECUTE_FAILURE), errmsg("SPI_execute failed: error code %d", ret)));
    attval = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    if (isnull) {
        attval = 0;
    }
    SPI_finish();
    return DatumGetInt64(attval);
}

static int64 getCopySequenceMaxval(const char *nspname, const char *relname, const char *colname)
{
    StringInfoData buf;
    int ret;
    bool isnull;
    Datum attval;
    if (relname == NULL || colname == NULL) {
        return 0;
    }
    initStringInfo(&buf);
    if (nspname == NULL) {
        appendStringInfo(&buf, "select cast(nvl(max(%s),0) as bigint) from %s ", quote_identifier(colname),
                         quote_identifier(relname));
    } else {
        appendStringInfo(&buf, "select cast(nvl(max(%s),0) as bigint) from %s.%s ", quote_identifier(colname),
                         quote_identifier(nspname), quote_identifier(relname));
    }
    if ((ret = SPI_connect()) < 0) {
        /* internal error */
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI connect failure - returned %d", ret)));
    }
    ret = SPI_execute(buf.data, true, INT_MAX);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errcode(ERRCODE_SPI_EXECUTE_FAILURE), errmsg("SPI_execute failed: error code %d", ret)));
    attval = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    if (isnull) {
        attval = 0;
    }
    SPI_finish();
    return DatumGetInt64(attval);
}
