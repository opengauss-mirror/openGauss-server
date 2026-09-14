/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 *
 * online_ddl.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/online_ddl/online_ddl.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_class.h"
#include "catalog/namespace.h"
#include "commands/tablecmds.h"
#include "ddes/dms/ss_common_attr.h"
#include "nodes/parsenodes_common.h"
#include "utils/builtins.h"
#include "utils/mem_snapshot.h"
#include "utils/palloc.h"
#include "utils/rel.h"

#include "commands/online_ddl_ctid_map.h"
#include "commands/online_ddl_globalhash.h"
#include "commands/online_ddl_util.h"
#include "commands/online_ddl.h"

static constexpr int ONLINE_DDL_ALTER_TABLE_TYPE_COUNT = 2;

AlterTableType online_ddl_alter_table_types[ONLINE_DDL_ALTER_TABLE_TYPE_COUNT] = {
    AT_AlterColumnType, /* alter column type */
    AT_SetRelOptions    /* set reloptions */
};

static constexpr int ONLINE_DDL_RELATION_COUNT = 1;

char online_ddl_relations[ONLINE_DDL_RELATION_COUNT] = {
    RELKIND_RELATION /* temp schema for online ddl */
};

void OnlineDDLinit()
{
    if (g_instance.online_ddl_cxt.isInited) {
        return;
    }
    if (g_instance.online_ddl_cxt.context == NULL) {
        g_instance.online_ddl_cxt.context =
            AllocSetContextCreate(g_instance.instance_context, "Online DDL Context", ALLOCSET_DEFAULT_MINSIZE,
                                  ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }
    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.online_ddl_cxt.context);
    initDDLGlobalHash(g_instance.online_ddl_cxt.context);
    MemoryContextSwitchTo(oldCxt);
    g_instance.online_ddl_cxt.isInited = true;
}

void OnlineDDLDestroy()
{
    if (g_instance.online_ddl_cxt.isInited) {
        g_instance.online_ddl_cxt.isInited = false;
        MemoryContextDelete(g_instance.online_ddl_cxt.context);
    }
}

static bool OnlineDDLPreCheck(Relation relation)
{
    if (IsTransactionBlock() || IsSubTransaction()) {
        ereport(NOTICE,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Online DDL operation is not supported in transaction block, do it without online ddl.")));
        return false;
    }
    if (RelationIsTsStore(relation) || RelationIsColStore(relation) || RelationIsUstoreFormat(relation)) {
        const char* kind =
            RelationIsTsStore(relation) ? "time series" : (RelationIsColStore(relation) ? "column-store" : "ustore");
        ereport(NOTICE,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Online DDL operation is not supported for %s tables, do it without online ddl.", kind)));
        return false;
    }
    if (ENABLE_DMS) {
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for share storage, do it without online ddl.")));
        return false;
    }
    if (IsSystemRelation(relation)) {
        ereport(NOTICE,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Online DDL operation is not supported for system relation %s, do it without online ddl.",
                        relation->rd_rel->relname.data)));
        return false;
    }
    /* Online DDL operation is not supported in partitioned table with global index. */
    if (RelationIsPartitioned(relation)) {
        List* indexOidlist = RelationGetIndexList(relation);
        ListCell* l = NULL;
        foreach (l, indexOidlist) {
            Oid indexOid = lfirst_oid(l);
            Relation indexRel = index_open(indexOid, AccessShareLock);
            if (RelationIsGlobalIndex(indexRel)) {
                ereport(NOTICE,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for partitioned table %s with global index %s, "
                                "do it without online ddl.",
                                relation->rd_rel->relname.data, indexRel->rd_rel->relname.data)));
                index_close(indexRel, AccessShareLock);
                list_free_ext(indexOidlist);
                return ONLINE_DDL_INVALID;
            }
            index_close(indexRel, AccessShareLock);
        }
        list_free_ext(indexOidlist);
    }
    return true;
}

/**
 * Check if the online DDL operation is feasible.
 *
 * @param wqueue The work queue for online DDL operations.
 * @param relation The relation on which the operation is to be performed.
 * @param cmds The list of commands to be executed.
 * @param lockmode The lock mode required for the operation.
 * @return true if the operation is feasible, false otherwise.
 */
OnlineDDLType OnlineDDLCheckAlterFeasible(List** wqueue, Relation relation, List* cmds, LOCKMODE lockmode)
{
    if (!OnlineDDLPreCheck(relation)) {
        return ONLINE_DDL_INVALID;
    }

    /* check need rewrite */
    ListCell* ltab = NULL;
    bool rewriteOpt = false;
    bool checkOpt = false;
    bool unsupportOpt = false;
    AlterTableType errorType = (AlterTableType)0;
    foreach (ltab, *wqueue) {
        AlteredTableInfo* tab = (AlteredTableInfo*)lfirst(ltab);
        rewriteOpt |= tab->rewrite;
        checkOpt |= (tab->constraints != NIL || tab->new_notnull);
    }

    bool allowed = false;
    for (int pass = 0; pass < AT_NUM_PASSES; pass++) {
        ltab = NULL;
        foreach (ltab, *wqueue) {
            AlteredTableInfo* tab = (AlteredTableInfo*)lfirst(ltab);
            List* subcmds = tab->subcmds[pass];
            Relation rel;
            ListCell* lcmd = NULL;
            if (subcmds == NIL) {
                continue;
            }
            /*
             * Appropriate lock was obtained by phase 1, needn't get it again
             */
            rel = relation_open(tab->relid, NoLock);
            foreach (lcmd, subcmds) {
                AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lcmd);
                switch (cmd->subtype) {
                    case AT_AlterColumnType: {
                        allowed = true;
                        break;
                    }
                    case AT_AddConstraint:
                    case AT_AddConstraintRecurse:
                    case AT_ReAddConstraint: {
                        allowed = true;
                        checkOpt |= allowed;
                        break;
                    }
                    case AT_ResetRelOptions:   /* RESET (...) */
                    case AT_ReplaceRelOptions: /* replace entire option list */
                    case AT_SetRelOptions: {   /* SET (...) */
                        /* check set reloptions feasible */
                        bool feasible =
                            OnlineDDLCheckSetCompressOptFeasible(rel, (List*)cmd->def, cmd->subtype, lockmode, tab);
                        allowed |= feasible;
                        rewriteOpt |= allowed;
                        break;
                    }
                    case AT_SetNotNull: { /* ALTER COLUMN SET NOT NULL */
                        bool feasible = OnlineDDLCheckSetNotNullFeasible(rel, cmd->name, lockmode);
                        allowed |= feasible;
                        checkOpt |= allowed;
                        break;
                    }
                    case AT_ModifyColumn: { /* MODIFY column NOT NULL */
                        bool feasible = OnlineDDLCheckAlterModifyColumnFeasible(tab, rel, cmd);
                        allowed |= feasible;
                        checkOpt |= allowed;
                        break;
                    }
                    case AT_UnusableIndex:
                    case AT_AddIndex:
                    case AT_ReAddIndex:
                    case AT_AddIndexConstraint: {
                        unsupportOpt = true;
                        errorType = cmd->subtype;
                        break;
                    }
                    case AT_SplitPartition:
                    case AT_SplitSubPartition:
                    case AT_MergePartition: {
                        allowed = true;
                        rewriteOpt |= allowed;
                        break;
                    }
                    default: {
                        break;
                    }
                }
                if (unsupportOpt) {
                    break;
                }
            }
            relation_close(rel, NoLock);
        }
    }

    if (unsupportOpt) {
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for command types, type: %d, "
                                "do it without online ddl.",
                                errorType)));
        return ONLINE_DDL_INVALID;
    }

    if (!rewriteOpt && !checkOpt) {
        ereport(NOTICE, (errcode(MOD_ONLINE_DDL), errmsg("Current command doestn't need to rewrite table.")));
        return ONLINE_DDL_INVALID;
    }

    if (!allowed) {
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for this command type, type: %d, "
                                "do it without online ddl.",
                                cmds->type)));
        return ONLINE_DDL_INVALID;
    }

    /* check relation type */
    allowed = false;
    for (int i = 0; i < ONLINE_DDL_RELATION_COUNT; i++) {
        if (relation->rd_rel->relkind == online_ddl_relations[i]) {
            allowed = true;
            break;
        }
    }
    if (!allowed) {
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for this relind, type: %c, "
                                "do it without online ddl.",
                                relation->rd_rel->relkind)));
        return ONLINE_DDL_INVALID;
    }
    return rewriteOpt ? ONLINE_DDL_REWRITE : ONLINE_DDL_CHECK;
}

/**
 * @brief Check if the online DDL vacuum full operation is feasible
 *
 * @param relation The relation on which the operation is to be performed.
 * @return OnlineDDLType true if the operation is feasible, false otherwise.
 */
OnlineDDLType OnlineDDLCheckVacuumFeasible(Relation relation, Oid indexOid)
{
    if (!OnlineDDLPreCheck(relation)) {
        return ONLINE_DDL_INVALID;
    }

    bool allowed = false;
    for (int i = 0; i < ONLINE_DDL_RELATION_COUNT; i++) {
        if (relation->rd_rel->relkind == online_ddl_relations[i]) {
            allowed = true;
            break;
        }
    }
    if (!allowed) {
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("Online DDL operation is not supported for this relind, type: %c, "
                                "do it without online ddl.",
                                relation->rd_rel->relkind)));
        return ONLINE_DDL_INVALID;
    }
    if (!allowed) {
        return ONLINE_DDL_INVALID;
    }
    return indexOid == InvalidOid ? ONLINE_DDL_VACUUM : ONLINE_DDL_CLUSTER;
}

void OnlineDDLCreateTempSchema(Relation relation)
{
    OnlineDDLRelOperators* operators = RelationGetOnlineDDLOperators(relation);
    TransactionId xid = operators->getStartXid();
    RelFileNode relFileNode = relation->rd_node;
    Oid spcNode = relFileNode.spcNode;
    Oid dbNode = relFileNode.dbNode;
    Oid relId = relation->rd_id;

    char tempSchemaName[NAMEDATALEN] = {0};
    errno_t rc = snprintf_s(tempSchemaName, NAMEDATALEN, NAMEDATALEN - 1, "online_ddl_temp_schema_%lu_%lu_%lu_%lu", xid,
                            spcNode, dbNode, relId);

    securec_check_ss(rc, "\0", "\0");
    operators->setStringInfoTempSchemaName(tempSchemaName);
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE SCHEMA %s", tempSchemaName);
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
}

void OnlineDDLCreateTempDelta(Relation relation)
{
    OnlineDDLRelOperators* operators = RelationGetOnlineDDLOperators(relation);
    StringInfo tempSchemaName = operators->getStringInfoTempSchemaName();

    operators->setStringInfoDeltaRelname(ONLINE_DDL_DELTA_RELNAME);
    Oid deltaRelOid = InvalidOid;

    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE UNLOGGED TABLE %s.%s", tempSchemaName->data, ONLINE_DDL_DELTA_RELNAME);

    /*
     * Create delta table for partitioned table.
     * For partitioned table, we need to record the partition number for each delta record.
     * For non-partitioned table, we only record the operation type and old tuple ctid.
     * If vacuuming partitioned table, we rewrite one partition at a time.
     */
    if (RELATION_IS_PARTITIONED(relation) &&
        (!OnlineDDLIsVacuumOrClusterMode(operators) || OnlineDDLIsClusterAllPartition(operators))) {
        appendStringInfo(query, "("
                                "operation TINYINT NOT NULL, "
                                "old_tup_ctid TID NOT NULL, "
                                "partion_no OID NOT NULL"
                                ")");
    } else {
        appendStringInfo(query, "("
                                "operation TINYINT NOT NULL, "
                                "old_tup_ctid TID NOT NULL"
                                ")");
    }
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);

    /* Build the qualified name for the delta table */
    List* qualifiedName =
        list_make2(makeString(pstrdup(tempSchemaName->data)), makeString(pstrdup(ONLINE_DDL_DELTA_RELNAME)));
    deltaRelOid = RangeVarGetRelid(makeRangeVarFromNameList(qualifiedName), AccessShareLock, false);
    operators->setDeltaRelid(deltaRelOid);
    list_free_deep(qualifiedName);
}

void OnlineDDLInitTempObject(Relation relation)
{
    OnlineDDLCreateTempSchema(relation);
    OnlineDDLCreateTempDelta(relation);
}

void OnlineDDLDropTempSchema(Relation relation)
{
    if (relation == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("[Online-DDL] OnlineDDLDropTempSchema failed, relation is null.")));
    }
    OnlineDDLRelOperators* operators = RelationGetOnlineDDLOperators(relation);
    if (operators == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("[Online-DDL] OnlineDDLDropTempSchema failed, can't get operators from relation.")));
    }
    StringInfo tempSchemaName = operators->getStringInfoTempSchemaName();
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "DROP SCHEMA IF EXISTS %s CASCADE", tempSchemaName->data);
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
}

void OnlineDDLDropTempSchema(OnlineDDLRelOperators* operators)
{
    if (operators == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("[Online-DDL] OnlineDDLDropTempSchema failed can't get operators from relation.")));
    }
    StringInfo tempSchemaName = operators->getStringInfoTempSchemaName();
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "DROP SCHEMA IF EXISTS %s CASCADE", tempSchemaName->data);
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
}

void OnlineDDLResetAppendMode(Relation* relation)
{
    /*  Close relation, but don't release AccessExclusiveLock. */
    Oid relId = (*relation)->rd_id;
    char* relname = (*relation)->rd_rel->relname.data;
    char* namespacename = get_namespace_name((*relation)->rd_rel->relnamespace);
    relation_close(*relation, NoLock);

    StringInfo query = makeStringInfo();
    appendStringInfo(query, "ALTER TABLE %s.%s RESET (append_mode)", quote_identifier(namespacename),
                     quote_identifier(relname));
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);

    *relation = relation_open(relId, AccessExclusiveLock);
    if (*relation == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("[Online-DDL] OnlineDDLResetAppendMode reopen relation failed.")));
    }
}

void OnlineDDLSetTargetPartitionOid(OnlineDDLRelOperators* operators, Relation relation, Oid partitionOid)
{
    OnlineDDLType onlineDDLType = operators->getOnlineDDLType();
    if (partitionOid != InvalidOid) {
        /* Used only for partitioned table vacuum */
        if (!RELATION_IS_PARTITIONED(relation)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("OnlineDDLInstanceInit failed, cannot perform partition-level operation on "
                                   "non-partitioned table \"%s\"",
                                   RelationGetRelationName(relation))));
        }
        operators->setCurrentPartitionOid(partitionOid);
        ereport(NOTICE,
                (errmsg("Online DDL start to vacuum for partition %u of relation %u.", partitionOid, relation->rd_id)));
    }
}

/**
 * Initialize the online DDL instance.
 *
 * @param relation The relation on which the operation is to be performed.
 * @param lockmode The lock mode required for the operation.
 * @param partitionOid The OID of the partition, used when vacuum/cluster single partition.
 * @return true if the initialization is successful, false otherwise.
 */
bool OnlineDDLInstanceInit(Relation* relation, LOCKMODE lockmode, OnlineDDLType onlineDDLType, Oid partitionOid)
{
    if (!CheckLockRelationOid((*relation)->rd_id, AccessExclusiveLock)) {
        LockRelationOid((*relation)->rd_id, AccessExclusiveLock);
    }
    /* init online ddl hash entry */
    DDLGlobalHashEntry* hashEntry = onlineDDLInitHashEntry(*relation, true, onlineDDLType);
    OnlineDDLRegisterGlobalHashEntry(hashEntry);
    u_sess->online_ddl_operators = hashEntry->operators;
    (*relation)->rd_online_ddl_operators = (void*)hashEntry->operators;
    OnlineDDLRelOperators* operators = (OnlineDDLRelOperators*)(*relation)->rd_online_ddl_operators;
    operators->setStatus(ONLINE_DDL_STATUS_PREPARE);

    /* Used when rewrite only on partition, if partitioned */
    OnlineDDLSetTargetPartitionOid(operators, *relation, partitionOid);

    /* Set relation enable append mode. */
    OnlineDDLEnableRelationAppendMode(*relation);
    OnlineDDLInitTempObject(*relation);
    pfree(hashEntry);

    /* Get session AccessExclusiveLock lock, before finish xact. */
    LockRelId relLockRelId = (*relation)->rd_lockInfo.lockRelId;
    LockRelationIdForSession(&relLockRelId, AccessExclusiveLock);
    /* release origin ExclusiveLock lock, hold AccessExclusiveLock lock for online ddl vacuum */
    if (onlineDDLType == ONLINE_DDL_VACUUM || onlineDDLType == ONLINE_DDL_CLUSTER) {
        UnlockRelationIdForSession(&relLockRelId, ExclusiveLock);
    }
    operators->openDeltaRelation(AccessExclusiveLock);
    LockRelId deltaRelLockRelId = operators->getDeltaRelation()->rd_lockInfo.lockRelId;
    LockRelationIdForSession(&deltaRelLockRelId, ShareUpdateExclusiveLock);

    Snapshot snapshot = GetTransactionSnapshot();
    operators->setBaselineSnapshot(snapshot);

    /* Close relation before finish xact. */
    relation_close(*relation, AccessExclusiveLock);

    /* Relation has been closed, add append_mode, exec ddl with no pin of target table. */
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "ALTER TABLE %s.%s SET (append_mode = online_ddl)",
                     quote_identifier(get_namespace_name((*relation)->rd_rel->relnamespace)),
                     quote_identifier((*relation)->rd_rel->relname.data));
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);

    /* Close delta log relation before finish xact. */
    operators->closeDeltaRelation(AccessExclusiveLock);

    /* restart another transaction, close relation */
    CommandCounterIncrement();
    PopActiveSnapshot();
    finish_xact_command();
    start_xact_command();
    PushActiveSnapshot(snapshot);

    /* reopen and lock relation */
    *relation = relation_open(operators->getRelId(), ShareUpdateExclusiveLock);
    relLockRelId = (*relation)->rd_lockInfo.lockRelId;
    UnlockRelationIdForSession(&relLockRelId, AccessExclusiveLock);
    operators->openDeltaRelation(ShareUpdateExclusiveLock);
    // ctid start from (0, 2)
    operators->recordTupleEmpty();

    /* init ctid map */
    operators->initCtidMapRelation();

    if (onlineDDLType == ONLINE_DDL_REWRITE || onlineDDLType == ONLINE_DDL_CHECK) {
    /* Alter table mode: START->PREPARE->REWRITE_CATALOG->BASE_LINE_COPY->CATCHUP->COMMITING->END */
        operators->setStatus(ONLINE_DDL_STATUS_REWRITE_CATALOG);
    } else {
        /* Vacuum full/ cluster mode: START->PREPARE->BASELINE_COPY->BASELINE_COPY->COMMITING->END */
        operators->setStatus(ONLINE_DDL_STATUS_BASELINE_COPY);
    }
    ereport(NOTICE, (errmsg("Online DDL instance init finish, start to copy baseline data.")));
    return true;
}

bool OnlineDDLInstanceFinish(Relation relation)
{
    OnlineDDLRelOperators* operators = RelationGetOnlineDDLOperators(relation);
    operators->setStatus(ONLINE_DDL_END);
    operators->closeDeltaRelation(ShareUpdateExclusiveLock);
    operators->closeCtidMapRelation(AccessShareLock);
    OnlineDDLResetAppendMode(&relation);
    OnlineDDLDropTempSchema(operators);
    OnlineDDLReleaseHashEntry(GetDDLGlobalHashKey(relation->rd_node, relation->rd_id), operators->getStartXid());
    relation->rd_online_ddl_operators = NULL;
    u_sess->online_ddl_operators = NULL;
    relation_close(relation, ShareUpdateExclusiveLock);
    return true;
}

void OnlineDDLCleanup()
{
    if (u_sess->online_ddl_operators == NULL) {
        return;
    }
    OnlineDDLRelOperators* operators = (OnlineDDLRelOperators*)u_sess->online_ddl_operators;
    if (operators->getStatus() == ONLINE_DDL_END) {
        return;
    }
    operators->setStatus(ONLINE_DDL_END);
    operators->closeDeltaRelation(ShareUpdateExclusiveLock);
    operators->closeCtidMapRelation(AccessShareLock);
    u_sess->online_ddl_operators = NULL;
}