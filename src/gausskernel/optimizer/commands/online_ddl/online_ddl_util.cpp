/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * online_ddl_util.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/online_ddl/online_ddl_util.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "parser/parser.h"
#include "parser/parse_utilcmd.h"
#include "utils/relcache.h"

#include "commands/online_ddl.h"
#include "commands/online_ddl_append.h"
#include "commands/online_ddl_ctid_map.h"
#include "commands/online_ddl_deltalog.h"
#include "commands/online_ddl_globalhash.h"
#include "commands/online_ddl_util.h"

void OnlineDDLExecuteCommand(char* query)
{
    List* parsetreeList = NULL;
    ListCell* parsetreeItem = NULL;
    List* query_string_locationlist = NULL;

    parsetreeList = pg_parse_query(query, &query_string_locationlist);
    if (parsetreeList == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("unexpected null parsetree list, during online ddl, sql: %s", query)));
    }
    foreach (parsetreeItem, parsetreeList) {
        instr_unique_sql_handle_multi_sql(parsetreeItem == list_head(parsetreeList));
        Node* parsetree = (Node*)lfirst(parsetreeItem);
        List* querytreeList = NULL;
        List* plantreeList = NULL;
        ListCell* statementlistItem = NULL;

        Node* statement = NULL;
        querytreeList = pg_analyze_and_rewrite(parsetree, query, NULL, 0);
        plantreeList = pg_plan_queries(querytreeList, 0, NULL);
        foreach (statementlistItem, plantreeList) {
            statement = (Node*)lfirst(statementlistItem);
            processutility_context proutility_cxt;
            proutility_cxt.parse_tree = statement;
            proutility_cxt.query_string = query;
            proutility_cxt.readOnlyTree = false;
            proutility_cxt.params = NULL;
            proutility_cxt.is_top_level = false;
            ProcessUtility(&proutility_cxt, None_Receiver, false, NULL, PROCESS_UTILITY_GENERATED);
            CommandCounterIncrement();
        }
    }
}

/**
 * Set the target relation to append mode.
 *
 * @param relation The relation to set append mode.
 * @return true if the operation is successful, false otherwise.
 */
void OnlineDDLEnableRelationAppendMode(Relation relation)
{
    OnlineDDLRelOperators* operators = (OnlineDDLRelOperators*)u_sess->online_ddl_operators;
    Assert(operators != NULL);
    if (operators == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("rd_online_ddl_operators is null, during online ddl.")));
    }

    if (relation->rd_rel->relkind != RELKIND_RELATION) {
        ereport(ERROR, (errmsg("Unsupported relkind '%c' for online DDL append mode", relation->rd_rel->relkind)));
    }

    if (RELATION_IS_PARTITIONED(relation) &&
        (!OnlineDDLIsVacuumOrClusterMode(operators) || OnlineDDLIsClusterAllPartition(operators))) {
        List* partitions = NULL;
        ListCell* cell = NULL;

        if (RelationIsSubPartitioned(relation)) {
            Assert(!OnlineDDLIsVacuumOrClusterMode(operators));
            partitions = relationGetPartitionList(relation, NoLock);
            foreach (cell, partitions) {
                Partition partition = (Partition)lfirst(cell);
                Relation partrel = partitionGetRelation(relation, partition);

                List* subpartitions = relationGetPartitionList(partrel, NoLock);
                ListCell* subcell = NULL;
                foreach (subcell, subpartitions) {
                    Partition subpartition = (Partition)lfirst(subcell);
                    Relation subpartrel = partitionGetRelation(partrel, subpartition);

                    ItemPointerData endCtid;
                    heap_get_max_tid(subpartrel, &endCtid);

                    operators->enableTargetRelationAppendMode(RelationGetRelid(subpartrel), endCtid);
                    releaseDummyRelation(&subpartrel);
                }
                releasePartitionList(partrel, &subpartitions, NoLock);
                releaseDummyRelation(&partrel);
            }
        } else {
            Assert(!OnlineDDLIsVacuumOrClusterMode(operators) || OnlineDDLIsClusterAllPartition(operators));
            partitions = relationGetPartitionList(relation, NoLock);
            foreach (cell, partitions) {
                Partition partition = (Partition)lfirst(cell);
                Relation partrel = partitionGetRelation(relation, partition);

                ItemPointerData endCtid;
                heap_get_max_tid(partrel, &endCtid);

                operators->enableTargetRelationAppendMode(RelationGetRelid(partrel), endCtid);
                releaseDummyRelation(&partrel);
            }
        }
        releasePartitionList(relation, &partitions, NoLock);
    } else if (RELATION_IS_PARTITIONED(relation) && (OnlineDDLIsVacuumOrClusterMode(operators))) {
        List* partitions = NULL;
        ListCell* cell = NULL;
        bool findOut = false;

        if (RelationIsSubPartitioned(relation)) {
            partitions = relationGetPartitionList(relation, NoLock);
            foreach (cell, partitions) {
                Partition partition = (Partition)lfirst(cell);
                if (findOut) {
                    continue;
                }
                Relation partrel = partitionGetRelation(relation, partition);

                List* subpartitions = relationGetPartitionList(partrel, NoLock);
                ListCell* subcell = NULL;
                foreach (subcell, subpartitions) {
                    Partition subpartition = (Partition)lfirst(subcell);
                    if (operators->getCurrentPartitionOid() != subpartition->pd_id || findOut) {
                        continue;
                    }
                    Relation subpartrel = partitionGetRelation(partrel, subpartition);
                    ItemPointerData endCtid;
                    heap_get_max_tid(subpartrel, &endCtid);

                    operators->enableTargetRelationAppendMode(endCtid);
                    releaseDummyRelation(&subpartrel);
                    findOut = true;
                }
                releasePartitionList(partrel, &subpartitions, NoLock);
                releaseDummyRelation(&partrel);
            }
        } else {
            partitions = relationGetPartitionList(relation, NoLock);
            foreach (cell, partitions) {
                Partition partition = (Partition)lfirst(cell);
                if (operators->getCurrentPartitionOid() != partition->pd_id) {
                    continue;
                }
                Relation partrel = partitionGetRelation(relation, partition);
                ItemPointerData endCtid;
                heap_get_max_tid(partrel, &endCtid);

                operators->enableTargetRelationAppendMode(endCtid);
                releaseDummyRelation(&partrel);
                findOut = true;
                break;
            }
        }
        releasePartitionList(relation, &partitions, NoLock);
        Assert(findOut);
    } else {
        ItemPointerData endCtid;
        heap_get_max_tid(relation, &endCtid);
        operators->enableTargetRelationAppendMode(endCtid);
    }
}

void OnlineDDLCopyRelationIndexs(Relation srcRelation, Relation destRelation, List** srcIndexOidList,
                                 List** destIndexOidList)
{
    if (srcIndexOidList == NULL || destIndexOidList == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                 errmsg("[Online-DDL] srcIndexOidList or destIndexOidList is null, during copy relation index.")));
    }
    if (srcRelation == NULL || destRelation == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("[Online-DDL] srcRelation or destRelation is invalid, during copy relation index.")));
    }
    Oid destRelationOid = destRelation->rd_id;

    if (RelationIsPartitioned(srcRelation)) {
        List* gobalIndexList = RelationGetSpecificKindIndexList(srcRelation, true);
        if (gobalIndexList != NIL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("[Online-DDL] Online copying global index is not supported.")));
        }
        *srcIndexOidList = RelationGetSpecificKindIndexList(srcRelation, false);
    } else {
        List* gobalIndexList = RelationGetSpecificKindIndexList(srcRelation, true);
        if (gobalIndexList != NIL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("[Online-DDL] Online copying global index is not supported.")));
        }
        *srcIndexOidList = RelationGetIndexList(srcRelation);
    }

    ListCell* cell = NULL;
    foreach (cell, *srcIndexOidList) {
        Oid dstIndexPartTblspcOid;
        Oid clonedIndexRelationId;
        char tmpIndexName[NAMEDATALEN];
        Relation currentIndex;
        bool skipBuild = false;
        errno_t rc = EOK;

        Oid srcIndexOid = lfirst_oid(cell);
        /* Open the src index relation. */
        currentIndex = index_open(srcIndexOid, AccessShareLock);
        Assert(IndexIsUsable(currentIndex->rd_index));
        if (OID_IS_BTREE(currentIndex->rd_rel->relam)) {
            skipBuild = true;
        } else {
            skipBuild = false;
        }
        /* build name for tmp index */
        rc = snprintf_s(tmpIndexName, sizeof(tmpIndexName), sizeof(tmpIndexName) - 1, "pg_tmp_%u_%u_index_online_ddl",
                        destRelationOid, currentIndex->rd_id);
        securec_check_ss_c(rc, "\0", "\0");
        dstIndexPartTblspcOid = destRelation->rd_rel->reltablespace;
        clonedIndexRelationId =
            generateClonedIndex(currentIndex, destRelation, tmpIndexName, dstIndexPartTblspcOid, skipBuild, false);
        *destIndexOidList = lappend_oid(*destIndexOidList, clonedIndexRelationId);
        ereport(ONLINE_DDL_LOG_LEVEL,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                 errmsg("[Online-DDL] copy index %s to relation %s success.", RelationGetRelationName(currentIndex),
                        RelationGetRelationName(destRelation))));
        index_close(currentIndex, AccessShareLock);
    }
}

static const int LOCKMODE_NUM = 4;
LOCKMODE ddlForbiddenLockmode[LOCKMODE_NUM] = {ShareLock, ShareRowExclusiveLock, ExclusiveLock, AccessExclusiveLock};
void OnlineDDLLockCheck(Oid relid)
{
    for (int i = 0; i < LOCKMODE_NUM; i++) {
        if (CheckLockRelationOid(relid, ddlForbiddenLockmode[i])) {
            ereport(
                WARNING,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                 errmsg("OnlineDDLLockCheck warning, relation %d has lock mode %d, which is forbidden in online ddl.",
                        relid, ddlForbiddenLockmode[i])));
        }
    }
    if (!CheckLockRelationOid(relid, ShareUpdateExclusiveLock)) {
        ereport(
            WARNING,
            (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
             errmsg("OnlineDDLLockCheck warning, relation %d has no lock mode ShareUpdateExclusiveLock in online ddl.",
                    relid)));
    }
}

void OnlineDDLLockCheck(Oid relationId, Oid partitionId)
{
    for (int i = 0; i < LOCKMODE_NUM; i++) {
        if (CheckLockPartitionOid(relationId, partitionId, ddlForbiddenLockmode[i])) {
            ereport(WARNING, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                              errmsg("OnlineDDLLockCheck warning, relation %d partition %d has lock mode %d, which is "
                                     "forbidden in online ddl.",
                                     relationId, partitionId, ddlForbiddenLockmode[i])));
        }
    }
    if (!CheckLockPartitionOid(relationId, partitionId, ShareUpdateExclusiveLock)) {
        ereport(WARNING, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                          errmsg("OnlineDDLLockCheck warning, relation %d partition %d has no lock mode "
                                 "ShareUpdateExclusiveLock in online ddl.",
                                 relationId, partitionId)));
    }
}

void OnlineDDLPartitionLockCleanup(Oid relationId, Oid partitionId)
{
    for (int i = 0; i < LOCKMODE_NUM; i++) {
        while (CheckLockPartitionOid(relationId, partitionId, ddlForbiddenLockmode[i])) {
            UnlockPartitionOid(relationId, partitionId, ddlForbiddenLockmode[i]);
        }
    }
}