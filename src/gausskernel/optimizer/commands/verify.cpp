/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * verify.cpp
 *    Check if the data file is damaged for anlayse verify commands.
 *
 * This file now includes verification code for ANLYSE VERIFY and
 * ANALYZE commands will be in analyze.cpp and vacuum.cpp.
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/commands/verify.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include <math.h>
#include "access/clog.h"
#include "access/cstore_rewrite.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pgxc_class.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "catalog/pg_hashbucket_fn.h"
#include "commands/cluster.h"
#include "commands/tablespace.h"
#include "commands/verify.h"
#include "vecexecutor/vecnodes.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "catalog/catalog.h"
#endif

static bool CheckVerifyPermission(Relation rel, Oid relid, bool isDatabaseMode);
static bool IsMainCatalogObjectForVerify(Oid relid);
static void DoGlobalVerifySpecialCatalogTable(VacuumStmt* stmt, bool sentToRemote, MemoryContext verifyContext);
static void DoVerifySpecialCatalogTableOtherNode(VacuumStmt* stmt, bool sentToRemote, MemoryContext verifyContext);
static void CheckVerifyRelation(Relation rel);
static bool IsInternalObject(Form_pg_class classtuple);
static List* GetVerifyRelidList(VacuumStmt* stmt);
static void DoGlobalVerifyRowRel(VacuumStmt* stmt, Oid relid, bool isDatabase);
static void DoGlobalVerifyColRel(VacuumStmt* stmt, Oid relid, bool isDatabase);
static void DoVerifyIndexRel(VacuumStmt* stmt, Oid indexRelid);
static void VerifyPartRel(VacuumStmt* stmt, Relation rel, Oid partOid, bool isRowTable, bool issubpartition = false);
static void VerifyPartIndexRels(VacuumStmt* stmt, Relation rel, Relation partitionRel);
static void VerifyPartIndexRel(VacuumStmt* stmt, Relation rel, Relation partitionRel, Oid indexOid);
static void VerifyIndexRels(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc = NULL);
static void VerifyIndexRel(VacuumStmt* stmt, Relation indexRel, VerifyDesc* checkCudesc = NULL);
static void VerifyRowRels(VacuumStmt* stmt, Relation parentRel, Relation rel);
static void VerifyRowRel(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc = NULL);
static bool VerifyRowRelFull(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc = NULL);
static bool VerifyRowRelFast(Relation rel, VerifyDesc* checkCudesc = NULL);
static bool VerifyRowRelComplete(Relation rel, VerifyDesc* checkCudesc = NULL);
static void VerifyColRels(VacuumStmt* stmt, Relation parentRel, Relation rel);
static void VerifyColRel(VacuumStmt* stmt, Relation rel);
static void VerifyColRelFast(Relation rel);
static void VerifyColRelComplete(Relation rel);
static void reportColVerifyFailed(
    Relation rel, bool isdesc = false, bool iscomplete = false, BlockNumber cuId = 0, int col = 0);

/*
 * MainCatalogRelid is used to analyse verify the main system tables.
 */
static const Oid MainCatalogRelid[] = {RelationRelationId,
    AttributeRelationId,
    NamespaceRelationId,
    TableSpaceRelationId,
    TypeRelationId,
    DatabaseRelationId,
    IndexRelationId,
    AuthIdRelationId,
    AuthMemRelationId,
    ProcedureRelationId,
    PartitionRelationId,
    UserStatusRelationId};

static void reportVerifyTableInfo(Oid relid)
{
    ereport(NOTICE,
        (errmsg("table \"%s.%s\" was verified", get_namespace_name(get_rel_namespace(relid)), get_rel_name(relid))));
}

/*
 * CheckVerifyPermission
 *
 * @Description: check the table's permission.
 * @in rel - the relation.
 * @in relid - the relation oid
 * @in isDatabaseMode - is database mode or on single table to verify.
 * @return: bool
 */
static bool CheckVerifyPermission(Relation rel, Oid relid, bool isDatabaseMode)
{
    /* check the permissions to verify */
    AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(relid, GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !rel->rd_rel->relisshared))) {
        /* ignore system tables, inheritors */
        if (isDatabaseMode && (relid < FirstNormalObjectId || IsInheritor(relid))) {
            return false;
        }

        if (rel->rd_rel->relisshared) {
            ereport(WARNING,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only system admin can verify it", RelationGetRelationName(rel))));
        } else if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE) {
            ereport(WARNING,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only system admin or database owner can verify it",
                        RelationGetRelationName(rel))));
        } else {
            ereport(WARNING,
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("skipping \"%s\" --- only table or database owner can verify it",
                        RelationGetRelationName(rel))));
        }
        return false;
    }

    return true;
}

/*
 * DoGlobalVerifyMppTable
 *
 * @Description: do verify relation file where come from client in local coordinator.
 * @in stmt - the statment for analyse verify command
 * @in queryString - the query string for analyse verify command
 * @in sentToRemote - identify if this statement has been sent to the nodes
 * @return: void
 */
void DoGlobalVerifyMppTable(VacuumStmt* stmt, const char* queryString, bool sentToRemote)
{
    Oid relid = InvalidOid;
    MemoryContext oldContext = NULL;
    MemoryContext verifyContext = NULL;

    /* Create new memcontext for verify. */
    verifyContext = AllocSetContextCreate(u_sess->temp_mem_cxt,
        "verify_context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    relid = RangeVarGetRelid(stmt->relation, NoLock, false);

    /*
     * Switch to global_stats_context because it only used to get sample from all datanodes
     * and we want to put this memory into this memory context.
     */
    oldContext = MemoryContextSwitchTo(verifyContext);

    Relation rel = relation_open(relid, AccessShareLock);

    /* detect the object type and filter the unsupport objects. */
    CheckVerifyRelation(rel);

    /* check the table's permissions for single table mode to verify */
    if (!CheckVerifyPermission(rel, relid, false)) {
        relation_close(rel, AccessShareLock);
        (void)MemoryContextSwitchTo(oldContext);
        MemoryContextDelete(verifyContext);
        return;
    }

    if (RelationIsRowFormat(rel)) {
        if (IsSystemNamespace(RelationGetNamespace(rel)) && relid < FirstNormalObjectId) {
            relation_close(rel, NoLock);
            /* All the coordinator and datanodes work */
            ExecUtilityStmtOnNodes(queryString, NULL, sentToRemote, true, EXEC_ON_ALL_NODES, false, (Node*)stmt);

            DoGlobalVerifyRowRel(stmt, relid, false);
        } else {
            relation_close(rel, NoLock);
            ExecUtilityStmtOnNodes(queryString, NULL, sentToRemote, true, EXEC_ON_DATANODES, false, (Node*)stmt);
        }
    } else if (RelationIsCUFormat(rel)) {
        relation_close(rel, NoLock);

        /* All the datanodes work */
        ExecUtilityStmtOnNodes(queryString, NULL, sentToRemote, true, EXEC_ON_DATANODES, false, (Node*)stmt);
    } else if (RelationIsIndex(rel)) {
        relation_close(rel, NoLock);

        if (stmt->isCascade) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The index table does not support verify on cascade mode.")));
        }

        /* All the datanodes work */
        ExecUtilityStmtOnNodes(queryString, NULL, sentToRemote, true, EXEC_ON_DATANODES, false, (Node*)stmt);
    } else {
        relation_close(rel, AccessShareLock);
        ereport(LOG,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("The relation %u is not a row or column table", relid)));
    }

    /*
     * We have finished analyze for one relation, so we should switch to global_stats_context
     * in order to do analyze for next relation.
     */
    (void)MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(verifyContext);
    return;
}

/*
 * IsMainCatalogObject
 *
 * @Description: filter the 11 main catalog objects because we will analyse verify them at first.
 * @in Oid- the relation's oid.
 * @return: bool
 */
static bool IsMainCatalogObjectForVerify(Oid relid)
{
    for (int i = 0; (unsigned int)i < lengthof(MainCatalogRelid); i++) {
        if (relid == MainCatalogRelid[i]) {
            return true;
        }
    }
    return false;
}

static void generateQuery(VacuumStmt* stmt, StringInfoData* tmpQueryString, Relation rel)
{
    initStringInfo(tmpQueryString);
    if ((unsigned int)stmt->options & VACOPT_FAST) {
        appendStringInfo(tmpQueryString,
            "ANALYSE VERIFY FAST %s.%s CASCADE",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel));
    } else if ((unsigned int)stmt->options & VACOPT_COMPLETE) {
        appendStringInfo(tmpQueryString,
            "ANALYSE VERIFY COMPLETE %s.%s CASCADE",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel));
    }
}

/*
 * DoGlobalVerifySpecialCatalogTable
 *
 * @Description: do verify database where come from client in local coordinator and send remote query to others.
 * @in stmt - the statment for analyse verify command
 * @in sentToRemote - identify if this statement has been sent to the nodes
 * @in verifyContext - the memory context for verify.
 * @return: void
 */
static void DoGlobalVerifySpecialCatalogTable(VacuumStmt* stmt, bool sentToRemote, MemoryContext verifyContext)
{
    MemoryContext oldContext = CurrentMemoryContext;
    Oid relid = InvalidOid;

    (void)MemoryContextSwitchTo(verifyContext);

    /* Loop to process each selected catalog relation. */
    for (int i = 0; (unsigned int)i < lengthof(MainCatalogRelid); i++) {
        relid = MainCatalogRelid[i];
        StringInfoData tmpQueryString;

        StartTransactionCommand();
        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());

        Relation rel = relation_open(relid, AccessShareLock);
        /* check the permissions to verify */
        if (!CheckVerifyPermission(rel, relid, true)) {
            relation_close(rel, AccessShareLock);

            if (ActiveSnapshotSet()) {
                PopActiveSnapshot();
            }
            CommitTransactionCommand();

            continue;
        }

        /* on database mode, append querystring for every relation to send remote query. */
        generateQuery(stmt, &tmpQueryString, rel);

        /* Set the current relid for stmt. On databse mode, we need to fetch the nodes for every relation. */
        stmt->curVerifyRel = relid;
        relation_close(rel, NoLock);

        /* All the coordinator and datanodes work */
        ExecUtilityStmtOnNodes(tmpQueryString.data, NULL, sentToRemote, true, EXEC_ON_ALL_NODES, false, (Node*)stmt);
        DoGlobalVerifyRowRel(stmt, relid, true);
        reportVerifyTableInfo(relid);

        PopActiveSnapshot();
        CommitTransactionCommand();

        /*
         * Verify a big table or verify database may consume a lot of memeory, so we reset
         * the memory after verify each relation in order to avoid memory increase.
         */
        MemoryContextReset(verifyContext);

        /*
         * We have finished analyze for one relation, so we should switch to global_stats_context
         * in order to do analyze for next relation.
         */
        (void)MemoryContextSwitchTo(verifyContext);
    }

    (void)MemoryContextSwitchTo(oldContext);

    return;
}

/*
 * DoGlobalVerifyDatabase
 *
 * @Description: do verify database where come from client in local coordinator.
 * @in stmt - the statment for analyse verify command
 * @in queryString - the query string for analyse verify command
 * @in sentToRemote - identify if this statement has been sent to the nodes
 * @return: void
 */
void DoGlobalVerifyDatabase(VacuumStmt* stmt, const char* queryString, bool sentToRemote)
{
    List* oidList = NIL;
    ListCell* cur = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    MemoryContext verifyContext = NULL;

    /* Create new memcontext for verify. */
    verifyContext = AllocSetContextCreate(u_sess->temp_mem_cxt,
        "verify_context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    Assert(stmt->relation == NULL);
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* we need to check special catalog table first. Then we can scan pg_class to get oid list. */
    DoGlobalVerifySpecialCatalogTable(stmt, sentToRemote, verifyContext);

    StartTransactionCommand();
    /* functions in indexes may want a snapshot set */
    PushActiveSnapshot(GetTransactionSnapshot());

    (void)MemoryContextSwitchTo(oldContext);
    oidList = GetVerifyRelidList(stmt);

    PopActiveSnapshot();
    CommitTransactionCommand();
    /* Now verify each rel in a separate transaction */
    (void)MemoryContextSwitchTo(verifyContext);
    /* Loop to process each selected relation. */
    foreach (cur, oidList) {
        Oid relid = lfirst_oid(cur);
        StringInfoData tmpQueryString;
        bool canVerify = true;

        StartTransactionCommand();
        /* functions in indexes may want a snapshot set */
        PushActiveSnapshot(GetTransactionSnapshot());

        Relation rel = relation_open(relid, AccessShareLock);
        if (RelationIsForeignTable(rel) || RelationIsStream(rel)) {
            ereport(LOG, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("The hdfs table does not support verify.")));
            canVerify = false;
        }

        /* check the every table's permissions for database to verify */
        if (!canVerify || !CheckVerifyPermission(rel, relid, true)) {
            relation_close(rel, AccessShareLock);

            if (ActiveSnapshotSet()) {
                PopActiveSnapshot();
            }
            CommitTransactionCommand();

            continue;
        }

        /* on database mode, append querystring for every relation to send remote query. */
        generateQuery(stmt, &tmpQueryString, rel);

        /* Set the current relid for stmt. On databse mode, we need to fetch the nodes for every relation. */
        stmt->curVerifyRel = relid;
        if (IsSystemNamespace(RelationGetNamespace(rel)) && relid < FirstNormalObjectId) {
            relation_close(rel, NoLock);
            /* All the coordinator and datanodes work */
            ExecUtilityStmtOnNodes(
                tmpQueryString.data, NULL, sentToRemote, true, EXEC_ON_ALL_NODES, false, (Node*)stmt);
            DoGlobalVerifyRowRel(stmt, relid, true);
        } else {
            relation_close(rel, NoLock);
            ExecUtilityStmtOnNodes(
                tmpQueryString.data, NULL, sentToRemote, true, EXEC_ON_DATANODES, false, (Node*)stmt);
        }
        reportVerifyTableInfo(relid);

        PopActiveSnapshot();
        CommitTransactionCommand();

        /*
         * Verify a big table or verify database may consume a lot of memeory, so we reset
         * the memory after verify each relation in order to avoid memory increase.
         */
        MemoryContextReset(verifyContext);

        /*
         * We have finished analyze for one relation, so we should switch to global_stats_context
         * in order to do analyze for next relation.
         */
        (void)MemoryContextSwitchTo(verifyContext);
    }

    (void)MemoryContextSwitchTo(oldContext);
    StartTransactionCommand();
    MemoryContextDelete(verifyContext);
    return;
}

/*
 * DoVerifySpecialCatalogTableOtherNode
 *
 * @Description: do verify database where come from client in other node.
 * @in stmt - the statment for analyse verify command
 * @in sentToRemote - identify if this statement has been sent to the nodes
 * @in verifyContext - the memory context for verify.
 * @return: void
 */
static void DoVerifySpecialCatalogTableOtherNode(VacuumStmt* stmt, bool sentToRemote, MemoryContext verifyContext)
{
    Oid relid = InvalidOid;
    MemoryContext oldContext = CurrentMemoryContext;

    /*
     * Switch to verifyContext because it only used to get sample from all datanodes
     * and we want to put this memory into this memory context.
     */
    (void)MemoryContextSwitchTo(verifyContext);

    /* Loop to process each selected catalog relation. */
    for (int i = 0; (unsigned int)i < lengthof(MainCatalogRelid); i++) {
        relid = MainCatalogRelid[i];
        Relation rel = relation_open(relid, AccessShareLock);
        /* check the permissions to verify */
        if (!CheckVerifyPermission(rel, relid, true)) {
            relation_close(rel, AccessShareLock);
            continue;
        }

        relation_close(rel, NoLock);
        DoGlobalVerifyRowRel(stmt, relid, true);
        reportVerifyTableInfo(relid);

        /*
         * verify a big table or verify database may consume a lot of memeory, so we reset
         * the memory after verify each relation in order to avoid memory increase.
         */
        (void)MemoryContextSwitchTo(verifyContext);

        /*
         * We have finished verify for one relation, so we should switch to global_stats_context
         * in order to do analyze for next relation.
         */
        MemoryContextReset(verifyContext);
    }
    (void)MemoryContextSwitchTo(oldContext);
    return;
}

/*
 * DoVerifyTableOtherNode
 *
 * @Description: do verify table on other nodes.
 * @in stmt - the statment for analyse verify command
 * @in sentToRemote - identify if this statement has been sent to the nodes
 * @return: void
 */
void DoVerifyTableOtherNode(VacuumStmt* stmt, bool sentToRemote)
{
    List* oidList = NIL;
    ListCell* cur = NULL;
    Oid relOid = InvalidOid;
    bool isDatabase = false;
    MemoryContext oldContext = CurrentMemoryContext;
    MemoryContext verifyContext = NULL;

    /* Create new memcontext for verify. */
    verifyContext = AllocSetContextCreate(u_sess->temp_mem_cxt,
        "verify_context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* we need to check special catalog table first. Then we can scan pg_class to get oid list. */
    if (stmt->relation == NULL) {
        DoVerifySpecialCatalogTableOtherNode(stmt, sentToRemote, verifyContext);
        oidList = GetVerifyRelidList(stmt);
        isDatabase = true;
    } else {
        relOid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
        oidList = lappend_oid(oidList, relOid);
    }

    /*
     * Switch to global_stats_context because it only used to get sample from all datanodes
     * and we want to put this memory into this memory context.
     */
    oldContext = MemoryContextSwitchTo(verifyContext);

    /* Loop to process each selected relation. */
    foreach (cur, oidList) {
        Oid relid = lfirst_oid(cur);
        Relation rel = relation_open(relid, AccessShareLock);
        if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
        } else if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !validateTempNamespace(rel->rd_rel->relnamespace)) {
            relation_close(rel, AccessShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                    errmsg("Temp table's data is invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.", 
                       g_instance.attr.attr_common.PGXCNodeName)));
        }

        if (isDatabase) {
            /* check the permissions to verify */
            if (!CheckVerifyPermission(rel, relid, isDatabase)) {
                relation_close(rel, AccessShareLock);
                continue;
            }

            /*
             * On database mode, we will only check the main row/col table. The related table will be
             * check on DoGlobalVerifyRowRel or DoGlobalVerifyColRel.
             */
            if (RelationIsRowFormat(rel)) {
                relation_close(rel, NoLock);
                DoGlobalVerifyRowRel(stmt, relid, isDatabase);
            } else if (RelationIsCUFormat(rel)) {
                relation_close(rel, NoLock);
                DoGlobalVerifyColRel(stmt, relid, isDatabase);
            }
            reportVerifyTableInfo(relid);
        } else {
            /*
             * The commond "analyse verify fast|complete index_rel|toast_rel" will come into this branch.
             * And on the database mode it will never go into this branch because index|toast will
             * be checked too in its data relation.
             */
            if (!CheckVerifyPermission(rel, relid, isDatabase)) {
                relation_close(rel, AccessShareLock);
                (void)MemoryContextSwitchTo(oldContext);
                MemoryContextDelete(verifyContext);
                return;
            }

            if (RelationIsRowFormat(rel) || RelationIsToast(rel)) {
                relation_close(rel, NoLock);
                DoGlobalVerifyRowRel(stmt, relid, isDatabase);
            } else if (RelationIsCUFormat(rel)) {
                relation_close(rel, NoLock);
                DoGlobalVerifyColRel(stmt, relid, isDatabase);
            } else if (RelationIsIndex(rel)) {
                relation_close(rel, NoLock);

                if (stmt->isCascade) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("The index table does not support verify on cascade mode.")));
                }

                DoVerifyIndexRel(stmt, relid);
            } else {
                relation_close(rel, AccessShareLock);
                ereport(LOG,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("The relation %u is not a row or column table", relid)));
            }
        }

        /*
         * verify a big table or verify database may consume a lot of memeory, so we reset
         * the memory after verify each relation in order to avoid memory increase.
         */
        (void)MemoryContextSwitchTo(verifyContext);

        /*
         * We have finished verify for one relation, so we should switch to global_stats_context
         * in order to do analyze for next relation.
         */
        MemoryContextReset(verifyContext);
    }
    (void)MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(verifyContext);
    return;
}

/*
 * CheckVerifyRelation
 *
 * @Description: detect the object type and filter the unsupport objects.
 * @in rel- the relation for check.
 * @return: void
 */
static void CheckVerifyRelation(Relation rel)
{
    if (RelationIsView(rel) || RelationIsSequnce(rel) || RelationIsContquery(rel)) {
        relation_close(rel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Non-table objects do not support verify.")));
    }

    if (RelationIsForeignTable(rel) || RelationIsStream(rel)) {
        relation_close(rel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("The hdfs table does not support verify.")));
    }

    if (RelationIsLocalTemp(rel) || RelationIsUnlogged(rel)) {
        relation_close(rel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("The temporary/unlog table does not support verify.")));
    }
}

/*
 * IsInternalObject
 *
 * @Description: filter the internal objects because we will check them on their main table.
 * @in classtuple- Form_pg_class. Scan the class tuple.
 * @return: bool
 */
static bool IsInternalObject(Form_pg_class classtuple)
{
    /* check the internal toast table including catalog's toast object. */
    if (IsToastClass(classtuple) && pg_strncasecmp(NameStr(classtuple->relname), "pg_toast", strlen("pg_toast")) == 0) {
        return true;
    }

    /* check the internal cudesc table */
    if (IsCStoreNamespace(classtuple->relnamespace) &&
        pg_strncasecmp(NameStr(classtuple->relname), "pg_cudesc", strlen("pg_cudesc")) == 0) {
        return true;
    }

    /* check the internal delta table */
    if (IsCStoreNamespace(classtuple->relnamespace) &&
        pg_strncasecmp(NameStr(classtuple->relname), "pg_delta", strlen("pg_delta")) == 0) {
        return true;
    }

    /* check the internal psort index table */
    if (IsCStoreNamespace(classtuple->relnamespace) &&
        pg_strncasecmp(NameStr(classtuple->relname), "psort_", strlen("psort_")) == 0) {
        return true;
    }

    return false;
}

/*
 * GetVerifyRelidList
 *
 * @Description: get the relation list including the oridinary system table and the oridinary table.It will not
 *               deal with the toast tables , cudesc tables, delta tables and other internal tables. The special
 *               catalog tables will be check used GetVerifyMainCatalogRelidList.
 * @in stmt - the statment for analyse verify command
 * @return: oid list.
 */
static List* GetVerifyRelidList(VacuumStmt* stmt)
{
    Relation relationRelation;
    TableScanDesc scan;
    HeapTuple tuple;
    List* oidList = NIL;
    List* oidTempList = NIL;

    if (stmt->relation == NULL) {
        /*
         * Scan pg_class to build a list of the relations we need to reindex.
         *
         * We only consider plain relations here (toast rels will be processed
         * indirectly by ReindexRelation).
         */
        relationRelation = heap_open(RelationRelationId, AccessShareLock);
        scan = tableam_scan_begin(relationRelation, GetActiveSnapshot(), 0, NULL);
        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Form_pg_class classtuple = (Form_pg_class)GETSTRUCT(tuple);
            if (classtuple->relkind != RELKIND_RELATION) {
                continue;
            }

            /* Skip temp tables of other backends; we can't verify them at all */
            if ((classtuple->relpersistence == RELPERSISTENCE_TEMP && !isTempNamespace(classtuple->relnamespace)) ||
                (classtuple->relpersistence == RELPERSISTENCE_UNLOGGED)) {
                continue;
            }

            /* we will skip the all internal table including calalog table's toast relation. */
            if (IsInternalObject(classtuple)) {
                continue;
            }

            /* got it already */
            if (IsMainCatalogObjectForVerify(HeapTupleGetOid(tuple))) {
                continue;
            }

            /* The catalog table will be verified first. */
            if (IsCatalogClass(HeapTupleGetOid(tuple), classtuple)) {
                oidList = lappend_oid(oidList, HeapTupleGetOid(tuple));
            } else {
                oidTempList = lappend_oid(oidTempList, HeapTupleGetOid(tuple));
            }
        }

        /* add each ordinary relation to the oidList in order to check system first, then check the ordinary table. */
        oidList = list_concat(oidList, oidTempList);

        tableam_scan_end(scan);
        heap_close(relationRelation, AccessShareLock);
    }
    return oidList;
}

/*
 * DoGlobalVerifyRowRel
 *
 * @Description: do verify row table file including partition table and ordinary table. Cascade mode is means all
 *               related table will be verified. otherwise, just the data related table will be verified.
 * @in stmt - the statment for analyse verify command
 * @in relid - the relation oid
 * @in isDatabase - the command is to verify the database or the relation
 * @return: void
 *
 * step 1: judging the scene:1)one parition;2)ordinary table;3)parition table.
 * step 2: analyse verify the main table.
 * step 3: check the toast table.
 * step 4: check the cascade mode: check the index table and the other related table.
 */
static void DoGlobalVerifyRowRel(VacuumStmt* stmt, Oid relid, bool isDatabase)
{
    Relation rel;
    rel = relation_open(relid, AccessShareLock);

    /*
     * In this branch, the query string is 'analyse verify fast|complete table partition part_name;'
     * on the database mode, it will be not to go into this branch.
     */
    if (!isDatabase) {
        if (stmt->relation->partitionname != NULL) {
            /* check the partition relation */
            Oid partOid = PartitionNameGetPartitionOid(relid,
                stmt->relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                AccessShareLock,
                false,
                false,
                NULL,
                NULL,
                NoLock);
            Partition part = partitionOpen(rel, partOid, AccessShareLock);
            if (PartitionHasSubpartition(part)) {
                Relation partrel = partitionGetRelation(rel, part);
                Oid subpartOid;
                ListCell* cell = NULL;
                List* partOidList = relationGetPartitionOidList(partrel);
                foreach (cell, partOidList) {
                    subpartOid = lfirst_oid(cell);
                    VerifyPartRel(stmt, rel, subpartOid, true, true);
                }
                releaseDummyRelation(&partrel);
            } else {
                VerifyPartRel(stmt, rel, partOid, true);
            }

            partitionClose(rel, part, AccessShareLock);
            relation_close(rel, AccessShareLock);
            return;
        }
    }

    if (RelationIsSubPartitioned(rel)) {
        Oid partOid;
        ListCell* cell = NULL;
        List* partOidList = RelationGetSubPartitionOidList(rel);
        foreach (cell, partOidList) {
            partOid = lfirst_oid(cell);
            VerifyPartRel(stmt, rel, partOid, true, true);
        }
    } else if (RELATION_IS_PARTITIONED(rel)) {
        Oid partOid;
        ListCell* cell = NULL;
        List* partOidList = relationGetPartitionOidList(rel);

        foreach (cell, partOidList) {
            partOid = lfirst_oid(cell);
            VerifyPartRel(stmt, rel, partOid, true);
        }
    } else {
        /* check the ordinary table */
        VerifyRowRels(stmt, NULL, rel);
    }
    relation_close(rel, AccessShareLock);
    return;
}

/*
 * DoGlobalVerifyColRel
 *
 * @Description: do verify row table file including partition table and ordinary table. Cascade mode is means all
 *               related table will be verified. otherwise, just the data related table will be verified.
 * @in stmt - the statment for analyse verify command
 * @in relid - the relation oid
 * @in isDatabase - the command is to verify the database or the relation
 * @return: void
 *
 * step 1: judging the scene:1)one parition;2)ordinary table;3)parition table.
 * step 2: check the cudesc table, including toast table and index table.
 * step 3. analyse verify the main table.
 * step 4: check delta table, including toast table and index table.
 * step 5: check the cascade mode: check the index table and the other related table.
 */
static void DoGlobalVerifyColRel(VacuumStmt* stmt, Oid relid, bool isDatabase)
{
    Relation rel = relation_open(relid, AccessShareLock);

    /*
     * In this branch, the query string is 'analyse verify fast|complete table partition part_name;'
     * on the database mode, it will be not to go into this branch.
     */
    if (!isDatabase) {
        if (stmt->relation->partitionname != NULL) {
            /* check the partition relation */
            Oid partOid = PartitionNameGetPartitionOid(relid,
                stmt->relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                AccessShareLock,
                false,
                false,
                NULL,
                NULL,
                NoLock);
            VerifyPartRel(stmt, rel, partOid, false);

            relation_close(rel, AccessShareLock);
            return;
        }
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        Oid partOid;
        ListCell* cell = NULL;
        List* partOidList = relationGetPartitionOidList(rel);

        foreach (cell, partOidList) {
            partOid = lfirst_oid(cell);
            VerifyPartRel(stmt, rel, partOid, false);
        }
    } else {
        /* check the ordinary table */
        VerifyColRels(stmt, NULL, rel);
    }
    relation_close(rel, AccessShareLock);
    return;
}

/*
 * DoVerifyIndexRel
 *
 * @Description: do verify index table file including partition table(beacause the index is the local index) and
                 ordinary table. And on the database mode it will never go into this branch because index will
                 be checked too in its data relation.
 * @in stmt - the statment for analyse verify command
 * @in indexRelid - the index relation oid
 * @return: void
 *
 */
static void DoVerifyIndexRel(VacuumStmt* stmt, Oid indexRelid)
{
    Oid heapOid = IndexGetRelation(indexRelid, false);
    Relation heapRel = heap_open(heapOid, AccessShareLock);
    Relation indexRel = index_open(indexRelid, AccessShareLock);
    if (!RelationIsPartitioned(heapRel) || RelationIsGlobalIndex(indexRel)) {
        VerifyIndexRel(stmt, indexRel);
        index_close(indexRel, AccessShareLock);
    } else {
        index_close(indexRel, AccessShareLock);
        Partition part;
        Relation partRel;
        ListCell* cell = NULL;
        List* partOidList = relationGetPartitionOidList(heapRel);

        foreach (cell, partOidList) {
            Oid partRelOid = lfirst_oid(cell);

            /* check the main partition table. */
            part = partitionOpen(heapRel, partRelOid, AccessShareLock);
            partRel = partitionGetRelation(heapRel, part);

            VerifyPartIndexRel(stmt, heapRel, partRel, indexRelid);

            partitionClose(heapRel, part, AccessShareLock);
            releaseDummyRelation(&partRel);
        }
    }

    heap_close(heapRel, AccessShareLock);

    return;
}

/*
 * VerifyPartRel
 *
 * @Description: do verify one part table file. Cascade mode is means all related table will be verified.
 *               otherwise, just the data related table will be verified.
 * @in stmt - the statment for analyse verify command
 * @in rel - the main relation
 * @in partOid - the part table oid
 * @in isRowTable - judge the table is row table or column table
 * @return: void
 */
static void VerifyPartRel(VacuumStmt* stmt, Relation rel, Oid partOid, bool isRowTable, bool issubpartition)
{
    Partition part = NULL;
    Relation partitionRel = NULL;
    Oid subparentid = InvalidOid;
    Partition parentpart = NULL;
    Relation parentpartitionRel = NULL;
    if (!issubpartition) {
        part = partitionOpen(rel, partOid, AccessShareLock);
        partitionRel = partitionGetRelation(rel, part);
    } else {
        subparentid = partid_get_parentid(partOid);
        parentpart = partitionOpen(rel, subparentid, AccessShareLock);
        parentpartitionRel = partitionGetRelation(rel, parentpart);
        part = partitionOpen(parentpartitionRel, partOid, AccessShareLock);
        partitionRel = partitionGetRelation(parentpartitionRel, part);
    }

    /* check the main partition table. */
    PG_TRY();
    {
        if (isRowTable) {
            VerifyRowRels(stmt, rel, partitionRel);
        } else {
            VerifyColRels(stmt, rel, partitionRel);
        }
    }
    PG_CATCH();
    {
        if (geterrcode() == ERRCODE_DATA_CORRUPTED || geterrcode() == ERRCODE_INTERNAL_ERROR) {
            FlushErrorState();
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Partition table verification failed. The node is %s, invalid page|CU of relation %s.%s. "
                           "The partition name is %s, partition Oid is %u.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        get_namespace_name(RelationGetNamespace(rel)),
                        RelationGetRelationName(rel),
                        PartitionGetPartitionName(part),
                        partOid),
                    handle_in_client(true)));
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    if (!issubpartition) {
        releaseDummyRelation(&partitionRel);
        partitionClose(rel, part, AccessShareLock);
    } else {
        releaseDummyRelation(&partitionRel);
        partitionClose(parentpartitionRel, part, AccessShareLock);
        releaseDummyRelation(&parentpartitionRel);
        partitionClose(rel, parentpart, AccessShareLock);
    }
    return;
}

/*
 * VerifyPartIndexRels
 *
 * @Description: do verify one part relation's local index on all main relation index list.
 * @in stmt - the statment for analyse verify command
 * @in rel - the main relation for get its global index oid list.
 * @in partitionRel - the main part relation for get part index oid list.
 * @return: void
 */
static void VerifyPartIndexRels(VacuumStmt* stmt, Relation rel, Relation partitionRel)
{
    List* indexIds = NIL;
    ListCell* indexId = NULL;

    /* check all index tables of the main relation. */
    indexIds = RelationGetIndexList(rel);
    foreach (indexId, indexIds) {
        Oid indexOid = lfirst_oid(indexId);
        Relation indexRel = index_open(indexOid, AccessShareLock);
        if (RelationIsGlobalIndex(indexRel)) {
            VerifyIndexRel(stmt, indexRel);
            index_close(indexRel, AccessShareLock);
            continue;
        }
        index_close(indexRel, AccessShareLock);
        VerifyPartIndexRel(stmt, rel, partitionRel, indexOid);
    }
    return;
}

/*
 * VerifyPartIndexRel
 *
 * @Description: do verify one index for one partition rel. The index means the main relation's EXTERANL index.
 * @in stmt - the statment for analyse verify command
 * @in rel - the main relation for get its global index oid list.
 * @in partitionRel - the main part relation for get part index oid list.
 * @in indexOid - the main relation's external index oid.
 * @return: void
 */
static void VerifyPartIndexRel(VacuumStmt* stmt, Relation rel, Relation partitionRel, Oid indexOid)
{
    Relation pgPartitionRel;
    Relation indexRel;
    Partition indexPart;
    Relation indexPartRel;
    SysScanDesc partScan;
    HeapTuple partTuple;
    Form_pg_partition partForm;
    ScanKeyData partKey;
    Oid indexPartOid = InvalidOid;
    Oid partOid = partitionRel->rd_id;

    indexRel = index_open(indexOid, AccessShareLock);

    /*
     * Find the tuple in pg_partition whose 'indextblid' is partOid
     * and 'parentid' is indexId with systable scan.
     */
    pgPartitionRel = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&partKey, Anum_pg_partition_indextblid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partOid));

    partScan = systable_beginscan(pgPartitionRel, PartitionIndexTableIdIndexId, true, GetActiveSnapshot(), 1, &partKey);

    while ((partTuple = systable_getnext(partScan)) != NULL) {
        partForm = (Form_pg_partition)GETSTRUCT(partTuple);
        if (partForm->parentid == indexOid) {
            indexPartOid = HeapTupleGetOid(partTuple);
            break;
        }
    }

    /* End scan and close pg_partition */
    systable_endscan(partScan);
    heap_close(pgPartitionRel, AccessShareLock);

    indexPart = partitionOpen(indexRel, indexPartOid, AccessShareLock);
    indexPartRel = partitionGetRelation(indexRel, indexPart);

    /* check index table. */
    VerifyIndexRel(stmt, indexPartRel);

    partitionClose(indexRel, indexPart, AccessShareLock);
    releaseDummyRelation(&indexPartRel);

    index_close(indexRel, AccessShareLock);

    return;
}

/*
 * VerifyIndexRels
 *
 * @Description: do verify the index list for one table.
 * @in stmt - the statment for analyse verify command
 * @in rel - the main relation for get its index oid list.
 * @in&out checkCudesc - checkCudesc is a struct to judge whether cudesc tables is damaged.
 * @return: void
 */
static void VerifyIndexRels(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc)
{
    List* indexIds = NIL;
    ListCell* indexId = NULL;

    /* check all index tables. */
    indexIds = RelationGetIndexList(rel);
    foreach (indexId, indexIds) {
        Oid indexOid = lfirst_oid(indexId);
        Relation indexRel = index_open(indexOid, AccessShareLock);

        /* check index table. */
        VerifyIndexRel(stmt, indexRel, checkCudesc);

        index_close(indexRel, AccessShareLock);
    }
    return;
}

/*
 * VerifyIndexRel
 *
 * @Description: do verify the index relation.
 * @in stmt - the statment for analyse verify command.
 * @in indexRel - the index relation. The psort index is a special index table, it will have its column table and
 *           internal table.
 * @in&out checkCudesc - checkCudesc is a struct to judge whether cudesc tables is damaged.
 * @return: void
 */
static void VerifyIndexRel(VacuumStmt* stmt, Relation indexRel, VerifyDesc* checkCudesc)
{
    PG_TRY();
    {
        if (indexRel->rd_rel->relam == PSORT_AM_OID) {
            VerifyRowRel(stmt, indexRel);

            /*
             * psort index is a special index table, it will include its column table and its column table's
             * cudesc table|delta table|index table.
             */
            Oid psortColOid = indexRel->rd_rel->relcudescrelid;
            Relation psortColRel = relation_open(psortColOid, AccessShareLock);
            VerifyColRels(stmt, NULL, psortColRel);
            relation_close(psortColRel, AccessShareLock);
        } else {
            VerifyRowRel(stmt, indexRel, checkCudesc);
        }
    }
    PG_CATCH();
    {
        if (geterrcode() == ERRCODE_DATA_CORRUPTED || geterrcode() == ERRCODE_INTERNAL_ERROR) {
            FlushErrorState();
            ereport(WARNING,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("Index verification failed. The node is %s, the index rel is %s.%s. please reindex it.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        get_namespace_name(RelationGetNamespace(indexRel)),
                        RelationGetRelationName(indexRel)),
                    handle_in_client(true)));
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return;
}

/*
 * VerifyRowRels
 *
 * @Description: do verify one row_relation. we need to check the main relation and its toast relation.
                 if the command is cascade mode we also need to verify index list.
 * @in stmt - the statment for analyse verify command
 * @in parentRel - the parent relation. For partition table, it means the main table. For ordinary table,
                   it means NULL.
 * @in rel - the child relation.
 * @return: void
 */
static void VerifyRowRels(VacuumStmt* stmt, Relation parentRel, Relation rel)
{
    Oid toastRelid;
    Relation toastRel;
    Oid toastIdxRelid;
    Relation toastIdxRel;

    /* check the main table. */
    VerifyRowRel(stmt, rel);

    /* check the toast table and its index table */
    toastRelid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(toastRelid)) {
        toastRel = relation_open(toastRelid, AccessShareLock);
        VerifyRowRel(stmt, toastRel);

        toastIdxRelid = toastRel->rd_rel->reltoastidxid;
        toastIdxRel = index_open(toastIdxRelid, AccessShareLock);
        VerifyIndexRels(stmt, toastIdxRel);
        index_close(toastIdxRel, AccessShareLock);

        relation_close(toastRel, AccessShareLock);
    }

    /* isCascade to judge checking the all tables or just checking the data file. */
    if (stmt->isCascade) {
        /* check rel's index list */
        if (parentRel == NULL) {
            VerifyIndexRels(stmt, rel);
        } else if (RELATION_IS_PARTITIONED(parentRel)) {
            VerifyPartIndexRels(stmt, parentRel, rel);
        }
    }
    return;
}

/*
 * VerifyRowRel
 *
 * @Description: do verify one row_relation including two modes: FAST & COMPLETE. In the fast mode, the page header and
 *               crc check will be checked on physical damage. In the complete mode,the tuple will be checked one by
 * one.
 * @in stmt - the statment for analyse verify command
 * @in rel - the relation
 * @in&out checkCudesc - checkCudesc is a struct to judge whether cudesc tables is damaged.
 * @return: void
 */
static void VerifyRowRel(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc)
{
    /* turn off the remote read and keep the old mode */
    int oldRemoteReadMode = SetRemoteReadModeOffAndGetOldMode();
    bool isValidRelationPage = true;
    Oid relid = RelationGetRelid(rel);

    isValidRelationPage = VerifyRowRelFull(stmt, rel, checkCudesc);
    SetRemoteReadMode(oldRemoteReadMode);

    if (!isValidRelationPage && IsMainCatalogObjectForVerify(relid)) {
        ereport(FATAL,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("The important catalog table %s.%s corrupts, the node is %s, please fix it.",
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel),
                    g_instance.attr.attr_common.PGXCNodeName),
                handle_in_client(true)));
    }

    return;
}

/*
 * VerifyRowRelFast
 *
 * @Description: do verify one row_relation on FAST mode. the page header and crc checksum will be checked
 *               on physical damage.
 * @in rel - the relation
 * @in&out checkCudesc - checkCudesc is a struct to judge whether cudesc tables is damaged.
 * @return: bool
 */
static bool VerifyRowRelFast(Relation rel, VerifyDesc* checkCudesc)
{
    if (unlikely(rel == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid rel value.")));
    }

    if (RELATION_IS_GLOBAL_TEMP(rel) && !gtt_storage_attached(RelationGetRelid(rel))) {
        return true;
    }

    char* buf = (char*)palloc(BLCKSZ);
    BlockNumber nblocks;
    BlockNumber blkno;
    ForkNumber forkNum = MAIN_FORKNUM;
    bool isValidRelationPage = true;

    char* namespace_name = get_namespace_name(RelationGetNamespace(rel));

    RelationOpenSmgr(rel);
    SMgrRelation src = rel->rd_smgr;
    nblocks = smgrnblocks(src, forkNum);

    /* copy main fork */
    for (blkno = 0; blkno < nblocks; blkno++) {
        /* If we got a cancel signal during the copy of the data, quit */
        CHECK_FOR_INTERRUPTS();
        SMGR_READ_STATUS rdStatus = smgrread(src, forkNum, blkno, buf);
        /* For DMS , try to read from buffer in case the data is not flused to disk */
        if (rdStatus == SMGR_RD_CRC_ERROR && ENABLE_DMS) {
            Buffer buffer = ReadBufferWithoutRelcache(src->smgr_rnode.node, forkNum, blkno, RBM_NORMAL, NULL, NULL);
            if (buffer != InvalidBuffer) {
                ReleaseBuffer(buffer);
                continue;
            }
        }

        /* check the page & crc */
        if (rdStatus == SMGR_RD_CRC_ERROR) {
            // Retry 5 times to increase program reliability.
            for (int retryTimes = 1; retryTimes < FAIL_RETRY_MAX_NUM && rdStatus == SMGR_RD_CRC_ERROR; ++retryTimes) {
                /* If we got a cancel signal during the copy of the data, quit */
                CHECK_FOR_INTERRUPTS();
                rdStatus = smgrread(src, forkNum, blkno, buf);
            }
            if (rdStatus != SMGR_RD_CRC_ERROR) {
                /* Ustrore white-box verification adapt to analyze verify. */
                if (rdStatus == SMGR_RD_OK) {
                    UPageVerifyParams verifyParam;
                    Page page = (char *) buf;
                    if (unlikely(ConstructUstoreVerifyParam(USTORE_VERIFY_MOD_UPAGE, USTORE_VERIFY_FAST,
                        (char *) &verifyParam, rel, page, InvalidBlockNumber, NULL, NULL, InvalidXLogRecPtr, NULL,
                        NULL, true))) {
                        ExecuteUstoreVerify(USTORE_VERIFY_MOD_UPAGE, (char *) &verifyParam);
                    }
                }
                continue;
            }

            isValidRelationPage = false;
            /*
             * check the cudesc table|cudesc-toast| cudesc_index. If one of them is damaged, we will have to
             * give up checking the main col_relation because it will generate unknown exceptions.
             */
            if (checkCudesc != NULL && checkCudesc->isCudesc) {
                checkCudesc->isCudescDamaged = true;
            }

            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Page verification failed. "
                           "The node is %s, invalid page in block %u of relation %s.%s, the file is %s.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        blkno,
                        namespace_name,
                        RelationGetRelationName(rel),
                        relpathbackend(src->smgr_rnode.node, src->smgr_rnode.backend, forkNum)),
                        handle_in_client(true)));
            /* Add the wye page to the global variable and try to fix it. */
            addGlobalRepairBadBlockStat(src->smgr_rnode, forkNum, blkno);
        } else if (rdStatus == SMGR_RD_OK) {
            /* Ustrore white-box verification adapt to analyze verify. */
            UPageVerifyParams verifyParam;
            Page page = (char *) buf;
            if (unlikely(ConstructUstoreVerifyParam(USTORE_VERIFY_MOD_UPAGE, USTORE_VERIFY_FAST,
                (char *) &verifyParam, rel, page, InvalidBlockNumber, NULL, NULL, InvalidXLogRecPtr, NULL,
                NULL, true))) {
                ExecuteUstoreVerify(USTORE_VERIFY_MOD_UPAGE, (char *) &verifyParam);
            }
        }
    }

    pfree_ext(buf);
    return isValidRelationPage;
}

/*
 * VerifyRowRelComplete
 *
 * @Description: do verify one row_relation on COMPLETE mode. the page header and crc checksum will be checked first.
 *               the tuple will be checked one by one.
 * @in rel - the relation
 * @in&out checkCudesc - checkCudesc is a struct to judge whether cudesc tables is damaged.
 * @return: bool
 */
static bool VerifyRowRelComplete(Relation rel, VerifyDesc* checkCudesc)
{
    if (RELATION_IS_GLOBAL_TEMP(rel) && !gtt_storage_attached(RelationGetRelid(rel))) {
        return true;
    }

    TableScanDesc scandesc;
    Tuple tuple;
    TupleDesc tupleDesc;
    Datum* values = NULL;
    bool* nulls = NULL;
    int numberOfAttributes = 0;
    ForkNumber forkNum = MAIN_FORKNUM;
    bool isValidRelationPageFast = true;
    bool isValidRelationPageComplete = true;
    SMgrRelation smgrRel = NULL;
    BlockNumber nblocks = 0;
    char *buf = NULL;

    /* create column table verify memory context */
    MemoryContext verifyRowMemContext = AllocSetContextCreate(CurrentMemoryContext,
        "row table verify memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /* switch to column table verify memory context */
    MemoryContext oldMemContext = MemoryContextSwitchTo(verifyRowMemContext);

    /* check page header and crc first */
    isValidRelationPageFast = VerifyRowRelFast(rel, checkCudesc);

    /* check all tuples of ustore relation. */
    buf = (char*)palloc(BLCKSZ);
    RelationOpenSmgr(rel);
    smgrRel = rel->rd_smgr;
    nblocks = smgrnblocks(smgrRel, forkNum);
    for (BlockNumber blkno = 0; blkno < nblocks; blkno++) {
        CHECK_FOR_INTERRUPTS();
        SMGR_READ_STATUS rdStatus = smgrread(smgrRel, forkNum, blkno, buf);
        if (rdStatus == SMGR_RD_OK) {
            UPageVerifyParams verifyParam;
            Page page = (char *) buf;
            if (unlikely(ConstructUstoreVerifyParam(USTORE_VERIFY_MOD_UPAGE, USTORE_VERIFY_COMPLETE,
                (char *) &verifyParam, rel, page, InvalidBlockNumber, NULL, NULL, InvalidXLogRecPtr, NULL, NULL,
                true))) {
                ExecuteUstoreVerify(USTORE_VERIFY_MOD_UPAGE, (char *) &verifyParam);
            }
        }
    }
    pfree_ext(buf);

    if (rel->rd_rel->relkind == RELKIND_RELATION || rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
        /* check the tuple */
        tupleDesc = RelationGetDescr(rel);
        numberOfAttributes = tupleDesc->natts;
        values = (Datum*)palloc(numberOfAttributes * sizeof(Datum));
        nulls = (bool*)palloc(numberOfAttributes * sizeof(bool));

        scandesc = tableam_scan_begin(rel, GetActiveSnapshot(), 0, NULL);

    NEXT_TUPLE:
        CHECK_FOR_INTERRUPTS();
        tuple = tableam_scan_gettuple_for_verify(scandesc, ForwardScanDirection, isValidRelationPageComplete);
        if (tuple == NULL) {
            tableam_scan_end(scandesc);
            pfree_ext(values);
            pfree_ext(nulls);

            (void)MemoryContextSwitchTo(oldMemContext);
            MemoryContextDelete(verifyRowMemContext);
            return (isValidRelationPageFast && isValidRelationPageComplete);
        } else if (tuple != NULL && ((HeapTuple)tuple)->t_data != NULL) {
            PG_TRY();
            {
                /*
                 * deform the passed heap tuple. call heap_deform_tuple() if it's not compressed,
                 * otherwise call heap_deform_cmprs_tuple().
                 */
                if (!HEAP_TUPLE_IS_COMPRESSED(((HeapTuple)tuple)->t_data)) {
                    tableam_tops_deform_tuple(tuple, tupleDesc, values, nulls);
                } else {
                    Page page = BufferGetPage(scandesc->rs_cbuf);
                    Assert(page != NULL);
                    Assert(PageIsCompressed(page));
                    tableam_tops_deform_cmprs_tuple(tuple, tupleDesc, values, nulls, (char*)getPageDict(page));
                }
            }
            PG_CATCH();
            {
                isValidRelationPageComplete = false;
                /* Set the verify memory context beacuse catching ERROR will change the context to ErrorContext. */
                (void)MemoryContextSwitchTo(verifyRowMemContext);
                /*
                 * check the cudesc table|cudesc-toast| cudesc_index. If one of them is damaged, we will have to
                 * give up checking the main col_relation because it will generate unknown exceptions.
                 */
                if (checkCudesc != NULL && checkCudesc->isCudesc) {
                    checkCudesc->isCudescDamaged = true;
                }

                FlushErrorState();
                ereport(WARNING,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("Tuple verification failed on complete mode."
                               "The node is %s, invalid page of relation %s.%s, the file is %s.",
                            g_instance.attr.attr_common.PGXCNodeName,
                            get_namespace_name(RelationGetNamespace(rel)),
                            RelationGetRelationName(rel),
                            relpathperm(rel->rd_node, forkNum)),
                        handle_in_client(true)));
            }
            PG_END_TRY();
            goto NEXT_TUPLE;
        } else {
            isValidRelationPageComplete = false;
            /*
             * check the cudesc table|cudesc-toast| cudesc_index. If one of them is damaged, we will have to
             * give up checking the main col_relation because it will generate unknown exceptions.
             */
            if (checkCudesc != NULL && checkCudesc->isCudesc) {
                checkCudesc->isCudescDamaged = true;
            }

            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Page verification failed on complete mode."
                           "The node is %s, invalid page of relation %s.%s, the file is %s.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        get_namespace_name(RelationGetNamespace(rel)),
                        RelationGetRelationName(rel),
                        relpathperm(rel->rd_node, forkNum)),
                    handle_in_client(true)));
            goto NEXT_TUPLE;
        }
    }

    (void)MemoryContextSwitchTo(oldMemContext);
    MemoryContextDelete(verifyRowMemContext);

    return (isValidRelationPageFast && isValidRelationPageComplete);
}

static bool VerifyRowRelFull(VacuumStmt* stmt, Relation rel, VerifyDesc* checkCudesc)
{
    bool (*verifyfunc)(Relation, VerifyDesc*);
    Relation bucketRel = NULL;

    if ((unsigned int)stmt->options & VACOPT_FAST) {
        verifyfunc = &VerifyRowRelFast;
    } else if ((unsigned int)stmt->options & VACOPT_COMPLETE) {
        verifyfunc = &VerifyRowRelComplete;
    } else {
        /* default. */
        return true;
    }

    if (RELATION_CREATE_BUCKET(rel)) {
        oidvector* bucketlist = searchHashBucketByOid(rel->rd_bucketoid);
        Assert(checkCudesc == NULL);
        for (int i = 0; i < bucketlist->dim1; i++) {
            bucketRel = bucketGetRelation(rel, NULL, bucketlist->values[i]);
            if (verifyfunc(bucketRel, NULL) == false) {
                bucketCloseRelation(bucketRel);
                return false;
            }
            bucketCloseRelation(bucketRel);
        }
        return true;
    } else
        return verifyfunc(rel, checkCudesc);
}


static void VerifyCuDescRel(VacuumStmt* stmt, VerifyDesc* checkCudesc, Oid cudescRelid)
{
    /* 1.1 check the cudesc table. */
    Relation cudescRel = heap_open(cudescRelid, AccessShareLock);
    checkCudesc->isCudesc = true;
    VerifyRowRel(stmt, cudescRel, checkCudesc);

    /* 1.2 check cudesc relation's toast and toast's index. */
    Oid cudescToastRelid = cudescRel->rd_rel->reltoastrelid;
    if (OidIsValid(cudescToastRelid)) {
        Relation cudescToastRel = relation_open(cudescToastRelid, AccessShareLock);
        VerifyRowRel(stmt, cudescToastRel, checkCudesc);

        Oid cudescToastIdxRelid = cudescToastRel->rd_rel->reltoastidxid;
        Relation cudescToastIdxRel = index_open(cudescToastIdxRelid, AccessShareLock);
        VerifyIndexRels(stmt, cudescToastIdxRel, checkCudesc);
        index_close(cudescToastIdxRel, AccessShareLock);

        relation_close(cudescToastRel, AccessShareLock);
    }
    /* 1.3 check the cudesc relation's index table. */
    VerifyIndexRels(stmt, cudescRel, checkCudesc);
    relation_close(cudescRel, AccessShareLock);
}

static void reportColVerifyFailed(Relation rel, bool isdesc, bool iscomplete, BlockNumber cuId, int col)
{
    if (isdesc) {
        ereport(WARNING,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("The main table are not detected because cudesc verification failed. The node is %s,"
                       "the main relation is %s.%s. Please check its cudesc|cudesc_toast|cudesc_index table.",
                    g_instance.attr.attr_common.PGXCNodeName,
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel)),
                handle_in_client(true)));
        return;
    }

    if (iscomplete) {
        ereport(WARNING,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("CU verification failed on complete mode. The node is %s, invalid CU in cu_id %u"
                       " of relation %s.%s, the column is %d, the file is %s.",
                    g_instance.attr.attr_common.PGXCNodeName,
                    cuId,
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel),
                    col,
                    relpathperm(rel->rd_node, MAIN_FORKNUM)),
                handle_in_client(true)));
    } else {
        ereport(WARNING,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("CU verification failed. The node is %s, invalid CU of relation %s.%s, the file is %s.",
                    g_instance.attr.attr_common.PGXCNodeName,
                    get_namespace_name(RelationGetNamespace(rel)),
                    RelationGetRelationName(rel),
                    relpathperm(rel->rd_node, MAIN_FORKNUM)),
                handle_in_client(true)));
    }
    return;
}

/*
 * VerifyColRels
 *
 * @Description: do verify column relations. Based on the main rel, we also need to check:1)cudesc relation;
 *               2)cudesc toast relation;3)cudesc index relation;4)the main relation; 5)the delta relation;
 *               6)delta toast relation;7)delta index relation; If we do in the cascade mode, the index
 *               relations of the main table will be checked too.
 * @in stmt - the statment for analyse verify command
 * @in parentRel - the parent relation. For partition table, it means the main table. For ordinary table,
                   it means NULL.
 * @in rel - the relation
 * @return: void
 */
static void VerifyColRels(VacuumStmt* stmt, Relation parentRel, Relation rel)
{
    VerifyDesc* checkCudesc = (VerifyDesc*)palloc(sizeof(VerifyDesc));
    checkCudesc->isCudesc = false;
    checkCudesc->isCudescDamaged = false;

    /* 1 check the cudesc. */
    VerifyCuDescRel(stmt, checkCudesc, rel->rd_rel->relcudescrelid);

    if (checkCudesc->isCudesc && checkCudesc->isCudescDamaged) {
        reportColVerifyFailed(rel, true);
    } else {
        /* 2.1 check the main table. */
        VerifyColRel(stmt, rel);

        /* 3.1 check the delta table */
        Oid deltaRelid = rel->rd_rel->reldeltarelid;
        if (OidIsValid(deltaRelid)) {
            Relation deltaRel = heap_open(deltaRelid, AccessShareLock);
            VerifyRowRel(stmt, deltaRel);

            /* 3.2 check delta relation's toast and toast's index. */
            Oid deltaToastRelid = deltaRel->rd_rel->reltoastrelid;
            if (OidIsValid(deltaToastRelid)) {
                Relation deltaToastRel = relation_open(deltaToastRelid, AccessShareLock);
                VerifyRowRel(stmt, deltaToastRel);

                Oid deltaToastIdxRelid = deltaToastRel->rd_rel->reltoastidxid;
                Relation deltaToastIdxRel = index_open(deltaToastIdxRelid, AccessShareLock);
                VerifyIndexRels(stmt, deltaToastIdxRel);
                index_close(deltaToastIdxRel, AccessShareLock);

                relation_close(deltaToastRel, AccessShareLock);
            }

            relation_close(deltaRel, AccessShareLock);
        }
    }

    pfree_ext(checkCudesc);

    /* isCascade to judge checking the all tables or just checking the data file. */
    if (stmt->isCascade) {
        /* check rel's index list */
        if (parentRel == NULL) {
            VerifyIndexRels(stmt, rel);
        } else if (RELATION_IS_PARTITIONED(parentRel)) {
            VerifyPartIndexRels(stmt, parentRel, rel);
        }
    }
    return;
}

/*
 * VerifyColRel
 *
 * @Description: do verify one col_relation including two modes: FAST & COMPLETE. In the fast mode, the page header and
 *               crc check will be checked on physical damage. In the complete mode,the tuple will be checked one by
 * one.
 * @in stmt - the statment for analyse verify command
 * @in rel - the relation
 * @return: void
 */
static void VerifyColRel(VacuumStmt* stmt, Relation rel)
{
    /* turn off the remote read and keep the old mode */
    int oldRemoteReadMode = SetRemoteReadModeOffAndGetOldMode();

    if ((unsigned int)stmt->options & VACOPT_FAST) {
        VerifyColRelFast(rel);
    } else if ((unsigned int)stmt->options & VACOPT_COMPLETE) {
        VerifyColRelComplete(rel);
    }

    SetRemoteReadMode(oldRemoteReadMode);
    return;
}

/*
 * VerifyColRelFast
 *
 * @Description: do verify one col_relation on FAST mode. cu magic and crc checksum will be checked
 *               on physical damage.
 * @in rel - the relation
 * @return: void
 */
static void VerifyColRelFast(Relation rel)
{
    TupleDesc tupleDesc;
    CStoreScanDesc scan = NULL;
    int16* colIdx = NULL;
    FormData_pg_attribute* attrs = NULL;
    int attno = rel->rd_rel->relnatts;
    int col = 0;

    /* create column table verify memory context */
    MemoryContext verifyColMemContext = AllocSetContextCreate(CurrentMemoryContext,
        "column table verify memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* switch to column table verify memory context */
    MemoryContext oldMemContext = MemoryContextSwitchTo(verifyColMemContext);

    BatchCUData* tmpBatchCUData = New(CurrentMemoryContext) BatchCUData();

    RelationOpenSmgr(rel);

    tupleDesc = RelationGetDescr(rel);
    colIdx = (int16*)palloc0(sizeof(int16) * tupleDesc->natts);
    attrs = tupleDesc->attrs;
    for (int i = 0; i < tupleDesc->natts; i++) {
        colIdx[i] = attrs[i].attnum;
    }

    scan = CStoreBeginScan(rel, tupleDesc->natts, colIdx, GetActiveSnapshot(), true);

    tmpBatchCUData->Init(tupleDesc->natts);
    do {
        PG_TRY();
        {
            /*
             * CStoreScanWithCU is used to scan all the CU and check the crc and magic.
             * In CUStorage::LoadCU we palloc CU_ptrs, which is used in tmpBatchCUData,
             * we have to pfree CU_ptrs.
             */
            scan->m_CStore->CStoreScanWithCU(scan, tmpBatchCUData, true);
        }
        PG_CATCH();
        {
            /* Set the verify memory context beacuse catching ERROR will change the context to ErrorContext. */
            (void)MemoryContextSwitchTo(verifyColMemContext);
            /* If one error is catched in CStoreScanWithCU, 
             * we need to check the next cuid in order to prevent endless loop.
             */
            scan->m_CStore->IncLoadCuDescCursor();

            if (geterrcode() != ERRCODE_OUT_OF_LOGICAL_MEMORY) {
                reportColVerifyFailed(rel);
            } else {
                ereport(WARNING,
                        (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                            errmsg("memory is temporarily unavailable on relation %s.%s, please retry verify.",
                                get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel))));
            }
            FlushErrorState();
        }
        PG_END_TRY();
        if (!tmpBatchCUData->batchCUIsNULL()) {
            for (col = 0; col < attno; col++) {
                if (tmpBatchCUData->CUptrData[col]) {
                    DELETE_EX(tmpBatchCUData->CUptrData[col]);
                }
            }
        }
        tmpBatchCUData->reset();
    } while (!scan->m_CStore->IsEndScan());

    DELETE_EX(tmpBatchCUData);
    CStoreEndScan(scan);
    pfree(colIdx);

    (void)MemoryContextSwitchTo(oldMemContext);
    MemoryContextDelete(verifyColMemContext);

    return;
}

/*
 * VerifyColRelComplete
 *
 * @Description: do verify one col_relation on COMPLETE mode. cu magic and crc checksum will be checked
 *               on physical damage. And the CU data will be checked too.
 * @in rel - the relation
 * @return: void
 */
static void VerifyColRelComplete(Relation rel)
{
    VerifyColRelFast(rel);

    int attrNum = rel->rd_att->natts;
    FormData_pg_attribute* attrs = rel->rd_att->attrs;
    CUDesc cuDesc;
    CU* cuPtr = NULL;
    BlockNumber cuId = FirstCUID + 1;

    /* create column table verify memory context */
    MemoryContext verifyColMemContext = AllocSetContextCreate(CurrentMemoryContext,
        "column table verify memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* switch to column table verify memory context */
    MemoryContext oldMemContext = MemoryContextSwitchTo(verifyColMemContext);

    AttrNumber* colIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * attrNum);
    int slotId = CACHE_BLOCK_INVALID_IDX;

    for (int i = 0; i < attrNum; i++) {
        colIdx[i] = attrs[i].attnum;
    }

    CStoreScanDesc cstoreScanDesc = CStoreBeginScan(rel, attrNum, colIdx, GetActiveSnapshot(), false);
    CStore* cstore = cstoreScanDesc->m_CStore;
    uint32 maxCuId = cstore->GetMaxCUID(rel->rd_rel->relcudescrelid, rel->rd_att, GetActiveSnapshot());

    /* check the all CU and check the CU of each column. */
    for (cuId = FirstCUID + 1; cuId <= maxCuId; cuId++) {
        for (int col = 0; col < attrNum; col++) {
            /* skip dropped column */
            if (attrs[col].attisdropped) {
                continue;
            }

            bool found = cstore->GetCUDesc(col, cuId, &cuDesc, GetActiveSnapshot());
            if (found && cuDesc.cu_size != 0) {
                PG_TRY();
                {
                    cuPtr = cstore->GetCUData(&cuDesc, col, attrs[col].attlen, slotId);
                    if (IsValidCacheSlotID(slotId)) {
                        CUCache->UnPinDataBlock(slotId);
                    }
                }
                PG_CATCH();
                {
                    /* Set the verify memory context beacuse catching ERROR will change the context to ErrorContext. */
                    (void)MemoryContextSwitchTo(verifyColMemContext);
                    
                    if (geterrcode() != ERRCODE_OUT_OF_LOGICAL_MEMORY) {
                        reportColVerifyFailed(rel, false, true, cuId, col);
                    } else {
                        ereport(WARNING,
                                (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                                    errmsg("memory is temporarily unavailable on relation %s.%s, please retry verify.",
                                        get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel))));
                    }
                    FlushErrorState();
                    VerifyAbortCU();

                    if (IsValidCacheSlotID(slotId)) {
                        CUCache->UnPinDataBlock(slotId);
                    }
                }
                PG_END_TRY();
            }
        }
    }

    pfree_ext(colIdx);
    CStoreEndScan(cstoreScanDesc);

    (void)MemoryContextSwitchTo(oldMemContext);
    MemoryContextDelete(verifyColMemContext);

    return;
}

/*
 * VerifyAbortBufferIO: Clean up the active buffer I/O after an error.
 * at this branch, abnormal situation will be done with.
 * The related LWLocks we might have held have been released,
 * but we haven't yet released buffer pins, so the buffer is still pinned.
 */
void VerifyAbortBufferIO(void)
{
    BufferDesc* buf = (BufferDesc*)t_thrd.storage_cxt.InProgressBuf;
    bool isForInput = (bool)t_thrd.storage_cxt.IsForInput;

    if (buf != NULL) {
        /* match the upcoming RESUME_INTERRUPTS */
        HOLD_INTERRUPTS();

        /* LWLockRelease will deal with the abnormal situation and statistical count to avoid the assert error. */
        LWLockRelease(buf->io_in_progress_lock);
        UnpinBuffer(buf, true);

        /*
         * LWLockRelease was already been called before, now we are in an abnormal situation,
         * so we're not holding the buffer's io_in_progress_lock. We have to re-acquire it so that
         * we can use TerminateBufferIO. Anyone who's executing WaitIO on the buffer will be in a
         * busy spin until we succeed in doing this.
         */
        LWLockAcquire(buf->io_in_progress_lock, LW_EXCLUSIVE);
        AbortBufferIO_common(buf, isForInput);
        TerminateBufferIO(buf, false, BM_IO_ERROR);
    }
}
