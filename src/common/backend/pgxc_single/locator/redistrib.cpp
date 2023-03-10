/* -------------------------------------------------------------------------
 *
 * redistrib.c
 *	  Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/locator/redistrib.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"

#include "access/cstore_am.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/tablecmds.h"
#include "cstore.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "pgxc/remotecopy.h"
#include "storage/lock/lwlock.h"
#include "storage/off.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/elog.h"

#define IsCommandTypePreUpdate(x) ((x) == CATALOG_UPDATE_BEFORE || (x) == CATALOG_UPDATE_BOTH)
#define IsCommandTypePostUpdate(x) ((x) == CATALOG_UPDATE_AFTER || (x) == CATALOG_UPDATE_BOTH)

#define DatumGetItemPointer(X) ((ItemPointer)DatumGetPointer(X))
#define ItemPointerGetDatum(X) PointerGetDatum(X)
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_ITEMPOINTER(x) return ItemPointerGetDatum(x)
#define DST_MAX_SIZE 64

#define RedisRelCtidFormat "(%u.%u)"

/* Functions used for the execution of redistribution commands */
static void distrib_execute_query(const char* sql, bool is_temp, ExecNodes* exec_nodes);
static void distrib_execute_command(RedistribState* distribState, RedistribCommand* command);
static void distrib_copy_to(RedistribState* distribState);
static void distrib_copy_from(RedistribState* distribState, ExecNodes* exec_nodes);
static void distrib_truncate(RedistribState* distribState, ExecNodes* exec_nodes);
static void distrib_reindex(RedistribState* distribState, ExecNodes* exec_nodes);
static void distrib_delete_hash(RedistribState* distribState, ExecNodes* exec_nodes);

/* Functions used to build the command list */
#ifdef ENABLE_MULTIPLE_NODES
static void pgxc_redist_build_entry(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo);
static void pgxc_redist_build_replicate(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo);
static void pgxc_redist_build_replicate_to_distrib(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo);

static void pgxc_redist_build_default(RedistribState* distribState);
static void pgxc_redist_add_reindex(RedistribState* distribState);
#endif
/*
 * PGXCRedistribTable
 * Execute redistribution operations after catalog update
 */
void PGXCRedistribTable(RedistribState* distribState, RedistribCatalog type)
{
    ListCell* item = NULL;

    /* Nothing to do if no redistribution operation */
    if (distribState == NULL)
        return;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* Execute each command if necessary */
    foreach (item, distribState->commands) {
        RedistribCommand* command = (RedistribCommand*)lfirst(item);

        /* Check if command can be run */
        if (!IsCommandTypePostUpdate(type) && IsCommandTypePostUpdate(command->updateState))
            continue;
        if (!IsCommandTypePreUpdate(type) && IsCommandTypePreUpdate(command->updateState))
            continue;

        /* Now enter in execution list */
        distrib_execute_command(distribState, command);
    }
}

/*
 * PGXCRedistribCreateCommandList
 * Look for the list of necessary commands to perform table redistribution.
 */
void PGXCRedistribCreateCommandList(RedistribState* distribState, RelationLocInfo* newLocInfo)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    Relation rel;
    RelationLocInfo* oldLocInfo;

    rel = relation_open(distribState->relid, NoLock);
    oldLocInfo = RelationGetLocInfo(rel);

    /* Build redistribution command list */
    pgxc_redist_build_entry(distribState, oldLocInfo, newLocInfo);

    relation_close(rel, NoLock);
#endif
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * pgxc_redist_build_entry
 * Entry point for command list building
 */
static void pgxc_redist_build_entry(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo)
{
    /* If distribution has not changed at all, nothing to do */
    if (IsLocatorInfoEqual(oldLocInfo, newLocInfo))
        return;

    /* Evaluate cases for replicated tables */
    pgxc_redist_build_replicate(distribState, oldLocInfo, newLocInfo);

    /* Evaluate cases for replicated to distributed tables */
    pgxc_redist_build_replicate_to_distrib(distribState, oldLocInfo, newLocInfo);

    /* PGXC:perform more complex builds of command list */

    /* Fallback to default */
    pgxc_redist_build_default(distribState);
}

/*
 * pgxc_redist_build_replicate_to_distrib
 * Build redistribution command list from replicated to distributed
 * table.
 */
static void pgxc_redist_build_replicate_to_distrib(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo)
{
    List* removedNodes = NULL;
    List* newNodes = NULL;

    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* Redistribution is done from replication to distributed (with value) */
    if (!IsRelationReplicated(oldLocInfo) || !IsRelationDistributedByValue(newLocInfo))
        return;

    /* Get the list of nodes that are added to the relation */
    removedNodes = list_difference_int(oldLocInfo->nodeList, newLocInfo->nodeList);

    /* Get the list of nodes that are removed from relation */
    newNodes = list_difference_int(newLocInfo->nodeList, oldLocInfo->nodeList);
    /*
     * If some nodes are added, turn back to default, we need to fetch data
     * and then redistribute it properly.
     */
    if (newNodes != NIL)
        return;

    /* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
    if (removedNodes != NIL) {
        ExecNodes* execNodes = makeNode(ExecNodes);
        execNodes->nodeList = removedNodes;
        /* Add TRUNCATE command */
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
    }

    /*
     * If the table is redistributed to a single node, a TRUNCATE on removed nodes
     * is sufficient so leave here.
     */
    if (list_length(newLocInfo->nodeList) == 1) {
        /* Add REINDEX command if necessary */
        pgxc_redist_add_reindex(distribState);
        return;
    }

    /*
     * If we are here we are sure that redistribution only requires to delete data on remote
     * nodes on the new subset of nodes. So launch to remote nodes a DELETE command that only
     * eliminates the data not verifying the new hashing condition.
     */
    if (newLocInfo->locatorType == LOCATOR_TYPE_HASH) {
        ExecNodes* execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newLocInfo->nodeList;
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_DELETE_HASH, CATALOG_UPDATE_AFTER, execNodes));
    } else if (newLocInfo->locatorType == LOCATOR_TYPE_MODULO) {
        ExecNodes* execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newLocInfo->nodeList;
        distribState->commands = lappend(
            distribState->commands, makeRedistribCommand(DISTRIB_DELETE_MODULO, CATALOG_UPDATE_AFTER, execNodes));
    } else
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Incorrect redistribution operation")));

    /* Add REINDEX command if necessary */
    pgxc_redist_add_reindex(distribState);
}

/*
 * pgxc_redist_build_replicate
 * Build redistribution command list for replicated tables
 */
static void pgxc_redist_build_replicate(
    RedistribState* distribState, RelationLocInfo* oldLocInfo, RelationLocInfo* newLocInfo)
{
    List* removedNodes = NULL;
    List* newNodes = NULL;

    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* Case of a replicated table whose set of nodes is changed */
    if (!IsRelationReplicated(newLocInfo) || !IsRelationReplicated(oldLocInfo))
        return;

    /* Get the list of nodes that are added to the relation */
    removedNodes = list_difference_int(oldLocInfo->nodeList, newLocInfo->nodeList);

    /* Get the list of nodes that are removed from relation */
    newNodes = list_difference_int(newLocInfo->nodeList, oldLocInfo->nodeList);
    /*
     * If nodes have to be added, we need to fetch data for redistribution first.
     * So add a COPY TO command to fetch data.
     */
    if (newNodes != NIL) {
        /* Add COPY TO command */
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
    }

    /* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
    if (removedNodes != NIL) {
        ExecNodes* execNodes = makeNode(ExecNodes);
        execNodes->nodeList = removedNodes;
        /* Add TRUNCATE command */
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
    }

    /* If necessary, COPY the data obtained at first step to the new nodes. */
    if (newNodes != NIL) {
        ExecNodes* execNodes = makeNode(ExecNodes);
        execNodes->nodeList = newNodes;
        /* Add COPY FROM command */
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, execNodes));
    }

    /* Add REINDEX command if necessary */
    pgxc_redist_add_reindex(distribState);
}

/*
 * pgxc_redist_build_default
 * Build a default list consisting of
 * COPY TO -> TRUNCATE -> COPY FROM ( -> REINDEX )
 */
static void pgxc_redist_build_default(RedistribState* distribState)
{
    /* If a command list has already been built, nothing to do */
    if (list_length(distribState->commands) != 0)
        return;

    /* COPY TO command */
    distribState->commands =
        lappend(distribState->commands, makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
    /* TRUNCATE command */
    distribState->commands =
        lappend(distribState->commands, makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, NULL));
    /* COPY FROM command */
    distribState->commands =
        lappend(distribState->commands, makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, NULL));

    /* REINDEX command */
    pgxc_redist_add_reindex(distribState);
}

/*
 * pgxc_redist_build_reindex
 * Add a reindex command if necessary
 */
static void pgxc_redist_add_reindex(RedistribState* distribState)
{
    Relation rel;

    rel = relation_open(distribState->relid, NoLock);
    /* Build REINDEX command if necessary */
    if (RelationGetIndexList(rel) != NIL) {
        distribState->commands =
            lappend(distribState->commands, makeRedistribCommand(DISTRIB_REINDEX, CATALOG_UPDATE_AFTER, NULL));
    }

    relation_close(rel, NoLock);
}
#endif

/*
 * distrib_execute_command
 * Execute a redistribution operation
 */
static void distrib_execute_command(RedistribState* distribState, RedistribCommand* command)
{
    /* Execute redistribution command */
    switch (command->type) {
        case DISTRIB_COPY_TO:
            distrib_copy_to(distribState);
            break;
        case DISTRIB_COPY_FROM:
            distrib_copy_from(distribState, command->execNodes);
            break;
        case DISTRIB_TRUNCATE:
            distrib_truncate(distribState, command->execNodes);
            break;
        case DISTRIB_REINDEX:
            distrib_reindex(distribState, command->execNodes);
            break;
        case DISTRIB_DELETE_HASH:
        case DISTRIB_DELETE_MODULO:
            distrib_delete_hash(distribState, command->execNodes);
            break;
        case DISTRIB_NONE:
        default:
            Assert(0); /* Should not happen */
            break;
    }
}

/*
 * distrib_copy_to
 * Copy all the data of table to be distributed.
 * This data is saved in a tuplestore saved in distribution state.
 * a COPY FROM operation is always done on nodes determined by the locator data
 * in catalogs, explaining why this cannot be done on a subset of nodes. It also
 * insures that no read operations are done on nodes where data is not yet located.
 */
static void distrib_copy_to(RedistribState* distribState)
{
    Oid relOid = distribState->relid;
    Relation rel;
    RemoteCopyOptions* options = NULL;
    RemoteCopyData* copyState = NULL;
    Tuplestorestate* store = NULL; /* Storage of redistributed data */

    /* Fetch necessary data to prepare for the table data acquisition */
    options = makeRemoteCopyOptions();

    /* All the fields are separated by tabs in redistribution */
    options->rco_delim = (char*)palloc(2);
    options->rco_delim[0] = COPYOPS_DELIMITER;
    options->rco_delim[1] = '\0';

    copyState = (RemoteCopyData*)palloc0(sizeof(RemoteCopyData));
    copyState->is_from = false;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);
    RemoteCopy_GetRelationLoc(copyState, rel, NIL);
    RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL);

    /* Inform client of operation being done */
    ereport(DEBUG1,
        (errmsg("Copying data for relation \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel))));

    /* Begin the COPY process */
    copyState->connections =
        DataNodeCopyBegin(copyState->query_buf.data, copyState->exec_nodes->nodeList, GetActiveSnapshot());

    /* Create tuplestore storage */
    store = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);

    /* Then get rows and copy them to the tuplestore used for redistribution */
    DataNodeCopyOut(copyState->exec_nodes,
        copyState->connections,
        RelationGetDescr(rel), /* Need also to set up the tuple descriptor */
        NULL,
        store, /* Tuplestore used for redistribution */
        REMOTE_COPY_TUPLESTORE);

    /* Do necessary clean-up */
    FreeRemoteCopyOptions(options);

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);

    /* Save results */
    distribState->store = store;
}

/*
 * PGXCDistribTableCopyFrom
 * Execute commands related to COPY FROM
 * Redistribute all the data of table with a COPY FROM from given tuplestore.
 */
static void distrib_copy_from(RedistribState* distribState, ExecNodes* exec_nodes)
{
    Oid relOid = distribState->relid;
    Tuplestorestate* store = distribState->store;
    Relation rel;
    RemoteCopyOptions* options = NULL;
    RemoteCopyData* copyState = NULL;
    bool replicated = false, contains_tuple = true;
    TupleDesc tupdesc;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* Fetch necessary data to prepare for the table data acquisition */
    options = makeRemoteCopyOptions();
    /* All the fields are separated by tabs in redistribution */
    options->rco_delim = (char*)palloc(2);
    options->rco_delim[0] = COPYOPS_DELIMITER;
    options->rco_delim[1] = '\0';

    copyState = (RemoteCopyData*)palloc0(sizeof(RemoteCopyData));
    copyState->is_from = true;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);
    RemoteCopy_GetRelationLoc(copyState, rel, NIL);
    RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL);

    /*
     * When building COPY FROM command in redistribution list,
     * use the list of nodes that has been calculated there.
     * It might be possible that this COPY is done only on a portion of nodes.
     */
    if (exec_nodes != NULL && exec_nodes->nodeList != NIL) {
        copyState->exec_nodes->nodeList = exec_nodes->nodeList;
        copyState->rel_loc->nodeList = exec_nodes->nodeList;
    }

    tupdesc = RelationGetDescr(rel);

    /* Inform client of operation being done */
    ereport(DEBUG1,
        (errmsg("Redistributing data for relation \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel))));

    /* Begin redistribution on remote nodes */
    copyState->connections =
        DataNodeCopyBegin(copyState->query_buf.data, copyState->exec_nodes->nodeList, GetActiveSnapshot());

    /* Transform each tuple stored into a COPY message and send it to remote nodes */
    while (contains_tuple) {
        char* data = NULL;
        int len;
        FormData_pg_attribute* attr = tupdesc->attrs;
        TupleTableSlot* slot = NULL;
        ExecNodes* local_execnodes = NULL;

        Oid* att_type = (Oid*)palloc(tupdesc->natts * sizeof(Oid));

        /* Build table slot for this relation */
        slot = MakeSingleTupleTableSlot(tupdesc);

        /* Get tuple slot from the tuplestore */
        contains_tuple = tuplestore_gettupleslot(store, true, false, slot);
        if (!contains_tuple) {
            ExecDropSingleTupleTableSlot(slot);
            break;
        }

        /* Make sure the tuple is fully deconstructed */
        tableam_tslot_getallattrs(slot);

        /* Find value of distribution column if necessary */
        for (int i = 0; i < tupdesc->natts; i++) {
            att_type[i] = attr[i].atttypid;
        }

        local_execnodes = GetRelationNodes(copyState->rel_loc,
            slot->tts_values,
            slot->tts_isnull,
            att_type,
            copyState->idx_dist_by_col,
            RELATION_ACCESS_INSERT,
            false);

        pfree(att_type);

        /* Build message to be sent to Datanodes */
        data = CopyOps_BuildOneRowTo(tupdesc, slot->tts_values, slot->tts_isnull, &len);

        /* Take a copy of the node lists so as not to interfere with locator info */
        local_execnodes->primarynodelist = list_copy(local_execnodes->primarynodelist);
        local_execnodes->nodeList = list_copy(local_execnodes->nodeList);

        /* Process data to Datanodes */
        DataNodeCopyIn(data, len, NULL, local_execnodes, copyState->connections);

        /* Clean up */
        pfree(data);
        FreeExecNodes(&local_execnodes);
        (void)ExecClearTuple(slot);
        ExecDropSingleTupleTableSlot(slot);
    }

    /* Finish the redistribution process */
    replicated = copyState->rel_loc->locatorType == LOCATOR_TYPE_REPLICATED;
    DataNodeCopyFinish(copyState->connections,
        u_sess->pgxc_cxt.NumDataNodes,
        replicated ? PGXCNodeGetNodeId(u_sess->pgxc_cxt.primary_data_node, PGXC_NODE_DATANODE) : -1,
        replicated ? COMBINE_TYPE_SAME : COMBINE_TYPE_SUM,
        rel);

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);
}

/*
 * distrib_truncate
 * Truncate all the data of specified table.
 * This is used as a second step of online data redistribution.
 */
static void distrib_truncate(RedistribState* distribState, ExecNodes* exec_nodes)
{
    Relation rel;
    StringInfo buf;
    Oid relOid = distribState->relid;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
        (errmsg("Truncating data for relation \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Build query to clean up table before redistribution */
    appendStringInfo(
        buf, "TRUNCATE %s.%s", get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel));

    /*
     * Lock is maintained until transaction commits,
     * relation needs also to be closed before effectively launching the query.
     */
    relation_close(rel, NoLock);

    /* Execute the query */
    distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);
}

/*
 * distrib_reindex
 * Reindex the table that has been redistributed
 */
static void distrib_reindex(RedistribState* distribState, ExecNodes* exec_nodes)
{
    Relation rel;
    StringInfo buf;
    Oid relOid = distribState->relid;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
        (errmsg("Reindexing relation \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Generate the query */
    appendStringInfo(
        buf, "REINDEX TABLE %s.%s", get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel));

    /* Execute the query */
    distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);

    /* Lock is maintained until transaction commits */
    relation_close(rel, NoLock);
}

/*
 * distrib_delete_hash
 * Perform a partial tuple deletion of remote tuples not checking the correct hash
 * condition. The new distribution condition is set up in exec_nodes when building
 * the command list.
 */
static void distrib_delete_hash(RedistribState* distribState, ExecNodes* exec_nodes)
{
    Relation rel;
    StringInfo buf;
    Oid relOid = distribState->relid;
    ListCell* item = NULL;

    /* Nothing to do if on remote node */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    /* A sufficient lock level needs to be taken at a higher level */
    rel = relation_open(relOid, NoLock);

    /* Inform client of operation being done */
    ereport(DEBUG1,
        (errmsg("Deleting necessary tuples \"%s.%s\"",
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetRelationName(rel))));

    /* Initialize buffer */
    buf = makeStringInfo();

    /* Build query to clean up table before redistribution */
    appendStringInfo(
        buf, "DELETE FROM %s.%s", get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel));

    /*
     * Launch the DELETE query to each node as the DELETE depends on
     * local conditions for each node.
     */
    foreach (item, exec_nodes->nodeList) {
        StringInfo buf2;
        char* colname = NULL;
        RelationLocInfo* locinfo = RelationGetLocInfo(rel);
        int nodenum = lfirst_int(item);
        int nodepos = 0;
        ExecNodes* local_exec_nodes = makeNode(ExecNodes);
        ListCell* item2 = NULL;

        /* Here the query is launched to a unique node */
        local_exec_nodes->nodeList = lappend_int(NIL, nodenum);

        /* Get the hash type of relation */

        /* Get function hash name */

        /* Get distribution column name */
        if (IsRelationDistributedByValue(locinfo)) {
            colname = NULL;
        } else
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Incorrect redistribution operation")));

        /*
         * Find the correct node position in node list of locator information.
         * So scan the node list and fetch the position of node.
         */
        foreach (item2, locinfo->nodeList) {
            int loc = lfirst_int(item2);
            if (loc == nodenum)
                break;
            nodepos++;
        }

        /*
         * Then build the WHERE clause for deletion.
         * The condition that allows to keep the tuples on remote nodes
         * is of the type "RemoteNodeNumber != abs(hash_func(dis_col)) % NumDatanodes".
         * the remote Datanode has no knowledge of its position in cluster so this
         * number needs to be compiled locally on Coordinator.
         * Taking the absolute value is necessary as hash may return a negative value.
         * For hash distributions a condition with correct hash function is used.
         * For modulo distribution, well we might need a hash function call but not
         * all the time, this is determined implicitely by get_compute_hash_function.
         */
        buf2 = makeStringInfo();
        {
            /* Lets leave NULLs on the first node and delete from the rest */
            if (nodepos != 0)
                appendStringInfo(buf2,
                    "%s WHERE %s IS NULL OR abs(%s) %% %d != %d",
                    buf->data,
                    colname,
                    colname,
                    list_length(locinfo->nodeList),
                    nodepos);
            else
                appendStringInfo(
                    buf2, "%s WHERE abs(%s) %% %d != %d", buf->data, colname, list_length(locinfo->nodeList), nodepos);
        }

        /* Then launch this single query */
        distrib_execute_query(buf2->data, IsTempTable(relOid), local_exec_nodes);

        FreeExecNodes(&local_exec_nodes);
        pfree(buf2->data);
        pfree(buf2);
    }

    relation_close(rel, NoLock);

    /* Clean buffers */
    pfree(buf->data);
    pfree(buf);
}

/*
 * makeRedistribState
 * Build a distribution state operator
 */
RedistribState* makeRedistribState(Oid relOid)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else

    RedistribState* res = (RedistribState*)palloc(sizeof(RedistribState));
    res->relid = relOid;
    res->commands = NIL;
    res->store = NULL;
    return res;

#endif
}

/*
 * FreeRedistribState
 * Free given distribution state
 */
void FreeRedistribState(RedistribState* state)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    ListCell* item = NULL;

    /* Leave if nothing to do */
    if (!state)
        return;

    foreach (item, state->commands)
        FreeRedistribCommand((RedistribCommand*)lfirst(item));
    if (list_length(state->commands) > 0)
        list_free(state->commands);
    if (state->store)
        tuplestore_clear(state->store);
#endif
}

/*
 * makeRedistribCommand
 * Build a distribution command
 */
RedistribCommand* makeRedistribCommand(RedistribOperation type, RedistribCatalog updateState, ExecNodes* nodes)
{
    RedistribCommand* res = (RedistribCommand*)palloc0(sizeof(RedistribCommand));
    res->type = type;
    res->updateState = updateState;
    res->execNodes = nodes;
    return res;
}

/*
 * FreeRedistribCommand
 * Free given distribution command
 */
void FreeRedistribCommand(RedistribCommand* command)
{
    ExecNodes* nodes = NULL;
    /* Leave if nothing to do */
    if (command == NULL)
        return;
    nodes = command->execNodes;

    if (nodes != NULL)
        FreeExecNodes(&nodes);
    pfree(command);
}

/*
 * distrib_execute_query
 * Execute single raw query on given list of nodes
 */
static void distrib_execute_query(const char* sql, bool is_temp, ExecNodes* exec_nodes)
{
    RemoteQuery* step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->exec_nodes = exec_nodes;
    step->sql_statement = pstrdup(sql);
    step->force_autocommit = false;

    /* Redistribution operations only concern Datanodes */
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = is_temp;
    ExecRemoteUtility(step);
    pfree(step->sql_statement);
    pfree(step);

    /* Be sure to advance the command counter after the last command */
    CommandCounterIncrement();
}

/*
 * - Brief: Get relation's ctid in Reloptions
 * - Parameter:
 *      @rel_name: relation's name
 *      @partition_name: partition's name
 *      @ctid_type: start_ctid or end_ctid
 *      @result : the start or end ctid
 */
void get_redis_rel_ctid(const char* rel_name, const char* partition_name, RedisCtidType ctid_type, ItemPointer result)
{
    Relation rel = NULL, fake_rel = NULL;
    Partition partition = NULL;
    Oid relid = InvalidOid, part_oid = InvalidOid;
    ItemPointerData start_ctid, end_ctid;
    List* names = NIL;

    names = stringToQualifiedNameList(rel_name);

    /* if rel_name doesn't exist, RangeVarGetRelid will emit ERROR. */
    relid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);
    if (names != NIL)
        list_free(names);

    rel = try_relation_open(relid, AccessShareLock);
    if (rel == NULL) {
        ereport(WARNING, (errmsg("try open relation %s failed", rel_name)));
        return;
    }

    if (partition_name != NULL) {
        /* For partitioned table, */
        part_oid = PartitionNameGetPartitionOid(
            relid, partition_name, PART_OBJ_TYPE_TABLE_PARTITION, NoLock, false, false, NULL, NULL, NoLock);
    }

    if (OidIsValid(part_oid)) {
        partition = partitionOpen(rel, part_oid, AccessShareLock);
        fake_rel = partitionGetRelation(rel, partition);
        RelationGetCtids(fake_rel, &start_ctid, &end_ctid);
        releaseDummyRelation(&fake_rel);
        partitionClose(rel, partition, AccessShareLock);
    } else {
        RelationGetCtids(rel, &start_ctid, &end_ctid);
    }
    relation_close(rel, AccessShareLock);

    /* Fetch relation's append mode status with given relid */
    if (ctid_type == REDIS_START_CTID)
        ItemPointerCopy(&start_ctid, result);
    else
        ItemPointerCopy(&end_ctid, result);
}

/*
 * - Brief: get max cuid. The caller should already have
 *          exclusive lock on relation.
 * - Parameter:
 *      @rel:  columnar store table
 * - Return:
 *      @tid:  returned max tid
 */
void col_get_max_tid(const Relation rel, ItemPointer tid)
{
    Oid cudescOid = rel->rd_rel->relcudescrelid;
    uint32 maxCUID = CStore::GetMaxCUID(cudescOid, rel->rd_att);

    Assert(tid != NULL);
    /*
     * Columnar store don't care about offsetnumber because we
     * always use append mode but to make it a valid
     * itempointer we set DefaultFullCUSize as the max offsetnumber.
     */
    ItemPointerSet(tid, maxCUID, DefaultFullCUSize);
}

/*
 * - Brief: Add start_ctid/end_ctid to DefElem List
 *			convert ctid item to string mode.
 * - Parameter:
 *      @def_list: input parameter, DefElem list to be added
 *		@item_name: start_ctid_internal/end_ctid_internal
 *		@ctid: ctid values
 * - Return:
 *      @void:
 */
List* add_ctid_string_to_reloptions(List* def_list, const char* item_name, ItemPointer ctid)
{
    char ctid_str[DST_MAX_SIZE];
    errno_t rc;
    int ret;

    /* 64 is not a friendly item. */
    rc = memset_s(ctid_str, DST_MAX_SIZE, 0, DST_MAX_SIZE);
    securec_check_c(rc, "\0", "\0");
    ret = snprintf_s(ctid_str,
        DST_MAX_SIZE,
        DST_MAX_SIZE - 1,
        RedisRelCtidFormat,
        RedisCtidGetBlockNumber(ctid),
        RedisCtidGetOffsetNumber(ctid));
    securec_check_ss_c(ret, "\0", "\0");
    def_list = lappend(def_list, makeDefElem(pstrdup(item_name), (Node*)makeString(pstrdup(ctid_str))));
    return def_list;
}

/*
 * - Brief: Remove redis reloptions from DefElem List*
 * - Parameter:
 *      @rel: pointer to reloptions list
 * - Return:
 *      @void:
 */
void RemoveRedisRelOptionsFromList(List** reloptions)
{
    ereport(DEBUG3, (errmsg("[%s()]: shouldn't run here", __FUNCTION__)));
    return;
} 

/*
 * - Brief: Modify relation's redistribute options
 * - Parameter:
 *      @rel: relation that needs to check
 *		@action: append/normal/refresh
 * - Return:
 *      @void:
 */
List* AlterTableSetRedistribute(Relation rel, RedisRelAction action, char *merge_list)
{
    List* rel_options = NIL;
    bool isCUFormat = false;
    ItemPointerData start_ctid, end_ctid;

    Assert(rel);
    merge_list = NULL;
    isCUFormat = RelationIsCUFormat(rel);

    switch (action) {
        case REDIS_REL_APPEND: {
            if (RELATION_IS_PARTITIONED(rel)) {
                rel_options =
                    lappend(rel_options, makeDefElem(pstrdup("append_mode_internal"), (Node*)makeInteger(action)));
                /* set start_ctid */
                if (isCUFormat)
                    ItemPointerSet(&start_ctid, FirstCUID + 1, 0);
                else
                    ItemPointerSet(&start_ctid, 0, 0);

                ItemPointerSet(&end_ctid, 0, 0);

                rel_options = add_ctid_string_to_reloptions(rel_options, "start_ctid_internal", &start_ctid);
                rel_options = add_ctid_string_to_reloptions(rel_options, "end_ctid_internal", &end_ctid);
                return rel_options;
            }

            /*
             * 1. set append_mode_internal
             * 			start_ctid = end_ctid + 1
             *			end_ctid = max_ctid
             */
            if (IS_PGXC_DATANODE) {
                if (isCUFormat)
                    col_get_max_tid(rel, &end_ctid);
                else
                    heap_get_max_tid(rel, &end_ctid);
            } else {
                ItemPointerSet(&end_ctid, 0, 0);
            }

            /* 2. update redis rel info
             *
             * We apply the lappend for append_mode_internal for both if/else
             * case here to prevent the scenario like
             * 1. Table is first to set append_mode_internal = read_only
             * 2. Table is then to set append_mode_internal = on
             * the secondary setup will not update append_mode_internal if the
             * append is not applied to else part. Although such scenario will
             * rarely happen. transformRelOptions later will only update the value
             * of append_mode_internal.
             */
            rel_options =
                lappend(rel_options, makeDefElem(pstrdup("append_mode_internal"), (Node*)makeInteger(action)));
            if (!RelationInRedistribute(rel)) {
                /* set start_ctid */
                if (isCUFormat)
                    ItemPointerSet(&start_ctid, FirstCUID + 1, 0);
                else
                    ItemPointerSet(&start_ctid, 0, 0);
            } else {
                if (!IS_PGXC_DATANODE) {
                    ItemPointerSet(&start_ctid, 0, 0);
                } else {
                    /*  relation is redistributing */
                    ItemPointerData cur_start_ctid, cur_end_ctid;

                    RelationGetCtids(rel, &cur_start_ctid, &cur_end_ctid);

                    /* set start_ctid */
                    if (isCUFormat) {
                        /*
                         * Since columnar store table always use append mode, we
                         * just end_ctid.cuid + 1 to get the start_ctid
                         */
                        ItemPointerSetBlockNumber(&start_ctid, BlockIdGetBlockNumber(&(cur_end_ctid.ip_blkid)) + 1);
                        ItemPointerSetOffsetNumber(&start_ctid, FirstOffsetNumber);
                    } else {
                        if (RedisCtidGetOffsetNumber(&cur_end_ctid) == MaxOffsetNumber) {
                            ItemPointerSetBlockNumber(&start_ctid, RedisCtidGetBlockNumber(&(cur_end_ctid)) + 1);
                            ItemPointerSetOffsetNumber(&start_ctid, FirstOffsetNumber);
                        } else {
                            ItemPointerSetBlockNumber(&start_ctid, RedisCtidGetBlockNumber(&(cur_end_ctid)));
                            ItemPointerSetOffsetNumber(&start_ctid, OffsetNumberNext(cur_end_ctid.ip_posid));
                        }
                    }
                }
            }

            rel_options = add_ctid_string_to_reloptions(rel_options, "start_ctid_internal", &start_ctid);
            rel_options = add_ctid_string_to_reloptions(rel_options, "end_ctid_internal", &end_ctid);
            break;
        }
        case REDIS_REL_READ_ONLY: {
            rel_options =
                lappend(rel_options, makeDefElem(pstrdup("append_mode_internal"), (Node*)makeInteger(action)));
            break;
        }
        case REDIS_REL_RESET_CTID: {
            /*
             * 1. start_ctid = (0,0)
             * 2. end_ctid = (0,0);
             */
            if (isCUFormat) {
                ItemPointerSet(&start_ctid, FirstCUID + 1, 0);
                ItemPointerSet(&end_ctid, 0, 0);
            } else {
                ItemPointerSet(&start_ctid, 0, 0);
                ItemPointerSet(&end_ctid, 0, 0);
            }
            rel_options = add_ctid_string_to_reloptions(rel_options, "start_ctid_internal", &start_ctid);
            rel_options = add_ctid_string_to_reloptions(rel_options, "end_ctid_internal", &end_ctid);
            break;
        }
        default:
            rel_options = NIL;
            break;
    }

    return rel_options;
}

/*
 * - Brief: Modify partition rel's redistribute options
 * - Parameter:
 *      @rel: relation that needs to check
 *		@defList: DefElem list
 *		@operation: Alter reloptions method
 *		@lockmode: lock method
 *		@action: append/normal/refresh
 * - Return:
 *      @void:
 */
void AlterTableSetPartRelOptions(
    Relation rel, List* defList, AlterTableType operation, LOCKMODE lockmode, char *merge_list, RedisRelAction action)
{
    merge_list = NULL;
    ereport(DEBUG3, (errmsg("[%s()]: shouldn't run here", __FUNCTION__)));
    return;
}

/*
 * @Description: Check and get redistribute info
 * @Param[IN] options: input user options
 * @Param[OUT] *action: 0: insert; 1: delete. *rel_cn_oid: oid from CN
 * @See also:
 */
#ifdef ENABLE_MULTIPLE_NODES
void CheckRedistributeOption(List* options, Oid* rel_cn_oid, RedisHtlAction* action, char *merge_list_str, Relation rel)
#else
void CheckRedistributeOption(List* options, Oid* rel_cn_oid, RedisHtlAction* action, Relation rel)
#endif
{
    ListCell* opt = NULL;
    bool append_set = false;
    bool cnoid_set = false;

    Assert(rel != NULL);

    foreach (opt, options) {
        DefElem* def = (DefElem*)lfirst(opt);

        if (pg_strcasecmp(def->defname, "append_mode") == 0) {
            char* mode_options = defGetString(def);
            if (pg_strcasecmp(mode_options, "on") == 0)
                *action = REDIS_REL_APPEND;
            else if (pg_strcasecmp(mode_options, "off") == 0)
                *action = REDIS_REL_NORMAL;
            else if (pg_strcasecmp(mode_options, "read_only") == 0)
                *action = REDIS_REL_READ_ONLY;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("you can only take append_mode = on, off, refresh, read_only, end_catchup")));
            append_set = true;
        } else if (pg_strcasecmp(def->defname, "rel_cn_oid") == 0) {
            *rel_cn_oid = atoi(defGetString(def));
            cnoid_set = true;
#ifdef ENABLE_MULTIPLE_NODES
        }else if (pg_strcasecmp(def->defname, "merge_list") == 0) {
            *merge_list_str = pstrdup(defGetString(def));
#endif
        }
    }
    if (*action == REDIS_REL_APPEND && append_set && !cnoid_set) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Alter-Table set append mode should have 'rel_cn_oid' set together")));
    } else if (!append_set && cnoid_set) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Alter-Table set rel_cn_oid should have 'append_mode' set together")));
    }

    return;
}

/*
 * - Brief: set PGPROC as redistribution
 * - Parameter:
 *      @void:
 * - Return:
 *      @true:  succeed
 *      @false: failed
 */
bool set_proc_redis(void)
{
    if (!u_sess->attr.attr_sql.enable_cluster_resize) {
        /*
         * fix me here with a better ERRCODE
         */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                (errmsg("Proc redistribution only can be set during data redistribution time"))));

        return false;
    }

    /*
     * set process as data redistribution
     */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    t_thrd.pgxact->vacuumFlags |= PROC_IS_REDIST;
    LWLockRelease(ProcArrayLock);
    u_sess->proc_cxt.Isredisworker = true;

    return true;
}

/*
 * - Brief: reset PGPROC as redistribution
 * - Parameter:
 *      @void:
 * - Return:
 *      @true:  succeed
 *      @false: failed
 */
bool reset_proc_redis(void)
{
    if (!u_sess->attr.attr_sql.enable_cluster_resize) {
        /*
         * fix me here with a better ERRCODE
         */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                (errmsg("Proc redistribution only can be reset during data redistribution time"))));
        return false;
    }

    /*
     * reset process as data redistribution
     */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->vacuumFlags &= ~PROC_IS_REDIST;
    LWLockRelease(ProcArrayLock);
    u_sess->proc_cxt.Isredisworker = false;

    return true;
}

/*
 * - Brief: Is t_thrd.proc a redistribution proc?
 * - Parameter:
 *      @void:
 * - Return:
 *      @true:  yes
 *      @false: no
 */
bool IsRedistributionWorkerProcess(void)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * - Brief: Get Redis Ctid values
 * - Parameter:
 *      @rel: redistibuting relation, RelationData
 *		@start_ctid : start_ctid_interlal of reloptions, output value
 *		@end_ctid : end_ctid_interlal of reloptions, output value
 * - Return:
 *		@void
 */
void RelationGetCtids(Relation rel, ItemPointer start_ctid, ItemPointer end_ctid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

uint32 RelationGetEndBlock(Relation rel)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

Node* eval_redis_func_direct(Relation rel, bool is_func_get_start_ctid, int num_of_slice, int slice_index)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
ItemPointer eval_redis_func_direct_slice(ItemPointer start_ctid, ItemPointer end_ctid, bool is_func_get_start_ctid, 
         int num_of_slices, int slice_index)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void reset_merge_list_on_pgxc_class(Relation rel) 
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}
