/*-------------------------------------------------------------------------
 *
 * ddltrigger.cpp
 *    Logical DDL triggers.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/replication/logical/ddltrigger.cpp
 *
 * NOTES
 *
 * Deparse the ddl command and log it.
 *
 * ---------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "commands/event_trigger.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "replication/ddlmessage.h"
#include "tcop/ddldeparse.h"
#include "utils/lsyscache.h"

/*
 * Check if the command can be published.
 *
 * XXX Executing a non-immutable expression during the table rewrite phase is
 * not allowed, as it may result in different data between publisher and
 * subscriber. While some may suggest converting the rewrite inserts to updates
 * and replicate them after the ddl command to maintain data consistency, but it
 * doesn't work if the replica identity column is altered in the command. This
 * is because the rewrite inserts do not contain the old values and therefore
 * cannot be converted to update.
 *
 * Apart from that, commands containing volatile functions are not allowed. Because
 * it's possible the functions contain DDL/DML in which case these operations
 * will be executed twice and cause duplicate data. In addition, we don't know
 * whether the tables being accessed by these DDL/DML are published or not. So
 * blindly allowing such functions can allow unintended clauses like the tables
 * accessed in those functions may not even exist on the subscriber.
 */
static void
check_command_publishable(ddl_deparse_context context)
{
    if (context.max_volatility == PROVOLATILE_VOLATILE)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot use volatile function in this command because it cannot be replicated in DDL replication")));
}

bool relation_support_ddl_replication(Oid relid)
{
    bool support = false;

    Relation rel = relation_open(relid, AccessShareLock);
    /* if relpersistence is 'p', not support */
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
        return false;
    }
    if (RelationIsRowFormat(rel) && RelationIsAstoreFormat(rel)) {
        support = true;
    } else if (RelationIsIndex(rel)) {
        if(IS_BTREE(rel) && !RelationAmIsBtree(rel)) {
            return false;
        }
        support = true;
    } else if(RelationIsSequnce(rel)) {
        support = true;
    }

    relation_close(rel, AccessShareLock);

    return support;
}

/*
 * Deparse the ddl command and log it prior to
 * execution. Currently only used for DROP TABLE command
 * so that catalog can be accessed before being deleted.
 * This is to check if the table is part of the publication
 * or not.
 */
Datum
publication_deparse_ddl_command_start(PG_FUNCTION_ARGS)
{
    EventTriggerData *trigdata;
    char *command = psprintf("Drop table command start");
    DropStmt *stmt;
    ListCell *cell1;

    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
        elog(ERROR, "not fired by event trigger manager");

    trigdata = (EventTriggerData *) fcinfo->context;
    stmt = (DropStmt *) trigdata->parsetree;

    /* Extract the relid from the parse tree */
    foreach(cell1, stmt->objects) {
        bool support = true;
        Node *object = (Node*)lfirst(cell1);
        ObjectAddress address;
        Relation relation = NULL;

        address = get_object_address(stmt->removeType,
                                     IsA(object, List) ? (List*)object : list_make1(object),
                                     NULL,
                                     &relation,
                                     AccessExclusiveLock,
                                     true);

        /* Object does not exist, nothing to do */
        if (!relation)
            continue;

        if (get_rel_relkind(address.objectId))
            support = relation_support_ddl_replication(address.objectId);

        /*
         * Do not generate wal log for commands whose target table is a
         * temporary or unlogged table.
         *
         * XXX We may generate wal logs for unlogged tables in the future so
         * that unlogged tables can also be created and altered on the
         * subscriber side. This makes it possible to directly replay the SET
         * LOGGED command and the incoming rewrite message without creating a
         * new table.
         */
        if (support)
            LogLogicalDDLMessage("deparse", address.objectId, DCT_TableDropStart,
                                 command, strlen(command) + 1);

        relation_close(relation, NoLock);
    }
    return PointerGetDatum(NULL);
}

/*
 * Deparse the ddl command and log it. This function
 * is called after the execution of the command but before the
 * transaction commits.
 */
Datum
publication_deparse_ddl_command_end(PG_FUNCTION_ARGS)
{
    ListCell *lc;
    slist_iter iter;
    DeparsedCommandType type;
    Oid relid = InvalidOid;
    ddl_deparse_context context;

    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
        elog(ERROR, "not fired by event trigger manager");

    foreach(lc, currentEventTriggerState->commandList) {
        bool support = true;
    
        CollectedCommand *cmd = (CollectedCommand*)lfirst(lc);
        char *json_string;

        if (cmd->type == SCT_Simple &&
            !OidIsValid(cmd->d.simple.address.objectId))
            continue;

        /* Only SCT_Simple for now */
        relid = cmd->d.simple.address.objectId;
        type = DCT_SimpleCmd;
        
        if (get_rel_relkind(relid)) {
            support = relation_support_ddl_replication(relid);
        }

        if (support) {
            /*
             * Deparse the DDL command and WAL log it to allow decoding of the
             * same.
             */
            context.include_owner = true;
            context.max_volatility = PROVOLATILE_IMMUTABLE;

            json_string = deparse_utility_command(cmd, &context);

            if (json_string != NULL) {
                check_command_publishable(context);
                LogLogicalDDLMessage("deparse", relid, type, json_string,
                                     strlen(json_string) + 1);
            }
        }
    }

    /* Drop commands are not part commandlist but handled here as part of SQLDropList */
    slist_foreach(iter, &(currentEventTriggerState->SQLDropList)) {
        SQLDropObject *obj;
        EventTriggerData *trigdata;
        char *command;
        DeparsedCommandType cmdtype;

        trigdata = (EventTriggerData *) fcinfo->context;

        obj = slist_container(SQLDropObject, next, iter.cur);

        if (obj->istemp || !obj->original) {
            continue;
        }

        if (strcmp(obj->objecttype, "table") == 0) {
            cmdtype = DCT_TableDropEnd;
        } else if (strcmp(obj->objecttype, "index") == 0) {
            cmdtype = DCT_ObjectDrop;
        } else {
            continue;
        }

        command = deparse_drop_command(obj->objidentity, obj->objecttype, (Node*)trigdata->parsetree);

        if (command)
            LogLogicalDDLMessage("deparse", obj->address.objectId, cmdtype,
                                    command, strlen(command) + 1);
        
    }

    return PointerGetDatum(NULL);
}
