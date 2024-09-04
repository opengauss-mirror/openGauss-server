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
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "replication/ddlmessage.h"
#include "tcop/ddldeparse.h"
#include "utils/lsyscache.h"

const char* string_objtype(ObjectType objtype, bool isgrant);

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
check_command_publishable(ddl_deparse_context context, bool rewrite)
{
    if (context.max_volatility == PROVOLATILE_VOLATILE)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot use volatile function in this command because it cannot be replicated in DDL replication")));
}

bool relation_support_ddl_replication(Oid relid, bool rewrite)
{
    bool support = true;
    
    Relation rel = relation_open(relid, AccessShareLock);
    Oid relrewrite = RelationGetRelrewriteOption(rel);
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
        support = false;
    } else if (rel->rd_rel->relkind == RELKIND_RELATION) {
        if (pg_strcasecmp(RelationGetOrientation(rel), ORIENTATION_ROW)) {
            support = false;
        } else if (OidIsValid(relrewrite)) {
            support = false;
        } else if (!RelationIsAstoreFormat(rel)) {
            support = false;
        }
    } else if (rel->rd_rel->relkind == RELKIND_INDEX && RelationIsUstoreIndex(rel)) {
        support = false;
    }

    relation_close(rel, AccessShareLock);

    return support;
}

bool type_support_ddl_replication(Oid typid)
{
    bool support = false;
    HeapTuple    typtup;
    Form_pg_type typform;

    typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typtup))
        elog(ERROR, "cache lookup failed for type with OID %u", typid);

    typform = (Form_pg_type) GETSTRUCT(typtup);
    if (typform->typtype == TYPTYPE_COMPOSITE || typform->typtype == TYPTYPE_ENUM) {
        support = true;
    }

    ReleaseSysCache(typtup);
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
        StringInfoData commandbuf;
        char         *removetype = NULL;
        char        *schemaname = NULL;
        char        *objname = NULL;
        TypeName    *typname = NULL;
        Node        *ptype = NULL;

        initStringInfo(&commandbuf);

        removetype = pstrdup(string_objtype(stmt->removeType, false));
        removetype = pg_strtolower(removetype);

        if (stmt->removeType == OBJECT_TYPE) {
            /* for DROP TYPE */
            Assert(IsA(object, List) && list_length((List*)object) >= 1);
            ptype = (Node *) linitial((List*)object);
            if (ptype->type == T_String)
                typname = makeTypeNameFromNameList((List*)object);
            else if (ptype->type == T_TypeName)
                typname = (TypeName *)ptype;

            objname = TypeNameToString(typname);
        } else {
            /* for DROP TABLE/DROP IDNEX/DROP MATERIALIZED VIEW */
            DeconstructQualifiedName((List*)object, &schemaname, &objname);
        }

        address = get_object_address(stmt->removeType,
                                     IsA(object, List) ? (List*)object : list_make1(object),
                                     NULL,
                                     &relation,
                                     AccessExclusiveLock,
                                     true);
        if (!OidIsValid(address.objectId)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("%s \"%s\" does not exist",
                                removetype, objname)));
        }

        appendStringInfo(&commandbuf, "Drop %s command start", removetype);

        pfree(removetype);

        /* Object does not exist, nothing to do */
        if (relation) {
            if (get_rel_relkind(address.objectId))
                support = relation_support_ddl_replication(address.objectId, false);

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
                                     commandbuf.data, strlen(commandbuf.data) + 1);

            relation_close(relation, NoLock);
        } else if (stmt->removeType == OBJECT_TYPE) {
            support = type_support_ddl_replication(address.objectId);
            if (support)
                    LogLogicalDDLMessage("deparse", address.objectId,
                        DCT_TypeDropStart, commandbuf.data, strlen(commandbuf.data) + 1);
        }
    }

    return PointerGetDatum(NULL);
}

static void finish_alter_table_ddl_command(CollectedCommand* cmd)
{
    ListCell *lc;
    List *cmds;
    Oid    relid;
    DeparsedCommandType type;

    relid = cmd->d.alterTable.objectId;
    type = DCT_TableAlter;

    cmds = deparse_altertable_end(cmd);
    foreach(lc, cmds) {
        char* json_string = (char*)lfirst(lc);
        if (json_string) {
            LogLogicalDDLMessage("deparse", relid, type, json_string,
                                 strlen(json_string) + 1);
        }
    }
}

/*
 * publication_deparse_table_rewrite
 *
 * Deparse the ddl table rewrite command and log it.
 */
Datum publication_deparse_table_rewrite(PG_FUNCTION_ARGS)
{
    bool        support = false;
    CollectedCommand *cmd;
    char       *json_string;

    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
        elog(ERROR, "not fired by event trigger manager");

    cmd = currentEventTriggerState->currentCommand;
    Assert(cmd && cmd->d.alterTable.rewrite);

    if (get_rel_relkind(cmd->d.alterTable.objectId))
        support = relation_support_ddl_replication(cmd->d.alterTable.objectId, true);

    /*
     * Do not generate wal log for commands whose target table is a temporary
     * or unlogged table.
     *
     * XXX We may generate wal logs for unlogged tables in the future so that
     * unlogged tables can also be created and altered on the subscriber side.
     * This makes it possible to directly replay the SET LOGGED command and the
     * incoming rewrite message without creating a new table.
     */
    if (support) {
        ddl_deparse_context context;
        context.verbose_mode = false;
        context.include_owner = true;
        context.max_volatility = PROVOLATILE_IMMUTABLE;
        /* Deparse the DDL command and WAL log it to allow decoding of the same. */
        json_string = deparse_utility_command(cmd, &context);
        if (json_string != NULL) {
            check_command_publishable(context, true);
            LogLogicalDDLMessage("deparse", cmd->d.alterTable.objectId, DCT_TableAlter,
                                 json_string, strlen(json_string) + 1);
        }
    }

    return PointerGetDatum(NULL);
}

/*
 * Deparse the ddl command and log it. This function
 * is called after the execution of the command but before the
 * transaction commits.
 */
Datum publication_deparse_ddl_command_end(PG_FUNCTION_ARGS)
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

        /* Rewrite DDL has been handled in table_rewrite trigger */
        if (cmd->d.alterTable.rewrite) {
            if (cmd->type == SCT_AlterTable) {
                relid = cmd->d.alterTable.objectId;
                support = relation_support_ddl_replication(relid, true);
                if (support) {
                    finish_alter_table_ddl_command(cmd);
                }
                continue;
            } else if (cmd->parsetree && IsA(cmd->parsetree, RenameStmt)) {
                RenameStmt *renameStmt = (RenameStmt *) cmd->parsetree;

                if (renameStmt && renameStmt->relationType != OBJECT_TYPE &&
                    renameStmt->relationType != OBJECT_TABLE)
                    continue;
            }
        }

        if (cmd->type == SCT_Simple && cmd->parsetree &&
            !OidIsValid(cmd->d.simple.address.objectId)) {
            relid = cmd->d.simple.address.objectId;
            type = DCT_SimpleCmd;
            /*
             * handle some syntax which can not be capture by event trigger
             * like rename table in dbcompatibility B
             */
            if (IsA(cmd->parsetree, RenameStmt)) {
                RenameStmt *renameStmt = (RenameStmt *) cmd->parsetree;
                if (renameStmt->renameTableflag && renameStmt->renameTargetList) {
                    context.verbose_mode = false;
                    context.include_owner = true;
                    context.max_volatility = PROVOLATILE_IMMUTABLE;
                    json_string = deparse_utility_command(cmd, &context);

                    if (json_string != NULL) {
                        check_command_publishable(context, false);
                        LogLogicalDDLMessage("deparse", relid, type, json_string, strlen(json_string) + 1);
                    }
                }
            } else if (DB_IS_CMPT(B_FORMAT) && (IsA(cmd->parsetree, CreateEventStmt) ||
                    IsA(cmd->parsetree, AlterEventStmt) ||
                    IsA(cmd->parsetree, DropEventStmt))) {
                context.verbose_mode = false;
                context.include_owner = true;
                context.max_volatility = PROVOLATILE_IMMUTABLE;
                json_string = deparse_utility_command(cmd, &context);
                if (json_string != NULL) {
                    LogLogicalDDLMessage("deparse", relid, type, json_string,
                                         strlen(json_string) + 1);
                }
            }

            continue;
        }

        if (cmd->type == SCT_AlterTable) {
            relid = cmd->d.alterTable.objectId;
            type = DCT_TableAlter;
        } else {
            /* Only SCT_Simple for now */
            relid = cmd->d.simple.address.objectId;
            type = DCT_SimpleCmd;
        }
        
        if (get_rel_relkind(relid))
            support = relation_support_ddl_replication(relid);
        else if (cmd->d.simple.address.classId == TypeRelationId)
            support = type_support_ddl_replication(relid);

        if (support) {
            /*
             * Deparse the DDL command and WAL log it to allow decoding of the
             * same.
             */
            context.verbose_mode = false;
            context.include_owner = true;
            context.max_volatility = PROVOLATILE_IMMUTABLE;

            json_string = deparse_utility_command(cmd, &context);

            if (json_string != NULL) {
                check_command_publishable(context, false);
                LogLogicalDDLMessage("deparse", relid, type, json_string,
                                     strlen(json_string) + 1);
            }
            if (cmd->type == SCT_AlterTable) {
                finish_alter_table_ddl_command(cmd);
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

        if (strcmp(obj->objecttype, "table") == 0 ||
                strcmp(obj->objecttype, "index") == 0) {
            cmdtype = DCT_TableDropEnd;
        } else if (strcmp(obj->objecttype, "type") == 0) {
            cmdtype = DCT_TypeDropEnd;
        } else if (strcmp(obj->objecttype, "schema") == 0 ||
                 strcmp(obj->objecttype, "index") == 0 ||
                 strcmp(obj->objecttype, "sequence") == 0 ||
                 strcmp(obj->objecttype, "large sequence") == 0 ||
                 strcmp(obj->objecttype, "view") == 0 ||
                 strcmp(obj->objecttype, "function") == 0 ||
                 strcmp(obj->objecttype, "trigger") == 0 ||
                 strcmp(obj->objecttype, "function") == 0) {
            cmdtype = DCT_ObjectDrop;
        } else {
            continue;
        }

        if (strcmp(obj->objecttype, "schema") == 0 &&
            (isTempNamespaceName(obj->objname) || isToastTempNamespaceName(obj->objname)
                || strcmp(obj->objidentity, "pg_temp") == 0)) {
            continue;
        }

        if (!IsA((Node*)trigdata->parsetree, DropStmt)) {
            continue;
        }

        command = deparse_drop_command(obj->objidentity, obj->objecttype, (Node*)trigdata->parsetree);

        if (command)
            LogLogicalDDLMessage("deparse", obj->address.objectId, cmdtype,
                                    command, strlen(command) + 1);
        
    }

    return PointerGetDatum(NULL);
}
