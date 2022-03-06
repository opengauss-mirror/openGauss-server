/* -------------------------------------------------------------------------
 *
 * foreigncmds.cpp
 *	  foreign-data wrapper/server creation/manipulation commands
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/foreigncmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/reloptions.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "cipher.h"
#include "commands/defrem.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "foreign/dummyserver.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parser.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "storage/smgr/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "catalog/toasting.h"
#include "bulkload/dist_fdw.h"

/* Sensitive options for user mapping, will be encrypted when saved to catalog. */
const char* g_sensitiveOptionsArray[] = {"password"};
const int g_sensitiveArrayLength = lengthof(g_sensitiveOptionsArray);

/*
 * Convert a DefElem list to the text array format that is used in
 * pg_foreign_data_wrapper, pg_foreign_server, and pg_user_mapping.
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * Note: The array is usually stored to database without further
 * processing, hence any validation should be done before this
 * conversion.
 */
Datum optionListToArray(List* options)
{
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    errno_t rc = EOK;

    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        const char* value = NULL;
        Size len;
        text* t = NULL;

        value = defGetString(def);
        len = VARHDRSZ + strlen(def->defname) + 1 + strlen(value);
        t = (text*)palloc(len + 1);
        SET_VARSIZE(t, len);
        rc = sprintf_s(VARDATA(t), len + 1, "%s=%s", def->defname, value);
        securec_check_ss(rc, "", "");

        astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
    }

    if (astate != NULL)
        return makeArrayResult(astate, CurrentMemoryContext);

    return PointerGetDatum(NULL);
}

/*
 * Transform a list of DefElem into text array format.	This is substantially
 * the same thing as optionListToArray(), except we recognize SET/ADD/DROP
 * actions for modifying an existing list of options, which is passed in
 * Datum form as oldOptions.  Also, if fdwvalidator isn't InvalidOid
 * it specifies a validator function to call on the result.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * This is used by CREATE/ALTER of FOREIGN DATA WRAPPER/SERVER/USER MAPPING.
 */
Datum transformGenericOptions(Oid catalogId, Datum oldOptions, List* options, Oid fdwvalidator)
{
    List* resultOptions = untransformRelOptions(oldOptions);
    ListCell* optcell = NULL;
    Datum result;

    foreach (optcell, options) {
        DefElem* od = (DefElem*)lfirst(optcell);
        ListCell* cell = NULL;
        ListCell* prev = NULL;

        /*
         * Find the element in resultOptions.  We need this for validation in
         * all cases.  Also identify the previous element.
         */
        foreach (cell, resultOptions) {
            DefElem* def = (DefElem*)lfirst(cell);

            if (strcmp(def->defname, od->defname) == 0)
                break;
            else
                prev = cell;
        }

        /*
         * It is possible to perform multiple SET/DROP actions on the same
         * option.	The standard permits this, as long as the options to be
         * added are unique.  Note that an unspecified action is taken to be
         * ADD.
         */
        switch (od->defaction) {
            case DEFELEM_DROP:
                if (cell == NULL)
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("option \"%s\" not found", od->defname)));
                resultOptions = list_delete_cell(resultOptions, cell, prev);
                break;

            case DEFELEM_SET:
                if (cell == NULL)
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("option \"%s\" not found", od->defname)));
                lfirst(cell) = od;
                break;

            case DEFELEM_ADD:
            case DEFELEM_UNSPEC:
                if (cell != NULL)
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("option \"%s\" provided more than once", od->defname)));
                resultOptions = lappend(resultOptions, od);
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("unrecognized action %d on option \"%s\"", (int)od->defaction, od->defname)));
                break;
        }
    }

    result = optionListToArray(resultOptions);

    if (OidIsValid(fdwvalidator)) {
        Datum valarg = result;

        /*
         * Pass a null options list as an empty array, so that validators
         * don't have to be declared non-strict to handle the case.
         */
        if (DatumGetPointer(valarg) == NULL)
            valarg = PointerGetDatum(construct_empty_array(TEXTOID));
        OidFunctionCall2(fdwvalidator, valarg, ObjectIdGetDatum(catalogId));
    }
    list_free(resultOptions);

    return result;
}

/*
 * Convert the user mapping user name to OID
 */
static Oid GetUserOidFromMapping(const char* username, bool missing_ok)
{
    if (username == NULL)
        /* PUBLIC user mapping */
        return InvalidOid;

    if (strcmp(username, "current_user") == 0)
        /* map to the owner */
        return GetUserId();

    /* map to provided user */
    return get_role_oid(username, missing_ok);
}

/*
 * Rename foreign-data wrapper
 */
void RenameForeignDataWrapper(const char* oldname, const char* newname)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(tup))
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign-data wrapper \"%s\" does not exist", oldname)));

    /* make sure the new name doesn't exist */
    if (SearchSysCacheExists1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(newname)))
        ereport(
            ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("foreign-data wrapper \"%s\" already exists", newname)));

    /* must be owner of FDW */
    if (!pg_foreign_data_wrapper_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FDW, oldname);

    /* rename */
    (void)namestrcpy(&(((Form_pg_foreign_data_wrapper)GETSTRUCT(tup))->fdwname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    heap_close(rel, NoLock);
    heap_freetuple(tup);
}

/*
 * Must be owner or have alter privilege to alter server
 */
static void AlterServerPermissionCheck(Oid srvId, const char* servername)
{
    AclResult aclresult = pg_foreign_server_aclcheck(srvId, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_foreign_server_ownercheck(srvId, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, servername);
    }
}

/*
 * Rename foreign server
 */
void RenameForeignServer(const char* oldname, const char* newname)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("server \"%s\" does not exist", oldname)));

    /* make sure the new name doesn't exist */
    if (SearchSysCacheExists1(FOREIGNSERVERNAME, CStringGetDatum(newname)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("server \"%s\" already exists", newname)));

    /* Permission check. */
    AlterServerPermissionCheck(HeapTupleGetOid(tup), oldname);

    /* rename */
    (void)namestrcpy(&(((Form_pg_foreign_server)GETSTRUCT(tup))->srvname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    heap_close(rel, NoLock);
    heap_freetuple(tup);
}

/*
 * Internal workhorse for changing a data wrapper's owner.
 *
 * Allow this only for superusers; also the new owner must be a
 * superuser.
 */
static void AlterForeignDataWrapperOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_foreign_data_wrapper form;

    form = (Form_pg_foreign_data_wrapper)GETSTRUCT(tup);

    /* Must be a superuser to change a FDW owner */
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to change owner of foreign-data wrapper \"%s\"", NameStr(form->fdwname)),
                errhint("Must be system admin to change owner of a foreign-data wrapper.")));

    /* New owner must also be a superuser */
    /* Database Security:  Support separation of privilege. */
    if (!(superuser_arg(newOwnerId) || systemDBA_arg(newOwnerId)))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to change owner of foreign-data wrapper \"%s\"", NameStr(form->fdwname)),
                errhint("The owner of a foreign-data wrapper must be a system admin.")));

    if (form->fdwowner != newOwnerId) {
        form->fdwowner = newOwnerId;

        simple_heap_update(rel, &tup->t_self, tup);
        CatalogUpdateIndexes(rel, tup);

        /* Update owner dependency reference */
        changeDependencyOnOwner(ForeignDataWrapperRelationId, HeapTupleGetOid(tup), newOwnerId);
    }
}

/*
 * Change foreign-data wrapper owner -- by name
 *
 * Note restrictions in the "_internal" function, above.
 */
void AlterForeignDataWrapperOwner(const char* name, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(name));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign-data wrapper \"%s\" does not exist", name)));

    AlterForeignDataWrapperOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Change foreign-data wrapper owner -- by OID
 *
 * Note restrictions in the "_internal" function, above.
 */
void AlterForeignDataWrapperOwner_oid(Oid fwdId, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fwdId));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign-data wrapper with OID %u does not exist", fwdId)));

    AlterForeignDataWrapperOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Internal workhorse for changing a foreign server's owner
 */
static void AlterForeignServerOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_foreign_server form;

    form = (Form_pg_foreign_server)GETSTRUCT(tup);

    if (form->srvowner != newOwnerId) {
        /* Superusers can always do it */
        if (!superuser()) {
            Oid srvId;
            AclResult aclresult;

            srvId = HeapTupleGetOid(tup);

            /* Must be owner */
            if (!pg_foreign_server_ownercheck(srvId, GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FOREIGN_SERVER, NameStr(form->srvname));

            /* Must be able to become new owner */
            check_is_member_of_role(GetUserId(), newOwnerId);

            /* New owner must have USAGE privilege on foreign-data wrapper */
            aclresult = pg_foreign_data_wrapper_aclcheck(form->srvfdw, newOwnerId, ACL_USAGE);
            if (aclresult != ACLCHECK_OK) {
                ForeignDataWrapper* fdw = GetForeignDataWrapper(form->srvfdw);

                aclcheck_error(aclresult, ACL_KIND_FDW, fdw->fdwname);
            }
        }

        form->srvowner = newOwnerId;

        simple_heap_update(rel, &tup->t_self, tup);
        CatalogUpdateIndexes(rel, tup);

        /* Update owner dependency reference */
        changeDependencyOnOwner(ForeignServerRelationId, HeapTupleGetOid(tup), newOwnerId);
    }
}

/*
 * Change foreign server owner -- by name
 */
void AlterForeignServerOwner(const char* name, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, CStringGetDatum(name));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("server \"%s\" does not exist", name)));

    AlterForeignServerOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Change foreign server owner -- by OID
 */
void AlterForeignServerOwner_oid(Oid srvId, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(FOREIGNSERVEROID, ObjectIdGetDatum(srvId));

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign server with OID %u does not exist", srvId)));

    AlterForeignServerOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Convert a handler function name passed from the parser to an Oid.
 */
static Oid lookup_fdw_handler_func(DefElem* handler)
{
    Oid handlerOid;

    if (handler == NULL || handler->arg == NULL)
        return InvalidOid;

    /* handlers have no arguments */
    handlerOid = LookupFuncName((List*)handler->arg, 0, NULL, false);

    /* check that handler has correct return type */
    if (get_func_rettype(handlerOid) != FDW_HANDLEROID)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("function %s must return type \"fdw_handler\"", NameListToString((List*)handler->arg))));

    return handlerOid;
}

/*
 * Convert a validator function name passed from the parser to an Oid.
 */
static Oid lookup_fdw_validator_func(DefElem* validator)
{
    Oid funcargtypes[2];

    if (validator == NULL || validator->arg == NULL)
        return InvalidOid;

    /* validators take text[], oid */
    funcargtypes[0] = TEXTARRAYOID;
    funcargtypes[1] = OIDOID;

    return LookupFuncName((List*)validator->arg, 2, funcargtypes, false);
    /* validator's return value is ignored, so we don't check the type */
}

/*
 * Process function options of CREATE/ALTER FDW
 */
static void parse_func_options(
    List* func_options, bool* handler_given, Oid* fdwhandler, bool* validator_given, Oid* fdwvalidator)
{
    ListCell* cell = NULL;

    *handler_given = false;
    *validator_given = false;
    /* return InvalidOid if not given */
    *fdwhandler = InvalidOid;
    *fdwvalidator = InvalidOid;

    foreach (cell, func_options) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (strcmp(def->defname, "handler") == 0) {
            if (*handler_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            *handler_given = true;
            *fdwhandler = lookup_fdw_handler_func(def);
        } else if (strcmp(def->defname, "validator") == 0) {
            if (*validator_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            *validator_given = true;
            *fdwvalidator = lookup_fdw_validator_func(def);
        } else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("option \"%s\" not recognized", def->defname)));
    }
}

/*
 * Create a foreign-data wrapper
 */
void CreateForeignDataWrapper(CreateFdwStmt* stmt)
{
    Relation rel;
    Datum values[Natts_pg_foreign_data_wrapper];
    bool nulls[Natts_pg_foreign_data_wrapper];
    HeapTuple tuple;
    Oid fdwId;
    bool handler_given = false;
    bool validator_given = false;
    Oid fdwhandler;
    Oid fdwvalidator;
    Datum fdwoptions;
    Oid ownerId;
    ObjectAddress myself;
    ObjectAddress referenced;
    errno_t ret = EOK;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    /* Must be super user */
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create foreign-data wrapper \"%s\"", stmt->fdwname),
                errhint("Must be system admin to create a foreign-data wrapper.")));

    /* For now the owner cannot be specified on create. Use effective user ID. */
    ownerId = GetUserId();

    /*
     * Check that there is no other foreign-data wrapper by this name.
     */
    if (GetForeignDataWrapperByName(stmt->fdwname, true) != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("foreign-data wrapper \"%s\" already exists", stmt->fdwname)));

    /*
     * Insert tuple into pg_foreign_data_wrapper.
     */
    ret = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ret, "", "");
    ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ret, "", "");

    values[Anum_pg_foreign_data_wrapper_fdwname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->fdwname));
    values[Anum_pg_foreign_data_wrapper_fdwowner - 1] = ObjectIdGetDatum(ownerId);

    /* Lookup handler and validator functions, if given */
    parse_func_options(stmt->func_options, &handler_given, &fdwhandler, &validator_given, &fdwvalidator);

    values[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = ObjectIdGetDatum(fdwhandler);
    values[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = ObjectIdGetDatum(fdwvalidator);

    nulls[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;

    fdwoptions =
        transformGenericOptions(ForeignDataWrapperRelationId, PointerGetDatum(NULL), stmt->options, fdwvalidator);

    if (PointerIsValid(DatumGetPointer(fdwoptions)))
        values[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = fdwoptions;
    else
        nulls[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);

    fdwId = simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple(tuple);

    /* record dependencies */
    myself.classId = ForeignDataWrapperRelationId;
    myself.objectId = fdwId;
    myself.objectSubId = 0;

    if (OidIsValid(fdwhandler)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwhandler;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(fdwvalidator)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = fdwvalidator;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    recordDependencyOnOwner(ForeignDataWrapperRelationId, fdwId, ownerId);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new foreign data wrapper */
    InvokeObjectAccessHook(OAT_POST_CREATE, ForeignDataWrapperRelationId, fdwId, 0, NULL);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Alter foreign-data wrapper
 */
void AlterForeignDataWrapper(AlterFdwStmt* stmt)
{
    Relation rel;
    HeapTuple tp;
    Form_pg_foreign_data_wrapper fdwForm;
    Datum repl_val[Natts_pg_foreign_data_wrapper];
    bool repl_null[Natts_pg_foreign_data_wrapper];
    bool repl_repl[Natts_pg_foreign_data_wrapper];
    Oid fdwId;
    bool isnull = false;
    Datum datum;
    bool handler_given = false;
    bool validator_given = false;
    Oid fdwhandler;
    Oid fdwvalidator;
    errno_t ret = EOK;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    /* Must be super user */
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to alter foreign-data wrapper \"%s\"", stmt->fdwname),
                errhint("Must be system admin to alter a foreign-data wrapper.")));

    tp = SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(stmt->fdwname));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign-data wrapper \"%s\" does not exist", stmt->fdwname)));

    fdwForm = (Form_pg_foreign_data_wrapper)GETSTRUCT(tp);
    fdwId = HeapTupleGetOid(tp);

    ret = memset_s(repl_val, sizeof(repl_val), 0, sizeof(repl_val));
    securec_check(ret, "", "");
    ret = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
    securec_check(ret, "", "");
    ret = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
    securec_check(ret, "", "");

    parse_func_options(stmt->func_options, &handler_given, &fdwhandler, &validator_given, &fdwvalidator);

    if (handler_given) {
        repl_val[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = ObjectIdGetDatum(fdwhandler);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwhandler - 1] = true;

        /*
         * It could be that the behavior of accessing foreign table changes
         * with the new handler.  Warn about this.
         */
        ereport(WARNING,
            (errmsg("changing the foreign-data wrapper handler can change behavior of existing foreign tables")));
    }

    if (validator_given) {
        repl_val[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = ObjectIdGetDatum(fdwvalidator);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwvalidator - 1] = true;

        /*
         * It could be that the options for the FDW, SERVER and USER MAPPING
         * are no longer valid with the new validator.	Warn about this.
         */
        if (OidIsValid(fdwvalidator))
            ereport(WARNING,
                (errmsg("changing the foreign-data wrapper validator can cause "
                        "the options for dependent objects to become invalid")));
    } else {
        /*
         * Validator is not changed, but we need it for validating options.
         */
        fdwvalidator = fdwForm->fdwvalidator;
    }

    /*
     * If options specified, validate and update.
     */
    if (stmt->options) {
        /* Extract the current options */
        datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tp, Anum_pg_foreign_data_wrapper_fdwoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        /* Transform the options */
        datum = transformGenericOptions(ForeignDataWrapperRelationId, datum, stmt->options, fdwvalidator);

        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = datum;
        else
            repl_null[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;

        repl_repl[Anum_pg_foreign_data_wrapper_fdwoptions - 1] = true;
    }

    /* Everything looks good - update the tuple */
    tp = (HeapTuple) tableam_tops_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    simple_heap_update(rel, &tp->t_self, tp);
    CatalogUpdateIndexes(rel, tp);

    heap_freetuple(tp);

    /* Update function dependencies if we changed them */
    if (handler_given || validator_given) {
        ObjectAddress myself;
        ObjectAddress referenced;

        /*
         * Flush all existing dependency records of this FDW on functions; we
         * assume there can be none other than the ones we are fixing.
         */
        (void)deleteDependencyRecordsForClass(
            ForeignDataWrapperRelationId, fdwId, ProcedureRelationId, DEPENDENCY_NORMAL);

        /* And build new ones. */
        myself.classId = ForeignDataWrapperRelationId;
        myself.objectId = fdwId;
        myself.objectSubId = 0;

        if (OidIsValid(fdwhandler)) {
            referenced.classId = ProcedureRelationId;
            referenced.objectId = fdwhandler;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }

        if (OidIsValid(fdwvalidator)) {
            referenced.classId = ProcedureRelationId;
            referenced.objectId = fdwvalidator;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }
    }

    heap_close(rel, RowExclusiveLock);
}

/*
 * Drop foreign-data wrapper by OID
 */
void RemoveForeignDataWrapperById(Oid fdwId)
{
    HeapTuple tp;
    Relation rel;

    rel = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwId));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for foreign-data wrapper %u", fdwId)));

    simple_heap_delete(rel, &tp->t_self);

    ReleaseSysCache(tp);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Create a foreign server
 */
void CreateForeignServer(CreateForeignServerStmt* stmt)
{
    Relation rel;
    Datum srvoptions;
    Datum values[Natts_pg_foreign_server];
    bool nulls[Natts_pg_foreign_server];
    HeapTuple tuple;
    Oid srvId;
    Oid ownerId;
    AclResult aclresult;
    ObjectAddress myself;
    ObjectAddress referenced;
    ForeignDataWrapper* fdw = NULL;
    errno_t ret = EOK;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    /* For now the owner cannot be specified on create. Use effective user ID. */
    ownerId = GetUserId();

    /*
     * Check that there is no other foreign server by this name.
     */
    if (GetForeignServerByName(stmt->servername, true) != NULL)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("server \"%s\" already exists", stmt->servername)));

    /*
     * Check that the FDW exists and that we have USAGE on it. Also get the
     * actual FDW for option validation etc.
     */
    fdw = GetForeignDataWrapperByName(stmt->fdwname, false);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdw->fdwid, ownerId, ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_FDW, fdw->fdwname);

#ifdef ENABLE_MOT
    /* Creating additional MOT server is disallowed here because of the following reasons:
     * 1. While using the FDW interface, MOT table is not really "foreign" but rather local. The original idea of FDW
     *    that came with openGauss itself was only a set of interface and function calls to remote, and therefore there
     *    is nothing to drop locally. MOT table does have actually data to be dropped cascade whenever a dependent
     *    database is dropped. We could not simply just remove the entries in catalog.
     * 2. MOT engine down below does not have the mapping between table and database,
     *    in other words, the idea of schema. But new foreign servers are database-dependent (pg_foreign_server).
     * 3. In the current implementation, dropping a foreign table itself cannot successfully delete the its entry in
     * pg_shdepend.
     *
     * We could potentially solve this in the future by adding metadata of schema in the engine layer, which would need
     * to sync correctly with pg_catalog.
     */
    if (strcmp(stmt->fdwname, MOT_FDW) == 0 && strcmp(stmt->servername, MOT_FDW_SERVER) != 0) {
        ereport(ERROR, (errmodule(MOD_MOT), errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                errmsg("Creating additional server with %s is not allowed.", MOT_FDW)));
    }
#endif

    /*
     * Insert tuple into pg_foreign_server.
     */
    ret = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ret, "", "");
    ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ret, "", "");

    values[Anum_pg_foreign_server_srvname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->servername));
    values[Anum_pg_foreign_server_srvowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_foreign_server_srvfdw - 1] = ObjectIdGetDatum(fdw->fdwid);

    /* Add server type if supplied */
    if (stmt->servertype)
        values[Anum_pg_foreign_server_srvtype - 1] = CStringGetTextDatum(stmt->servertype);
    else
        nulls[Anum_pg_foreign_server_srvtype - 1] = true;

    /* Add server version if supplied */
    if (stmt->version)
        values[Anum_pg_foreign_server_srvversion - 1] = CStringGetTextDatum(stmt->version);
    else
        nulls[Anum_pg_foreign_server_srvversion - 1] = true;

    /* Start with a blank acl */
    nulls[Anum_pg_foreign_server_srvacl - 1] = true;

    /* Add server options */
    srvoptions =
        transformGenericOptions(ForeignServerRelationId, PointerGetDatum(NULL), stmt->options, fdw->fdwvalidator);
    if (PointerIsValid(DatumGetPointer(srvoptions))) {
        List* resultOptions = untransformRelOptions(srvoptions);
        checkExistDummyServer(resultOptions);
        encryptOBSForeignTableOption(&resultOptions);
        srvoptions = optionListToArray(resultOptions);
    }

    if (PointerIsValid(DatumGetPointer(srvoptions)))
        values[Anum_pg_foreign_server_srvoptions - 1] = srvoptions;
    else
        nulls[Anum_pg_foreign_server_srvoptions - 1] = true;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);

    srvId = simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple(tuple);

    /* record dependencies */
    myself.classId = ForeignServerRelationId;
    myself.objectId = srvId;
    myself.objectSubId = 0;

    referenced.classId = ForeignDataWrapperRelationId;
    referenced.objectId = fdw->fdwid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnOwner(ForeignServerRelationId, srvId, ownerId);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new foreign server */
    InvokeObjectAccessHook(OAT_POST_CREATE, ForeignServerRelationId, srvId, 0, NULL);

    heap_close(rel, RowExclusiveLock);

    /* enable rls(row level security) for pg_foreign_server when create server with gc_fdw by dba. */
    if (superuser() && pg_strcasecmp(stmt->fdwname, GC_FDW) == 0) {
        Oid rlsPolicyId = get_rlspolicy_oid(ForeignServerRelationId, "pg_foreign_server_rls", true);
        if (OidIsValid(rlsPolicyId) == false) {
            CreateRlsPolicyForSystem(
                "pg_catalog", "pg_foreign_server", "pg_foreign_server_rls", "has_server_privilege", "oid", "usage");
            Relation tmp_rel = relation_open(ForeignServerRelationId, ShareUpdateExclusiveLock);
            ATExecEnableDisableRls(tmp_rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
            relation_close(tmp_rel, ShareUpdateExclusiveLock);
        }
    }
}

/*
 * Alter foreign server
 */
void AlterForeignServer(AlterForeignServerStmt* stmt)
{
    Relation rel;
    HeapTuple tp;
    Datum repl_val[Natts_pg_foreign_server];
    bool repl_null[Natts_pg_foreign_server];
    bool repl_repl[Natts_pg_foreign_server];
    Oid srvId;
    Form_pg_foreign_server srvForm;
    errno_t ret = EOK;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    tp = SearchSysCacheCopy1(FOREIGNSERVERNAME, CStringGetDatum(stmt->servername));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("server \"%s\" does not exist", stmt->servername)));

    srvId = HeapTupleGetOid(tp);
    srvForm = (Form_pg_foreign_server)GETSTRUCT(tp);

    /* Permission check */
    AlterServerPermissionCheck(srvId, stmt->servername);

    ret = memset_s(repl_val, sizeof(repl_val), 0, sizeof(repl_val));
    securec_check(ret, "", "");
    ret = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
    securec_check(ret, "", "");
    ret = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
    securec_check(ret, "", "");

    if (stmt->has_version) {
        /*
         * Change the server VERSION string.
         */
        if (stmt->version)
            repl_val[Anum_pg_foreign_server_srvversion - 1] = CStringGetTextDatum(stmt->version);
        else
            repl_null[Anum_pg_foreign_server_srvversion - 1] = true;

        repl_repl[Anum_pg_foreign_server_srvversion - 1] = true;
    }

    ForeignDataWrapper* fdw = GetForeignDataWrapper(srvForm->srvfdw);

    /* password with prefix "encryptstr" is not allowed for gc_fdw with alter server statement. */
    if (pg_strcasecmp(fdw->fdwname, GC_FDW) == 0 && stmt->options) {
        ListCell* cell = NULL;
        foreach (cell, stmt->options) {
            DefElem* def = (DefElem*)lfirst(cell);
            if (pg_strcasecmp(def->defname, "password") == 0) {
                char* password = defGetString(def);
                bool is_encrypted = isEncryptedPassword(password);
                errno_t rc = memset_s(password, strlen(password) + 1, 0, strlen(password) + 1);
                securec_check(rc, "\0", "\0");
                if (is_encrypted)
                    ereport(
                        ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("An encrypted password is not allowed.")));
            }
        }
    }

    if (stmt->options) {
        Datum datum;
        bool isnull = false;

        /* Extract the current srvoptions */
        datum = SysCacheGetAttr(FOREIGNSERVEROID, tp, Anum_pg_foreign_server_srvoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        /* Prepare the options array */
        datum = transformGenericOptions(ForeignServerRelationId, datum, stmt->options, fdw->fdwvalidator);
        if (PointerIsValid(DatumGetPointer(datum))) {
            List* resultOptions = untransformRelOptions(datum);
            encryptOBSForeignTableOption(&resultOptions);
            datum = optionListToArray(resultOptions);
        }

        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_foreign_server_srvoptions - 1] = datum;
        else
            repl_null[Anum_pg_foreign_server_srvoptions - 1] = true;

        repl_repl[Anum_pg_foreign_server_srvoptions - 1] = true;
    }

    /* Everything looks good - update the tuple */
    tp = (HeapTuple) tableam_tops_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    simple_heap_update(rel, &tp->t_self, tp);
    CatalogUpdateIndexes(rel, tp);

    heap_freetuple(tp);

    heap_close(rel, RowExclusiveLock);

    char* typeName = getServerOptionValue(srvId, "type");
    if (NULL == typeName) {
        /* hdfs foreign server without option 'type' */
        if (0 == pg_strcasecmp(fdw->fdwname, HDFS_FDW)) {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
        }
    } else if (0 == pg_strcasecmp(typeName, HDFS)) {
        FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    }else if (0 == pg_strcasecmp(typeName, OBS)) {
#ifndef ENABLE_LITE_MODE
        (void)dfs::InvalidOBSConnectorCache(srvId);
#else
        FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    } else if (0 == pg_strcasecmp(typeName, DUMMY_SERVER)) {
        InvalidDummyServerCache(srvId);
    }
}

/*
 * Drop foreign server by OID
 */
void RemoveForeignServerById(Oid srvId)
{
    HeapTuple tp;
    Relation rel;

    rel = heap_open(ForeignServerRelationId, RowExclusiveLock);

    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(srvId));

    if (!HeapTupleIsValid(tp))
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for foreign server %u", srvId)));

    Form_pg_foreign_server serverform = (Form_pg_foreign_server)GETSTRUCT(tp);
    ForeignDataWrapper* fdw = GetForeignDataWrapper(serverform->srvfdw);
    const bool is_hdfs_fdw = (0 == pg_strcasecmp(fdw->fdwname, HDFS_FDW));

    simple_heap_delete(rel, &tp->t_self);

    ReleaseSysCache(tp);

    heap_close(rel, RowExclusiveLock);

    char* typeName = getServerOptionValue(srvId, "type");
    if (NULL == typeName) {
        /* hdfs foreign server without option 'type' */
        if (is_hdfs_fdw) {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
        }
    } else if (0 == pg_strcasecmp(typeName, HDFS)) {
        FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    } else if (0 == pg_strcasecmp(typeName, OBS)) {
#ifndef ENABLE_LITE_MODE
        (void)dfs::InvalidOBSConnectorCache(srvId);
#else
        FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    } else if (0 == pg_strcasecmp(typeName, DUMMY_SERVER)) {
        InvalidDummyServerCache(srvId);
    }
}

/*
 * Common routine to check permission for user-mapping-related DDL
 * commands.  We allow server owners to operate on any mapping, and
 * users to operate on their own mapping.
 */
static void user_mapping_ddl_aclcheck(Oid umuserid, Oid serverid, const char* servername)
{
    Oid curuserid = GetUserId();

    if (!pg_foreign_server_ownercheck(serverid, curuserid)) {
        if (umuserid == curuserid) {
            AclResult aclresult;

            aclresult = pg_foreign_server_aclcheck(serverid, curuserid, ACL_USAGE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, servername);
        } else
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FOREIGN_SERVER, servername);
    }
}

/*
 * Create user mapping
 */
void CreateUserMapping(CreateUserMappingStmt* stmt)
{
    Relation rel;
    Datum useoptions;
    Datum values[Natts_pg_user_mapping];
    bool nulls[Natts_pg_user_mapping];
    HeapTuple tuple;
    Oid useId;
    Oid umId;
    ObjectAddress myself;
    ObjectAddress referenced;
    ForeignServer* srv = NULL;
    ForeignDataWrapper* fdw = NULL;
    errno_t ret = EOK;

    rel = heap_open(UserMappingRelationId, RowExclusiveLock);

    useId = GetUserOidFromMapping(stmt->username, false);

    /* Check that the server exists. */
    srv = GetForeignServerByName(stmt->servername, false);

    user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

    /*
     * Check that the user mapping is unique within server.
     */
    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(useId), ObjectIdGetDatum(srv->serverid));
    if (OidIsValid(umId))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("user mapping \"%s\" already exists for server %s", MappingUserName(useId), stmt->servername)));

    fdw = GetForeignDataWrapper(srv->fdwid);

    /*
     * Insert tuple into pg_user_mapping.
     */
    ret = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ret, "", "");
    ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ret, "", "");

    values[Anum_pg_user_mapping_umuser - 1] = ObjectIdGetDatum(useId);
    values[Anum_pg_user_mapping_umserver - 1] = ObjectIdGetDatum(srv->serverid);

    EncryptGenericOptions(stmt->options, g_sensitiveOptionsArray, g_sensitiveArrayLength, USER_MAPPING_MODE);

    /* Add user options */
    useoptions =
        transformGenericOptions(UserMappingRelationId, PointerGetDatum(NULL), stmt->options, fdw->fdwvalidator);

    if (PointerIsValid(DatumGetPointer(useoptions)))
        values[Anum_pg_user_mapping_umoptions - 1] = useoptions;
    else
        nulls[Anum_pg_user_mapping_umoptions - 1] = true;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);

    umId = simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple(tuple);

    /* Add dependency on the server */
    myself.classId = UserMappingRelationId;
    myself.objectId = umId;
    myself.objectSubId = 0;

    referenced.classId = ForeignServerRelationId;
    referenced.objectId = srv->serverid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    if (OidIsValid(useId)) {
        /* Record the mapped user dependency */
        recordDependencyOnOwner(UserMappingRelationId, umId, useId);
    }

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new user mapping */
    InvokeObjectAccessHook(OAT_POST_CREATE, UserMappingRelationId, umId, 0, NULL);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Alter user mapping
 */
void AlterUserMapping(AlterUserMappingStmt* stmt)
{
    Relation rel;
    HeapTuple tp;
    Datum repl_val[Natts_pg_user_mapping];
    bool repl_null[Natts_pg_user_mapping];
    bool repl_repl[Natts_pg_user_mapping];
    Oid useId;
    Oid umId;
    ForeignServer* srv = NULL;
    errno_t ret = EOK;

    rel = heap_open(UserMappingRelationId, RowExclusiveLock);

    useId = GetUserOidFromMapping(stmt->username, false);
    srv = GetForeignServerByName(stmt->servername, false);

    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(useId), ObjectIdGetDatum(srv->serverid));
    if (!OidIsValid(umId))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("user mapping \"%s\" does not exist for the server", MappingUserName(useId))));

    user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

    tp = SearchSysCacheCopy1(USERMAPPINGOID, ObjectIdGetDatum(umId));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for user mapping %u", umId)));

    ret = memset_s(repl_val, sizeof(repl_val), 0, sizeof(repl_val));
    securec_check(ret, "", "");
    ret = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
    securec_check(ret, "", "");
    ret = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
    securec_check(ret, "", "");

    if (stmt->options) {
        ForeignDataWrapper* fdw = NULL;
        Datum datum;
        bool isnull = false;

        /*
         * Process the options.
         */
        fdw = GetForeignDataWrapper(srv->fdwid);

        datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp, Anum_pg_user_mapping_umoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        EncryptGenericOptions(stmt->options, g_sensitiveOptionsArray, g_sensitiveArrayLength, USER_MAPPING_MODE);

        /* Prepare the options array */
        datum = transformGenericOptions(UserMappingRelationId, datum, stmt->options, fdw->fdwvalidator);

        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_user_mapping_umoptions - 1] = datum;
        else
            repl_null[Anum_pg_user_mapping_umoptions - 1] = true;

        repl_repl[Anum_pg_user_mapping_umoptions - 1] = true;
    }

    /* Everything looks good - update the tuple */
    tp = (HeapTuple) tableam_tops_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    simple_heap_update(rel, &tp->t_self, tp);
    CatalogUpdateIndexes(rel, tp);

    heap_freetuple(tp);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Drop user mapping
 */
void RemoveUserMapping(DropUserMappingStmt* stmt)
{
    ObjectAddress object;
    Oid useId;
    Oid umId;
    ForeignServer* srv = NULL;

    useId = GetUserOidFromMapping(stmt->username, stmt->missing_ok);
    srv = GetForeignServerByName(stmt->servername, true);

    if (stmt->username && !OidIsValid(useId)) {
        /*
         * IF EXISTS specified, role not found and not public. Notice this and
         * leave.
         */
        ereport(NOTICE,
            (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                errmsg("role \"%s\" does not exist, skipping", stmt->username)));
        return;
    }

    if (srv == NULL) {
        if (!stmt->missing_ok)
            ereport(
                ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("server \"%s\" does not exist", stmt->servername)));
        /* IF EXISTS, just note it */
        ereport(NOTICE, (errcode(ERRCODE_LOG), (errmsg("server does not exist, skipping"))));
        return;
    }

    umId = GetSysCacheOid2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(useId), ObjectIdGetDatum(srv->serverid));

    if (!OidIsValid(umId)) {
        if (!stmt->missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("user mapping \"%s\" does not exist for the server", MappingUserName(useId))));

        /* IF EXISTS specified, just note it */
        ereport(
            NOTICE, (errmsg("user mapping \"%s\" does not exist for the server, skipping", MappingUserName(useId))));
        return;
    }

    user_mapping_ddl_aclcheck(useId, srv->serverid, srv->servername);

    /*
     * Do the deletion
     */
    object.classId = UserMappingRelationId;
    object.objectId = umId;
    object.objectSubId = 0;

    performDeletion(&object, DROP_CASCADE, 0);
}

/*
 * Drop user mapping by OID.  This is called to clean up dependencies.
 */
void RemoveUserMappingById(Oid umId)
{
    HeapTuple tp;
    Relation rel;

    rel = heap_open(UserMappingRelationId, RowExclusiveLock);

    tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(umId));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for user mapping %u", umId)));

    simple_heap_delete(rel, &tp->t_self);

    ReleaseSysCache(tp);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Make ColumnDef
 */
ColumnDef* makeColumnDef(const char* colname, char* coltype)
{
    ColumnDef* col = makeNode(ColumnDef);
    col->colname = pstrdup(colname);
    col->typname = SystemTypeName(coltype);
    col->kvtype = 0;
    col->generatedCol = '\0';
    col->inhcount = 0;
    col->is_local = true;
    col->is_not_null = false;
    col->is_from_type = false;
    col->storage = 0;
    /* foreign table don't provide with compress feature. so don't worry about column comrpession mode. */
    col->cmprs_mode = ATT_CMPR_UNDEFINED;
    col->raw_default = NULL;
    col->cooked_default = NULL;
    col->collClause = NULL;
    col->collOid = InvalidOid;
    col->constraints = NIL;
    col->fdwoptions = NIL;
    return col;
}

// if *remove* is false and the option name is found in *optList*,
//   we will set output parameter *found* and return the old list.
// if *remove* is true, we will delete the matched option, and
//   then return the new option list.
List* FindOrRemoveForeignTableOption(List* optList, const char* optName, bool remove, bool* found)
{
    ListCell* lcell = NULL;
    DefElem* opt = NULL;
    *found = false;

    foreach (lcell, optList) {
        opt = (DefElem*)lfirst(lcell);
        if (0 == strncmp(opt->defname, optName, strlen(optName))) {
            *found = true;
            break;
        }
    }

    if (remove && *found) {
        optList = list_delete_ptr(optList, opt);
        pfree_ext(opt);
    }

    return optList;
}

void getErrorTableFilePath(char* buf, int len, Oid databaseid, Oid reid)
{
    int rc;
    rc = snprintf_s(buf, len, len - 1, "./pg_errorinfo/%u.%u", databaseid, reid);
    securec_check_ss(rc, "", "");
    buf[len - 1] = '\0';
}

#define OptInternalMask "internal_mask"

#ifdef ENABLE_MOT
void CreateForeignIndex(IndexStmt* stmt, Oid indexRelationId)
{
    Relation ftrel, rel;
    Relation indrel;
    Oid ownerId;
    ForeignDataWrapper* fdw = NULL;
    ForeignServer* server = NULL;
    AclResult aclresult;

    /*
     * Advance command counter to ensure the pg_attribute tuple is visible;
     * the tuple might be updated to add constraints in previous step.
     */
    CommandCounterIncrement();

    ftrel = heap_open(ForeignTableRelationId, RowExclusiveLock);
    indrel = heap_open(IndexRelationId, AccessShareLock);
    rel = HeapOpenrvExtended(stmt->relation, (stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock), false, true);

    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE && isMOTFromTblOid(RelationGetRelid(rel))) {
        Oid relationId = RelationGetRelid(rel);
        ForeignTable* ftbl = GetForeignTable(relationId);

        /*
         * For now the owner cannot be specified on create. Use effective user ID.
         */
        ownerId = GetUserId();

        /*
         * Check that the foreign server exists and that we have USAGE on it. Also
         * get the actual FDW for option validation etc.
         */
        server = GetForeignServer(ftbl->serverid);
        aclresult = pg_foreign_server_aclcheck(server->serverid, ownerId, ACL_USAGE);
        if (strcmp(server->servername, "gsmpp_server") != 0 &&
                strcmp(server->servername, "gsmpp_errorinfo_server") != 0 && aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, server->servername);

        fdw = GetForeignDataWrapper(server->fdwid);

        FdwRoutine* fdwroutine = GetFdwRoutine(fdw->fdwhandler);
        if (fdwroutine->ValidateTableDef != NULL) {
            Oid prevIndxId = stmt->indexOid;
            stmt->relation->foreignOid = relationId;
            stmt->indexOid = indexRelationId;
            AssertEreport(stmt->idxname != NULL, MOD_MOT, "Index name cannot be NULL in the stmt for foreign index");

            fdwroutine->ValidateTableDef((Node*)stmt);

            stmt->indexOid = prevIndxId;
            index_update_stats(rel, true, stmt->primary, InvalidOid, InvalidOid, -1);
            SetCurrentTransactionStorageEngine(SE_TYPE_MOT);
        }
    }

    heap_close(rel, NoLock);
    heap_close(indrel, NoLock);
    heap_close(ftrel, NoLock);
}
#endif

/*
 * @Description: Check if current user has useft privilege, and the useft privileges
 * is in the pg_authid.
 * @Brief: Before a user wants to create, alter, insert, select or drop a foreign
 * table, the useft privilege of user must be checked first.
 * @Return: whether the current user has useft privilege.
 */
bool have_useft_privilege(void)
{
    /* Superusers can always do everything */
    if (superuser()) {
        return true;
    }

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
    Datum datum = BoolGetDatum(false); /* default value is false when tuple is invalid */

    if (HeapTupleIsValid(tuple)) {
        bool isNull = true;
        datum = heap_getattr(tuple, Anum_pg_authid_roluseft, RelationGetDescr(relation), &isNull);
        Assert(!isNull);
        ReleaseSysCache(tuple);
    }
    heap_close(relation, AccessShareLock);

    return BoolGetDatum(datum);
}

void encryptKeyString(char* keyStr, char destplainStr[], uint32 destplainLength)
{
#ifndef MIN
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#endif

    Assert(keyStr != NULL);

    errno_t rc = EOK;
    /*
     * If prefix of keyStr is "encryptstr" and length >= 54bytes, we do not encrypt it.
     * It has been an encrypt string. retrun it.
     */
    if (isEncryptedPassword(keyStr)) {
        rc = memcpy_s(destplainStr, destplainLength, keyStr, MIN((strlen(keyStr) + 1), destplainLength));
        securec_check(rc, "\0", "\0");

        ereport(NOTICE,
            (errcode(ERRCODE_INVALID_PASSWORD),
                errmsg("The input text will be treated as encrypted password, "
                       "and it is not recommended that \"%s\" is as prefix for password",
                    ENCRYPT_STR_PREFIX),
                errhint("It's possible that the foreign server don't work normally.")));
    } else {
        /*
         * use cipher key to decrypt option and store
         * results into encryptAccessKeyStr/encryptSecretAccessKeyStr
         */
        /*
         * Fill the encryptstr string as prefix of destplainStr.
         */
        rc = memcpy_s(destplainStr, destplainLength, ENCRYPT_STR_PREFIX, strlen(ENCRYPT_STR_PREFIX));
        securec_check(rc, "\0", "\0");

        encryptOBS(keyStr, destplainStr + strlen(ENCRYPT_STR_PREFIX), destplainLength - strlen(ENCRYPT_STR_PREFIX));
    }
}

/*
 * @Description: Encrpyt access key and security access key in options before insert
 * tuple into pg_foreign_table when creating foreign tables in obs protocol.
 * @Input: pointer to options list
 */
void encryptOBSForeignTableOption(List** options)
{
    char* keyStr = NULL;

    /* The maximum string length of the encrypt access key or encrypt access key is 1024*/
    char encryptSecretAccessKeyStr[DEST_CIPHER_LENGTH] = {'\0'};
    char encryptPasswordStr[DEST_CIPHER_LENGTH] = {'\0'};

    bool haveSecretAccessKey = false;
    bool havePassWord = false;

    ListCell* lc = NULL;
    ListCell* prev = NULL;
    errno_t rc = EOK;
    foreach (lc, *options) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (0 == pg_strcasecmp(def->defname, optSecretAccessKey)) {
            haveSecretAccessKey = true;

            keyStr = defGetString(def);
            encryptKeyString(keyStr, encryptSecretAccessKeyStr, DEST_CIPHER_LENGTH);

            *options = list_delete_cell(*options, lc, prev);
            break;
        }
        if (0 == pg_strcasecmp(def->defname, OPTION_NAME_PASSWD)) {
            havePassWord = true;
            keyStr = defGetString(def);
            encryptKeyString(keyStr, encryptPasswordStr, DEST_CIPHER_LENGTH);

            *options = list_delete_cell(*options, lc, prev);
            break;
        }
        prev = lc;
    }

    if (haveSecretAccessKey) {
        *options = lappend(
            *options, makeDefElem(pstrdup(optSecretAccessKey), (Node*)makeString(pstrdup(encryptSecretAccessKeyStr))));
    }

    if (havePassWord) {
        *options =
            lappend(*options, makeDefElem(pstrdup(OPTION_NAME_PASSWD), (Node*)makeString(pstrdup(encryptPasswordStr))));
    }

    if (keyStr != NULL) {
        /* safty concern, empty keyStr manaully. */
        size_t keyStrLen = strlen(keyStr);
        rc = memset_s(keyStr, keyStrLen, '\0', keyStrLen);
        securec_check(rc, "", "");
        pfree_ext(keyStr);
    }

    /* safty concern, empty encryptAccessKeyStr & encryptSecretAccessKeyStr */
    rc = memset_s(encryptSecretAccessKeyStr, DEST_CIPHER_LENGTH, '\0', DEST_CIPHER_LENGTH);
    securec_check(rc, "", "");
    rc = memset_s(encryptPasswordStr, DEST_CIPHER_LENGTH, '\0', DEST_CIPHER_LENGTH);
    securec_check(rc, "", "");
}

/*
 * Create a foreign table
 * call after DefineRelation().
 */
void CreateForeignTable(CreateForeignTableStmt* stmt, Oid relid)
{
    Relation ftrel;
    Datum ftoptions;
    Datum values[Natts_pg_foreign_table];
    bool nulls[Natts_pg_foreign_table];
    HeapTuple tuple;
    AclResult aclresult;
    ObjectAddress myself;
    ObjectAddress referenced;
    Oid ownerId;
    ForeignDataWrapper* fdw = NULL;
    ForeignServer* server = NULL;
    Oid errortableOid = InvalidOid;
    List* resultOptions = NIL;
    bool found = false;
    char* total_rows = NULL;
    double num_rows = 0.0;
    errno_t ret = EOK;

    /*
     * In the security mode, the useft privilege of a user must be
     * checked before the user creates a foreign table.
     */
    if (isSecurityMode && !have_useft_privilege()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create foreign table in security mode")));
    }

    /*
     * Advance command counter to ensure the pg_attribute tuple is visible;
     * the tuple might be updated to add constraints in previous step.
     */
    CommandCounterIncrement();

    ftrel = heap_open(ForeignTableRelationId, RowExclusiveLock);

    /*
     * For now the owner cannot be specified on create. Use effective user ID.
     */
    ownerId = GetUserId();

    /*
     * Check that the foreign server exists and that we have USAGE on it. Also
     * get the actual FDW for option validation etc.
     */
    server = GetForeignServerByName(stmt->servername, false);
    aclresult = pg_foreign_server_aclcheck(server->serverid, ownerId, ACL_USAGE);
    if (strcmp(server->servername, "gsmpp_server") != 0 && strcmp(server->servername, "gsmpp_errorinfo_server") != 0 &&
        aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, server->servername);

    if (isDummyServerByOptions(server->options)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_HDFS),
                errmsg("Failed to create foreign table \"%s\".", ((CreateStmt*)stmt)->relation->relname),
                errdetail("It is not allowed to create foreign table with dummy server.")));
    }

    fdw = GetForeignDataWrapper(server->fdwid);

    /* @hdfs
     * Check column data type and distribute by clause.
     */
    FdwRoutine* fdwroutine = GetFdwRoutine(fdw->fdwhandler);
    if (NULL != fdwroutine->ValidateTableDef) {
#ifdef ENABLE_MOT
        stmt->base.relation->foreignOid = relid;
#endif
        fdwroutine->ValidateTableDef((Node*)stmt);
    }

#ifdef ENABLE_MOT
    if (isMOTTableFromSrvName(stmt->servername)) {
        SetCurrentTransactionStorageEngine(SE_TYPE_MOT);
    }
#endif

    stmt->options = regularizeObsLocationInfo(stmt->options);

    /*
     * Insert tuple into pg_foreign_table.
     */
    ret = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ret, "", "");
    ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ret, "", "");

    values[Anum_pg_foreign_table_ftrelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_foreign_table_ftserver - 1] = ObjectIdGetDatum(server->serverid);
#ifdef ENABLE_MOT
    if (isMOTTableFromSrvName(stmt->servername)) {
        values[Anum_pg_foreign_table_ftwriteonly - 1] = true;
    } else {
#endif
        values[Anum_pg_foreign_table_ftwriteonly - 1] = BoolGetDatum(stmt->write_only);
#ifdef ENABLE_MOT
    }
#endif

    /*
     * we insert type information into option in order to distinguish server type
     * hdfs_fdw_validator function.
     */
    char* optValue = getServerOptionValue(server->serverid, "type");
    DefElem* defElem = NULL;
    if (NULL != optValue) {
        defElem = makeDefElem("type", (Node*)makeString(optValue));
        stmt->options = lappend(stmt->options, defElem);
    }

    /* Add table generic options */
    ftoptions =
        transformGenericOptions(ForeignTableRelationId, PointerGetDatum(NULL), stmt->options, fdw->fdwvalidator);

    // after check location option, remove updatable option for both stmt->options and ftoptions.
    //
    resultOptions = untransformRelOptions(ftoptions);

    // get Foreign Table totalrows Option values
    //
    total_rows = getFTOptionValue(resultOptions, "totalrows");
    if (total_rows != NULL)
        num_rows = convertFTOptionValue(total_rows);

    if (NULL != optValue) {
        stmt->options = list_delete(stmt->options, defElem);
        resultOptions = list_delete(resultOptions, defElem);
        pfree_ext(defElem);
    }
    resultOptions = FindOrRemoveForeignTableOption(resultOptions, "write_only", true, &found);
    ereport(
        DEBUG5, (errcode(ERRCODE_DEBUG), errmsg("The flag of write only property of the foreign table is %d", found)));

    // Add encrpyt function for obs access key and security access key in obs options
    encryptOBSForeignTableOption(&resultOptions);
    ftoptions = optionListToArray(resultOptions);

    if (PointerIsValid(DatumGetPointer(ftoptions)))
        values[Anum_pg_foreign_table_ftoptions - 1] = ftoptions;
    else
        nulls[Anum_pg_foreign_table_ftoptions - 1] = true;

    tuple = heap_form_tuple(ftrel->rd_att, values, nulls);

    (void)simple_heap_insert(ftrel, tuple);
    CatalogUpdateIndexes(ftrel, tuple);

    heap_freetuple(tuple);

    /* create error info table */
    if (stmt->error_relation != NULL && IsA(stmt->error_relation, RangeVar)) {
        RangeVar* error_relation = (RangeVar*)(stmt->error_relation);
        Datum toast_options;
        static const char* const validnsps[] = HEAP_RELOPT_NAMESPACES;
        List* colList = NIL;
        ColumnDef* col = NULL;
        CreateStmt* errorStmt = makeNode(CreateStmt);
        char* schemaname =
            ((CreateStmt*)stmt)->relation->schemaname ? pstrdup(((CreateStmt*)stmt)->relation->schemaname) : NULL;
        if (NULL != error_relation->schemaname) {
            if (schemaname == NULL) {
                List* search_path = fetch_search_path(false);
                if (NIL == search_path) {
                    ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("get search path failed")));
                }
                char* nspname = get_namespace_name(linitial_oid(search_path));
                if (NULL == nspname) {
                    ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("get namespace failed")));
                }
                list_free(search_path);
                schemaname = nspname;
            }
            if (pg_strcasecmp(schemaname, error_relation->schemaname)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_SCHEMA_NAME),
                        errmsg(
                            "invalid schema %s for relation %s", error_relation->schemaname, error_relation->relname)));
            }
        }

        if (u_sess->proc_cxt.IsBinaryUpgrade) {
            u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = u_sess->upg_cxt.binary_upgrade_next_etbl_pg_type_oid;
            u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_array_pg_type_oid;
            u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_type_oid;
            u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_heap_pg_class_oid;
            u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_index_pg_class_oid;
            u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_class_oid;

            u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_heap_pg_class_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_index_pg_class_rfoid;
            u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid =
                u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_class_rfoid;

            u_sess->upg_cxt.binary_upgrade_next_etbl_pg_type_oid = 0;
            u_sess->upg_cxt.binary_upgrade_next_etbl_array_pg_type_oid = 0;
            u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_type_oid = 0;
            u_sess->upg_cxt.binary_upgrade_next_etbl_heap_pg_class_oid = 0;
            u_sess->upg_cxt.binary_upgrade_next_etbl_index_pg_class_oid = 0;
            u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_class_oid = 0;
        }

        errorStmt->relation =
            makeRangeVar(schemaname, pstrdup(error_relation->relname), ((CreateStmt*)stmt)->relation->location);
        col = makeColumnDef("nodeid", "int4");
        colList = lappend(colList, col);
        col = makeColumnDef("begintime", "timestamptz");
        colList = lappend(colList, col);
        col = makeColumnDef("filename", "varchar");
        colList = lappend(colList, col);
        col = makeColumnDef("rownum", "int8");
        colList = lappend(colList, col);
        col = makeColumnDef("rawrecord", "text");
        colList = lappend(colList, col);
        col = makeColumnDef("detail", "text");
        colList = lappend(colList, col);
        errorStmt->tableElts = colList;
        errorStmt->oncommit = ONCOMMIT_NOOP;
        errorStmt->tablespacename =
            ((CreateStmt*)stmt)->tablespacename != NULL ? pstrdup(((CreateStmt*)stmt)->tablespacename) : NULL;
        if (IS_PGXC_COORDINATOR || ((CreateStmt*)stmt)->subcluster != NULL) {
            errorStmt->distributeby = makeNode(DistributeBy);
            errorStmt->distributeby->disttype = DISTTYPE_ROUNDROBIN;
            errorStmt->distributeby->colname = NULL;
        }

        errorStmt->options =
            lappend(errorStmt->options, makeDefElem("orientation", (Node*)makeString(ORIENTATION_ROW)));

        errorStmt->options = lappend(errorStmt->options,
            makeDefElem(pstrdup(OptInternalMask),
                (Node*)makeInteger(
                    INTERNAL_MASK_DALTER | INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE | INTERNAL_MASK_ENABLE)));
        errorStmt->options =
            lappend(errorStmt->options, makeDefElem(OptIgnoreEnableHadoopEnv, (Node*)makeInteger(true)));

        if (((CreateStmt*)stmt)->subcluster != NULL)
            errorStmt->subcluster = ((CreateStmt*)stmt)->subcluster;
        else {
            char* group_name = NULL;

            /*
             * Set the storage group of errot table as same as the
             * foreign table which is always the installation group.
             * Otherwise, pg will use default_storage_group to save
             * error table which could be different from installation group.
             */
            errorStmt->subcluster = makeNode(PGXCSubCluster);

            errorStmt->subcluster->clustertype = SUBCLUSTER_GROUP;

            if (in_logic_cluster())
                group_name = PgxcGroupGetCurrentLogicCluster();
            else
                group_name = pstrdup(PgxcGroupGetInstallationGroup());

            errorStmt->subcluster->members = list_make1(makeString(group_name));
        }
        errortableOid = DefineRelation(errorStmt, RELKIND_RELATION, InvalidOid);

        /*
         * Let AlterTableCreateToastTable decide if this one
         * needs a secondary relation too.
         */
        CommandCounterIncrement();

        /* parse and validate reloptions for the toast table */
        toast_options =
            transformRelOptions((Datum)0, ((CreateStmt*)errorStmt)->options, "toast", validnsps, true, false);
        (void)heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
        AlterTableCreateToastTable(errortableOid, toast_options);
    }

    /* Add pg_class dependency on the server */
    myself.classId = RelationRelationId;
    myself.objectId = relid;
    myself.objectSubId = 0;

    referenced.classId = ForeignServerRelationId;
    referenced.objectId = server->serverid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

#ifdef ENABLE_MOT
    // For external lookup when drop database
    recordDependencyOnDatabase(RelationRelationId, relid, server->serverid, u_sess->proc_cxt.MyDatabaseId);
#endif

    if (errortableOid != InvalidOid) {
        /* Add dependency on error info table */
        referenced.classId = RelationRelationId;
        referenced.objectId = errortableOid;
        referenced.objectSubId = 0;
        recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
    }

    /* @hdfs
     * When we create a hdfs partition foreign table, we need to do some more processes.
     */
    if (NULL != fdwroutine->PartitionTblProcess) {
        fdwroutine->PartitionTblProcess((Node*)stmt, relid, HDFS_CREATE_PARTITIONED_FOREIGNTBL);
        CommandCounterIncrement();
    }

    heap_close(ftrel, RowExclusiveLock);

    // Insert obs foreign table totalrows option into pg_class
    //
    if (IS_PGXC_COORDINATOR && num_rows > 0) {
        updateTotalRows(relid, num_rows);
    }
    list_free_deep(resultOptions);
}
