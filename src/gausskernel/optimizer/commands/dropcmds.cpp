/* -------------------------------------------------------------------------
 *
 * dropcmds.cpp
 *	  handle various "DROP" operations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/dropcmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "gs_policy/gs_policy_masking.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

static void does_not_exist_skipping(ObjectType objtype, List* objname, List* objargs, bool missing_ok);

static bool CheckObjectDropPrivilege(ObjectType removeType, Oid objectId)
{
    AclResult aclresult = ACLCHECK_NO_PRIV;
    switch (removeType) {
        case OBJECT_FUNCTION:
            aclresult = pg_proc_aclcheck(objectId, GetUserId(), ACL_DROP);
            break;
        case OBJECT_PACKAGE:
            aclresult = pg_package_aclcheck(objectId, GetUserId(), ACL_DROP);
            break;
        case OBJECT_SCHEMA:
            aclresult = pg_namespace_aclcheck(objectId, GetUserId(), ACL_DROP);
            break;
        case OBJECT_TYPE:
            aclresult = pg_type_aclcheck(objectId, GetUserId(), ACL_DROP);
            break;
        case OBJECT_FOREIGN_SERVER:
            aclresult = pg_foreign_server_aclcheck(objectId, GetUserId(), ACL_DROP);
            break;
        default:
            break;
    }
    return (aclresult == ACLCHECK_OK) ? true : false;
}

static void DropExtensionInListIsSupported(List* objname)
{
    static const char *supportList[] = {
        "drop",
        "postgis",
        "packages",
#ifndef ENABLE_MULTIPLE_NODES
        "mysql_fdw",
        "oracle_fdw",
        "postgres_fdw",
        "dblink",
        "security_plugin",
        "db_a_parser",
        "db_b_parser",
        "db_c_parser",
        "db_pg_parser",
#endif
    };
    int len = lengthof(supportList);
    const char* name = strVal(linitial(objname));

    for (int i = 0; i < len; i++) {
        if (pg_strcasecmp(name, supportList[i]) == 0) {
            return;
        }
    }

    /* Enable DROP operation of the above objects during inplace upgrade or support_extended_features is true */
    if (!u_sess->attr.attr_common.IsInplaceUpgrade && !g_instance.attr.attr_common.support_extended_features) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXTENSION is not yet supported.")));
    }
}

/*
 * @Description: drop one or more objects.
 *               We don't currently handle all object types here.  Relations, for example,
 *               require special handling, because (for example) indexes have additional
 *               locking requirements.
 *               We look up all the objects first, and then delete them in a single
 *               performMultipleDeletions() call.  This avoids unnecessary DROP RESTRICT
 *               errors if there are dependencies between them.
 * @in stmt : the info of dropstmt.
 * @in is_securityadmin : whether the is a security administrator doing this.
 * @return : nothing.
 */
void RemoveObjects(DropStmt* stmt, bool missing_ok, bool is_securityadmin)
{
    ObjectAddresses* objects = NULL;
    ListCell* cell1 = NULL;
    bool skip_check = false;
    ListCell* cell2 = NULL;
    Oid pkgOid = InvalidOid;
    objects = new_object_addresses();

    foreach (cell1, stmt->objects) {
        ObjectAddress address;
        List* objname = (List*)lfirst(cell1);
        List* objargs = NIL;
        Relation relation = NULL;
        Oid namespaceId;

        if (stmt->arguments) {
            cell2 = (!cell2 ? list_head(stmt->arguments) : lnext(cell2));
            objargs = (List*)lfirst(cell2);
        }

        /* Get an ObjectAddress for the object. */
        address =
            get_object_address(stmt->removeType, objname, objargs, &relation, AccessExclusiveLock, stmt->missing_ok);
        /* Issue NOTICE if supplied object was not found. */
        if (!OidIsValid(address.objectId)) {
            if (u_sess->attr.attr_common.xc_maintenance_mode)
                missing_ok = stmt->missing_ok;
            does_not_exist_skipping(stmt->removeType, objname, objargs, missing_ok);
            continue;
        } else if (stmt->removeType == OBJECT_EXTENSION) {
            DropExtensionInListIsSupported(objname);
        }

        TrForbidAccessRbObject(address.classId, address.objectId);

        /*
         * Although COMMENT ON FUNCTION, SECURITY LABEL ON FUNCTION, etc. are
         * happy to operate on an aggregate as on any other function, we have
         * historically not allowed this for DROP FUNCTION.
         */

        if (stmt->removeType == OBJECT_FUNCTION) {
            Oid funcOid = address.objectId;
            if (IsMaskingFunctionOid(funcOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
                ereport(ERROR,
                    (errmodule(MOD_FUNCTION),
                        errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("function \"%s\" is a masking function, it can not be droped", 
                                NameListToString(objname)),
                                errdetail("cannot drop masking function")));
            }

            HeapTuple tup;
            bool isnull = false;
            tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
            if (tup == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("function not found by oid:%u", address.objectId),
                        errhint("system error")));
            }
            Datum packageOidDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &isnull);
            if (!isnull) {
                Oid pkgFuncOid = DatumGetObjectId(packageOidDatum);
                if (OidIsValid(pkgFuncOid)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("\"%s\" is a function in package", NameListToString(objname)),
                            errhint("Use DROP PACKAGE.")));
                }
            }
            if (!HeapTupleIsValid(tup)) /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));

            if (((Form_pg_proc)GETSTRUCT(tup))->proisagg)
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("\"%s\" is an aggregate function", NameListToString(objname)),
                        errhint("Use DROP AGGREGATE to drop aggregate functions.")));

            CacheInvalidateFunction(funcOid, InvalidOid);
            /* send invalid message for for relation holding replaced function as trigger */
            InvalidRelcacheForTriggerFunction(funcOid, ((Form_pg_proc)GETSTRUCT(tup))->prorettype);
            ReleaseSysCache(tup);
        }

        /* For DROP PACKAGE */
        if (stmt->removeType == OBJECT_PACKAGE) {
            pkgOid = address.objectId;

            HeapTuple oldtup;
            ScanKeyData entry;
            ScanKeyInit(&entry, Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pkgOid));
            Relation pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);
            SysScanDesc scan = systable_beginscan(pg_proc_rel, InvalidOid, false, NULL, 1, &entry);
            /*
             * do we need job status not run, we can change the owner, 
             * now only check if have the permission to change the owner, if after change, the running job may 
             * failed in check permission
             */
            while ((oldtup = systable_getnext(scan)) != NULL) {
                HeapTuple proctup = heap_copytuple(oldtup);
                Oid funcOid = InvalidOid;
                if (HeapTupleIsValid(proctup)) {
                    funcOid = HeapTupleGetOid(proctup);
                    if (!OidIsValid(funcOid)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                                errmodule(MOD_PLSQL),
                                errmsg("cache lookup failed for relid %u", funcOid)));
                    }
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmodule(MOD_PLSQL),
                            errmsg("cache lookup failed for relid %u", funcOid)));
                }
                heap_freetuple(proctup);
            }
            systable_endscan(scan);
            heap_close(pg_proc_rel, RowExclusiveLock);

            HeapTuple tup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgOid));
            if (!HeapTupleIsValid(tup)) /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for package %u", pkgOid)));
            CacheInvalidateFunction(InvalidOid, pkgOid);
            ReleaseSysCache(tup);
        }
        // @Temp Table. myTempNamespace and myTempToastNamespace's owner is
        // bootstrap user, so can not be deleted by ordinary user. to ensuer this two
        // schema be deleted on session quiting, we should bypass acl check when
        // drop my own temp namespace
        if (stmt->removeType == OBJECT_SCHEMA && (address.objectId == u_sess->catalog_cxt.myTempNamespace ||
                                                     address.objectId == u_sess->catalog_cxt.myTempToastNamespace))
            skip_check = true;

        /* Check permissions. */
        if (!skip_check) {
            skip_check = CheckObjectDropPrivilege(stmt->removeType, address.objectId);
        }
        namespaceId = get_object_namespace(&address);
        if ((!is_securityadmin) && (!skip_check) &&
            (!OidIsValid(namespaceId) || !pg_namespace_ownercheck(namespaceId, GetUserId())))
            check_object_ownership(GetUserId(), stmt->removeType, address, objname, objargs, relation);

        /* Release any relcache reference count, but keep lock until commit. */
        if (relation)
            heap_close(relation, NoLock);

        add_exact_object_address(&address, objects);
    }

    /* Here we really delete them. */
    performMultipleDeletions(objects, stmt->behavior, 0);

    free_object_addresses(objects);
}

/*
 * Generate a NOTICE stating that the named object was not found, and is
 * being skipped.  This is only relevant when "IF EXISTS" is used; otherwise,
 * get_object_address() will throw an ERROR.
 */
static void does_not_exist_skipping(ObjectType objtype, List* objname, List* objargs, bool missing_ok)
{
    char* msg = NULL;
    char* name = NULL;
    char* args = NULL;
    StringInfo message = makeStringInfo();

    switch (objtype) {
        case OBJECT_TYPE:
        case OBJECT_DOMAIN:
            msg = gettext_noop("type \"%s\" does not exist");
            name = TypeNameToString(makeTypeNameFromNameList(objname));
            break;
        case OBJECT_COLLATION:
            msg = gettext_noop("collation \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_CONVERSION:
            msg = gettext_noop("conversion \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_SCHEMA:
            msg = gettext_noop("schema \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_TSPARSER:
            msg = gettext_noop("text search parser \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_TSDICTIONARY:
            msg = gettext_noop("text search dictionary \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_TSTEMPLATE:
            msg = gettext_noop("text search template \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_TSCONFIGURATION:
            msg = gettext_noop("text search configuration \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_EXTENSION:
            msg = gettext_noop("extension \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_FUNCTION:
            msg = gettext_noop("function %s(%s) does not exist");
            name = NameListToString(objname);
            args = TypeNameListToString(objargs);
            break;
        case OBJECT_PACKAGE:
        case OBJECT_PACKAGE_BODY:
            msg = gettext_noop("package %s(%s) does not exist");
            name = NameListToString(objname);
            args = TypeNameListToString(objargs);
            break;
        case OBJECT_AGGREGATE:
            /* Given ordered set aggregate with no direct args, aggr_args variable is modified in gram.y.
               So the parse of aggr_args should be changed. See gram.y for detail. */
            objargs = (List*)linitial(objargs);
            
            msg = gettext_noop("aggregate %s(%s) does not exist");
            name = NameListToString(objname);
            args = TypeNameListToString(objargs);
            break;
        case OBJECT_OPERATOR:
            msg = gettext_noop("operator %s does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_LANGUAGE:
            msg = gettext_noop("language \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_CAST:
            msg = gettext_noop("cast from type %s to type %s does not exist");
            name = format_type_be(typenameTypeId(NULL, (TypeName*)linitial(objname)));
            args = format_type_be(typenameTypeId(NULL, (TypeName*)linitial(objargs)));
            break;
        case OBJECT_TRIGGER:
            msg = gettext_noop("trigger \"%s\" for table \"%s\" does not exist");
            name = NameListToString(objname);
            args = NameListToString(list_truncate(list_copy(objname), list_length(objname) - 1));
            break;
        case OBJECT_RULE:
            msg = gettext_noop("rule \"%s\" for relation \"%s\" does not exist");
            name = strVal(llast(objname));
            args = NameListToString(list_truncate(list_copy(objname), list_length(objname) - 1));
            break;
        case OBJECT_FDW:
            msg = gettext_noop("foreign-data wrapper \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_FOREIGN_SERVER:
            msg = gettext_noop("server \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_OPCLASS:
            msg = gettext_noop("operator class \"%s\" does not exist for access method \"%s\"");
            name = NameListToString(objname);
            args = strVal(linitial(objargs));
            break;
        case OBJECT_OPFAMILY:
            msg = gettext_noop("operator family \"%s\" does not exist for access method \"%s\"");
            name = NameListToString(objname);
            args = strVal(linitial(objargs));
            break;
        case OBJECT_DATA_SOURCE:
            msg = gettext_noop("data source \"%s\" does not exist");
            name = NameListToString(objname);
            break;
        case OBJECT_RLSPOLICY:
            msg = gettext_noop("row level security policy \"%s\" for relation \"%s\" does not exist");
            name = pstrdup(strVal(llast(objname)));
            args = NameListToString(list_truncate(list_copy(objname), list_length(objname) - 1));
            break;
        case OBJECT_PUBLICATION:
            msg = gettext_noop("publication \"%s\" does not exist, skipping");
            name = NameListToString(objname);
            break;
        default:
            pfree_ext(message->data);
            pfree_ext(message);
            ereport(
                ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected object type (%d)", (int)objtype)));
            break;
    }

    if (missing_ok) {
        if (args == NULL) {
            appendStringInfo(message, msg, name);
        } else {
            appendStringInfo(message, msg, name, args);
        }

        appendStringInfo(message, ", skipping");

        ereport(NOTICE, (errmsg("%s", message->data)));

        pfree_ext(message->data);
        pfree_ext(message);
        pfree_ext(name);
    } else {
        pfree_ext(message->data);
        pfree_ext(message);

        if (args == NULL)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg(msg, name)));
        else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg(msg, name, args)));
    }
}
