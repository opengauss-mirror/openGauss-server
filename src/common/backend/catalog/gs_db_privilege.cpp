/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * gs_db_privilege.cpp
 *     routines to support manipulation of the gs_db_privilege relation
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/gs_db_privilege.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/indexing.h"
#include "miscadmin.h"
#include "postgres.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

/*
 * In internal format grantees have been turned into Oids.
 */
typedef struct {
    bool is_grant;       /* true = GRANT, false = REVOKE */
    bool admin_opt;      /* with admin option */
    List* privileges;    /* list of DbPriv nodes */
    List* grantees;      /* list of grantees Oids */
} InternalGrantDb;

static void ExecuteGrantDbPriv(InternalGrantDb* stmt, Oid granteeId, DbPriv* priv, Relation dbPrivRel);
static void ExecuteRevokeDbPriv(InternalGrantDb* stmt, Oid granteeId, DbPriv* priv, Relation dbPrivRel);

/*
 * Called to execute the utility commands GRANT and REVOKE ANY privileges
 */
void ExecuteGrantDbStmt(GrantDbStmt* stmt)
{
    InternalGrantDb istmt;
    ListCell* granteeCell = NULL;
    ListCell* privCell = NULL;

    /* Turn the GrantDbStmt into the InternalGrantDb form. */
    istmt.is_grant = stmt->is_grant;
    istmt.privileges = stmt->privileges;
    istmt.grantees = NIL;  /* filled below */
    istmt.admin_opt = stmt->admin_opt;

    /*
     * Grant ANY privileges to public operation and
     * revoke ANY privileges from public operation are forbidden.
     */
    foreach (granteeCell, stmt->grantees) {
        PrivGrantee* grantee = (PrivGrantee*)lfirst(granteeCell);
        if (grantee->rolname == NULL) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("Invalid grant or revoke operation."),
                    errdetail("Forbid to grant ANY privileges to PUBLIC or revoke ANY privileges from PUBLIC."),
                        errcause("Forbid to grant ANY privileges to PUBLIC or revoke ANY privileges from PUBLIC."),
                            erraction("Don't grant ANY privileges to PUBLIC or revoke ANY privileges from PUBLIC.")));
        } else {
            istmt.grantees= lappend_oid(istmt.grantees, get_role_oid(grantee->rolname, false));
        }
    }

    Relation dbPrivRel = heap_open(DbPrivilegeId, RowExclusiveLock);
    if (!RelationIsValid(dbPrivRel)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Could not open the relation gs_db_privilege."),
                errcause("System error."), erraction("Contact engineer to support.")));
    }

    foreach (granteeCell, istmt.grantees) {
        Oid granteeId = lfirst_oid(granteeCell);

        foreach (privCell, stmt->privileges) {
            DbPriv* priv = (DbPriv*)lfirst(privCell);

            if (stmt->is_grant) {
                /* if stmt->is_grant = true, it means GRANT option */
                ExecuteGrantDbPriv(&istmt, granteeId, priv, dbPrivRel);
            } else {
                /* if stmt->is_grant = false, it means REVOKE option */
                ExecuteRevokeDbPriv(&istmt, granteeId, priv, dbPrivRel);
            }
        }
    }

    /* Close gs_db_privilege. */
    heap_close(dbPrivRel, RowExclusiveLock);
}

/*
 * Internal entry point for granting ANY privileges.
 */
void ExecuteGrantDbPriv(InternalGrantDb* stmt, Oid granteeId, DbPriv* priv, Relation dbPrivRel)
{
    /* Permission check. */
    if (!HasSpecAnyPriv(GetUserId(), priv->db_priv_name, true)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Permission denied."), errdetail("Permission denied to grant %s.", priv->db_priv_name),
                errcause("Insufficient privileges."), erraction("Change to the user with sufficient privileges.")));
    }

    HeapTuple dbPrivTuple = SearchSysCache2(
        DBPRIVROLEPRIV, ObjectIdGetDatum(granteeId), CStringGetTextDatum(priv->db_priv_name));
    TupleDesc dbPrivDsc = RelationGetDescr(dbPrivRel);

    /* Build a tuple to insert or update */
    Datum newRecord[Natts_gs_db_privilege] = {0};
    bool newRecordNulls[Natts_gs_db_privilege] = {false};
    bool newRecordRepl[Natts_gs_db_privilege] = {false};

    newRecord[Anum_gs_db_privilege_roleid - 1] = ObjectIdGetDatum(granteeId);
    newRecord[Anum_gs_db_privilege_privilege_type - 1] = CStringGetTextDatum(priv->db_priv_name);
    newRecord[Anum_gs_db_privilege_admin_option - 1] = BoolGetDatum(stmt->admin_opt);

    bool isNull = false;
    HeapTuple tuple = NULL;
    if (HeapTupleIsValid(dbPrivTuple)) {
        /* If entry for this user' privilge already exists, just skip unless we are adding admin option. */
        Datum datum = heap_getattr(dbPrivTuple, Anum_gs_db_privilege_admin_option, dbPrivDsc, &isNull);
        if (!stmt->admin_opt || DatumGetBool(datum)) {
            ReleaseSysCache(dbPrivTuple);
            return;
        }

        /* Update the record in the gs_db_privilege table */
        newRecordRepl[Anum_gs_db_privilege_admin_option - 1] = true;
        tuple = (HeapTuple)tableam_tops_modify_tuple(dbPrivTuple, dbPrivDsc, newRecord, newRecordNulls, newRecordRepl);
        simple_heap_update(dbPrivRel, &tuple->t_self, tuple);
        CatalogUpdateIndexes(dbPrivRel, tuple);
        ReleaseSysCache(dbPrivTuple);
    } else {
        /* Insert new record to the gs_db_privilege relation */
        tuple = heap_form_tuple(dbPrivDsc, newRecord, newRecordNulls);
        Oid newRowId = simple_heap_insert(dbPrivRel, tuple);
        CatalogUpdateIndexes(dbPrivRel, tuple);

        /* Add shared dependency on users in pg_shdepend */
        ObjectAddress object;
        object.classId = DbPrivilegeId;
        object.objectId = newRowId;
        object.objectSubId = 0;

        ObjectAddress referenced;
        referenced.classId = AuthIdRelationId;
        referenced.objectId = granteeId;
        referenced.objectSubId = 0;
    
        recordSharedDependencyOn(&object, &referenced, SHARED_DEPENDENCY_DBPRIV);
    }
}

/*
 * Internal entry point for revoking ANY privileges.
 */
void ExecuteRevokeDbPriv(InternalGrantDb* stmt, Oid granteeId, DbPriv* priv, Relation dbPrivRel)
{
    /* Permission check. */
    if (!HasSpecAnyPriv(GetUserId(), priv->db_priv_name, true)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Permission denied."), errdetail("Permission denied to revoke %s.", priv->db_priv_name),
                errcause("Insufficient privileges."), erraction("Change to the user with sufficient privileges.")));
    }

    HeapTuple dbPrivTuple = SearchSysCache2(
        DBPRIVROLEPRIV, ObjectIdGetDatum(granteeId), CStringGetTextDatum(priv->db_priv_name));
    TupleDesc dbPrivDsc = RelationGetDescr(dbPrivRel);

    /* Check if entry for this user' privilge exists, if not, just skip. */
    if (!HeapTupleIsValid(dbPrivTuple)) {
        return;
    }

    HeapTuple tuple = NULL;
    if (!stmt->admin_opt) {
        /* Remove the shared dependency */
        deleteSharedDependencyRecordsFor(DbPrivilegeId, HeapTupleGetOid(dbPrivTuple), 0);
        /* Remove the entry altogether */
        simple_heap_delete(dbPrivRel, &dbPrivTuple->t_self);
    } else {
        /* Just turn off the admin option */
        Datum newRecord[Natts_gs_db_privilege] = {0};
        bool newRecordNulls[Natts_gs_db_privilege] = {false};
        bool newRecordRepl[Natts_gs_db_privilege] = {false};

        newRecord[Anum_gs_db_privilege_admin_option - 1] = BoolGetDatum(false);
        newRecordRepl[Anum_gs_db_privilege_admin_option - 1] = true;

        tuple = (HeapTuple)tableam_tops_modify_tuple(dbPrivTuple, dbPrivDsc, newRecord, newRecordNulls, newRecordRepl);
        simple_heap_update(dbPrivRel, &tuple->t_self, tuple);
        CatalogUpdateIndexes(dbPrivRel, tuple);
    }
    ReleaseSysCache(dbPrivTuple);
}

/*
 * The role may have the specific privlege explicitly
 * or may be a member of roles that have the privlege.
 *
 * roleId: the role to be checked
 * priv: the ANY privilege to be checked
 * isAdminOption: if true, return if the role has privilge to GRANT/REVOKE the ANY privilege
 *          if false, just return if the role has the ANY privilege
 */
bool HasSpecAnyPriv(Oid roleId, const char* priv, bool isAdminOption)
{
    if (superuser_arg(roleId)) {
        return true;
    }

    HeapTuple dbPrivTuple = NULL;
    List* roles_list = NIL;
    ListCell* cell = NULL;

    /*
     * We have to do a careful search to see
     * if roleId has the privileges of any suitable role.
     */
    roles_list = roles_has_privs_of(roleId);
    foreach (cell, roles_list) {
        Oid otherid = lfirst_oid(cell);
        dbPrivTuple = SearchSysCache2(DBPRIVROLEPRIV, ObjectIdGetDatum(otherid), CStringGetTextDatum(priv));
        if (!HeapTupleIsValid(dbPrivTuple)) {
            continue;
        }

        if (!isAdminOption) {
            ReleaseSysCache(dbPrivTuple);
            return true;
        }
        
        bool isNull = false;
        Datum datum = SysCacheGetAttr(DBPRIVROLEPRIV, dbPrivTuple, Anum_gs_db_privilege_admin_option, &isNull);
        if (DatumGetBool(datum)) {
            ReleaseSysCache(dbPrivTuple);
            return true;
        }
        ReleaseSysCache(dbPrivTuple);
    }
    return false;
}

/*
 * The role may have at least one of ANY privlege explicitly
 * or may be a member of roles that have at least one of ANY privlege.
 *
 * roleId: the role to be checked
 */
bool HasOneOfAnyPriv(Oid roleId)
{
    bool result = false;
    HeapTuple dbPrivTuple = NULL;
    ListCell* cell = NULL;

    if (superuser_arg(roleId)) {
        return true;
    }

    /*
     * We have to do a careful search to see
     * if roleId has the privileges of any suitable role.
     */
    List* roles_list = roles_has_privs_of(roleId);
    foreach (cell, roles_list) {
        Oid otherid = lfirst_oid(cell);
        dbPrivTuple = SearchSysCache1(DBPRIVROLE, ObjectIdGetDatum(otherid));
        if (HeapTupleIsValid(dbPrivTuple)) {
            result = true;
            ReleaseSysCache(dbPrivTuple);
            break;
        }
    }
    return result;
}

typedef struct {
    const char* name;
    const char* priv;
    bool adminOption;
} DbPrivMap;

static const DbPrivMap dbPrivMap[] = {{"CREATE ANY TABLE", "create any table", false},
    {"CREATE ANY TABLE WITH ADMIN OPTION", "create any table", true},
    {"ALTER ANY TABLE", "alter any table", false}, {"ALTER ANY TABLE WITH ADMIN OPTION", "alter any table", true},
    {"DROP ANY TABLE", "drop any table", false}, {"DROP ANY TABLE WITH ADMIN OPTION", "drop any table", true},
    {"SELECT ANY TABLE", "select any table", false}, {"SELECT ANY TABLE WITH ADMIN OPTION", "select any table", true},
    {"INSERT ANY TABLE", "insert any table", false}, {"INSERT ANY TABLE WITH ADMIN OPTION", "insert any table", true},
    {"UPDATE ANY TABLE", "update any table", false}, {"UPDATE ANY TABLE WITH ADMIN OPTION", "update any table", true},
    {"DELETE ANY TABLE", "delete any table", false}, {"DELETE ANY TABLE WITH ADMIN OPTION", "delete any table", true},
    {"CREATE ANY SEQUENCE", "create any sequence", false},
    {"CREATE ANY SEQUENCE WITH ADMIN OPTION", "create any sequence", true},
    {"CREATE ANY INDEX", "create any index", false},
    {"CREATE ANY INDEX WITH ADMIN OPTION", "create any index", true},
    {"CREATE ANY FUNCTION", "create any function", false},
    {"CREATE ANY FUNCTION WITH ADMIN OPTION", "create any function", true},
    {"EXECUTE ANY FUNCTION", "execute any function", false},
    {"EXECUTE ANY FUNCTION WITH ADMIN OPTION", "execute any function", true},
    {"CREATE ANY PACKAGE", "create any package", false},
    {"CREATE ANY PACKAGE WITH ADMIN OPTION", "create any package", true},
    {"EXECUTE ANY PACKAGE", "execute any package", false},
    {"EXECUTE ANY PACKAGE WITH ADMIN OPTION", "execute any package", true},
    {"CREATE ANY TYPE", "create any type", false}, {"CREATE ANY TYPE WITH ADMIN OPTION", "create any type", true},
    {NULL, NULL, false}};

/*
 * has_any_privilege
 *
 * Check role's ANY privlege.
 *
 * userName: the user to be checked
 * privList: the ANY privilege to be checked
 * isAdminOption: if true, return if the user has privilge to GRANT/REVOKE the ANY privilege
 *          if false, just return if the user has the ANY privilege
 */
Datum has_any_privilege(PG_FUNCTION_ARGS)
{
    Name userName = PG_GETARG_NAME(0);
    text* privText = PG_GETARG_TEXT_P(1);

    Oid roleId = get_role_oid(NameStr(*userName), false);

    char* privList = text_to_cstring(privText);
    bool result = false;
    char* chunk = NULL;
    char* nextChunk = NULL;
    for (chunk = privList; chunk; chunk = nextChunk) {
        /* Split string at commas */
        nextChunk = strchr(chunk, ',');
        if (nextChunk != NULL)
            *nextChunk++ = '\0';

        /* Drop leading/trailing whitespace in this chunk */
        while (*chunk != 0 && isspace((unsigned char)*chunk)) {
            chunk++;
        }
        int chunkLen = strlen(chunk);
        while (chunkLen > 0 && isspace((unsigned char)chunk[chunkLen - 1])) {
            chunkLen--;
        }
        chunk[chunkLen] = '\0';

        /* Match to the privileges list. */
        const DbPrivMap* this_priv = NULL;
        for (this_priv = dbPrivMap; this_priv->name; this_priv++) {
            if (pg_strcasecmp(this_priv->name, chunk) == 0) {
                result = result | HasSpecAnyPriv(roleId, this_priv->priv, this_priv->adminOption);
                break;
            }
        }
        if (this_priv->name == NULL) {
            ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Unrecognized privilege type."), errdetail("Unrecognized privilege type: \"%s\".", chunk),
                    errcause("The privilege type is not supported."),
                        erraction("Check GRANT/REVOKE syntax to obtain the supported privilege types.")));
        }
    }
    PG_RETURN_BOOL(result);
}

/*
 * Delete the record in gs_db_privilege by oid
 */
void DropDbPrivByOid(Oid rowOid)
{
    Relation dbPrivRel = heap_open(DbPrivilegeId, RowExclusiveLock);
    if (!RelationIsValid(dbPrivRel)) {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Could not open the relation gs_db_privilege."),
                errcause("System error."), erraction("Contact engineer to support.")));
    }

    HeapTuple dbPrivTuple = SearchSysCache1(DBPRIVOID, ObjectIdGetDatum(rowOid));
    if (HeapTupleIsValid(dbPrivTuple)) {
        simple_heap_delete(dbPrivRel, &dbPrivTuple->t_self);
        ReleaseSysCache(dbPrivTuple);
    }

    heap_close(dbPrivRel, RowExclusiveLock);
}
