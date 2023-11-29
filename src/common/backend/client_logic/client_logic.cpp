/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * client_logic.cpp
 *
 * IDENTIFICATION
 *	  src\common\backend\client_logic\client_logic.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "pg_config.h"

#include "client_logic/client_logic.h"
#include "client_logic/cstrings_map.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "commands/dbcommands.h"
#include "catalog/dependency.h"
#include "catalog/pg_namespace.h"
#include "client_logic/cache.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/acl.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "storage/lock/lock.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/sec_rls_cmds.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "nodes/makefuncs.h"
#include "commands/tablecmds.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "pgxc/pgxc.h"
#include "utils/fmgroids.h"
#include "funcapi.h"
#include "postgres_ext.h"
#include "catalog/pg_proc.h"

const size_t ENCRYPTED_VALUE_MIN_LENGTH = 116;
const size_t ENCRYPTED_VALUE_MAX_LENGTH = 1024;
static bool get_cmk_name(const Oid global_key_id, NameData &cmk_name);

void delete_client_master_keys(Oid roleid)
{
    Relation relation = heap_open(ClientLogicGlobalSettingsId, AccessShareLock);
    if (relation == NULL) {
        return;
    }
    HeapTuple rtup = NULL;
    Form_gs_client_global_keys rel_data = NULL;
    List *global_keys = NIL;
    ListCell* cell = NULL;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_client_global_keys)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        if (rel_data->key_owner == roleid) {
            Oid global_key_id = HeapTupleGetOid(rtup);
            if (global_key_id != InvalidOid) {
                KeyOidNameMap* key_map = (KeyOidNameMap*)palloc(sizeof(KeyOidNameMap));
                key_map->key_oid = global_key_id;
                key_map->key_name = rel_data->global_key_name;
                global_keys = lappend(global_keys, key_map);
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, AccessShareLock);

    foreach (cell, global_keys) {
        KeyOidNameMap *global_key = (KeyOidNameMap *)lfirst(cell);
        Oid global_key_id = global_key->key_oid;
        ObjectAddress cmk_addr;
        cmk_addr.classId = ClientLogicGlobalSettingsId;
        cmk_addr.objectId = global_key_id;
        cmk_addr.objectSubId = 0;
        ereport(NOTICE, (errmsg("drop cascades to client master key: %s", global_key->key_name.data)));
        performDeletion(&cmk_addr, DROP_CASCADE, 0);
    }
    list_free(global_keys);
}

void delete_column_keys(Oid roleid)
{
    Relation relation = heap_open(ClientLogicColumnSettingsId, AccessShareLock);
    if (relation == NULL) {
        return;
    }
    HeapTuple rtup = NULL;
    Form_gs_column_keys rel_data = NULL;
    List *column_keys = NIL;
    ListCell* cell = NULL;
    TableScanDesc scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while (scan && (rtup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        rel_data = (Form_gs_column_keys)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        if (rel_data->key_owner == roleid) {
            Oid column_key_id = HeapTupleGetOid(rtup);
            if (column_key_id != InvalidOid) {
                column_keys = lappend_oid(column_keys, column_key_id);
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, AccessShareLock);

    foreach (cell, column_keys) {
        Oid column_key_id = lfirst_oid(cell);
        ObjectAddress cek_addr;
        cek_addr.classId = ClientLogicColumnSettingsId;
        cek_addr.objectId = column_key_id;
        cek_addr.objectSubId = 0;
        performDeletion(&cek_addr, DROP_CASCADE, 0);
    }
    list_free(column_keys);
}

static Oid check_namespace(const List *key_name, Node * const stmt, char **keyname)
{
    Oid namespace_id;
    AclResult aclresult;
    namespace_id = QualifiedNameGetCreationNamespace(key_name, keyname);

    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(namespace_id, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespace_id));

        /*
         * @Temp Table. Lock Cluster after determine whether is a temp object,
         * so we can decide if locking other coordinator
         */
#ifdef ENABLE_MULTIPLE_NODES
    pgxc_lock_for_utility_stmt(stmt, namespace_id == u_sess->catalog_cxt.myTempNamespace);
#endif
    return namespace_id;
}
static Oid check_user(const Oid namespace_id)
{
    Oid keyowner = InvalidOid;
    AclResult aclresult;
    bool isalter = false;
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        keyowner = GetUserIdFromNspId(namespace_id, true);
        if (!OidIsValid(keyowner))
            keyowner = GetUserId();
        else if (keyowner != GetUserId())
            isalter = true;
        if (isalter) {
            aclresult = pg_namespace_aclcheck(namespace_id, keyowner, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespace_id));
        }
    } else {
        keyowner = GetUserId();
    }
    return keyowner;
}

Oid finish_processing(Relation *rel, HeapTuple *tup)
{
    Oid res = simple_heap_insert(*rel, *tup);
    CatalogUpdateIndexes(*rel, *tup);
    heap_freetuple(*tup);
    heap_close(*rel, RowExclusiveLock);
    return res;
}

Oid get_column_key_id_by_namespace(const char *key_name, const Oid namespace_id, Oid &global_key_id)
{
    HeapTuple rtup = NULL;
    Form_gs_column_keys rel_data = NULL;
    Oid column_key_id = InvalidOid;
    rtup = SearchSysCache2(COLUMNSETTINGNAME, PointerGetDatum(key_name), ObjectIdGetDatum(namespace_id));
    if (rtup != NULL) {
        rel_data = (Form_gs_column_keys)GETSTRUCT(rtup);
        if (rel_data != NULL && namespace_id == rel_data->key_namespace &&
            strcmp(rel_data->column_key_name.data, key_name) == 0) {
            column_key_id = HeapTupleGetOid(rtup);
            global_key_id = rel_data->global_key_id;
            ReleaseSysCache(rtup);
            return column_key_id;
        }
        ReleaseSysCache(rtup);
    }
    return column_key_id;
}

Oid get_column_key_id(const List *keyname_list, const Oid user_id, Oid &global_key_id)
{
    Oid column_key_id = InvalidOid;
    char *keyname = NULL;
    ListCell *l = NULL;
    char *schemaname = NULL;
    Oid namespace_id = InvalidOid;
    DeconstructQualifiedName(keyname_list, &schemaname, &keyname);
    if (schemaname != NULL) {
        namespace_id = SchemaNameGetSchemaOid(schemaname);
        Assert(OidIsValid(namespace_id));
        column_key_id = get_column_key_id_by_namespace(keyname, namespace_id, global_key_id);
        if (column_key_id != InvalidOid) {
            AclResult acl_res = gs_sec_cek_aclcheck(column_key_id, user_id, ACL_USAGE);
            if (acl_res != ACLCHECK_OK) {
                aclcheck_error(acl_res, ACL_KIND_COLUMN_SETTING, NameListToString(keyname_list));
            }
            return column_key_id;
        }
    } else { /* keyname_list doesn't include schema */
        recomputeNamespacePath();
        List *temp_active_search_path = NIL;
        temp_active_search_path = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, temp_active_search_path) {
            namespace_id = lfirst_oid(l);
            column_key_id = get_column_key_id_by_namespace(keyname, namespace_id, global_key_id);
            if (column_key_id != InvalidOid) {
                AclResult acl_res = gs_sec_cek_aclcheck(column_key_id, user_id, ACL_USAGE);
                if (acl_res != ACLCHECK_OK) {
                    aclcheck_error(acl_res, ACL_KIND_COLUMN_SETTING, NameListToString(keyname_list));
                    break;
                }
                list_free_ext(temp_active_search_path);
                return column_key_id;
            }
        }
        list_free_ext(temp_active_search_path);
    }
    return column_key_id;
}

void get_catalog_name(const RangeVar * const rel)
{
    Oid namespace_id;
    Oid existing_relid;
    RangeVar *relation = const_cast<RangeVar *>(rel);
    relation->catalogname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    namespace_id = RangeVarGetAndCheckCreationNamespace(relation, NoLock, &existing_relid, '\0');
    if (relation->schemaname == NULL) {
        relation->schemaname = get_namespace_name(namespace_id);
    }
}

int process_encrypted_columns(const ColumnDef * const def, CeHeapInfo *ce_heap_info)
{
    (void)namestrcpy(&ce_heap_info->column_name, def->colname);
    ce_heap_info->alg_type = static_cast<int>(def->clientLogicColumnRef->columnEncryptionAlgorithmType);
    ce_heap_info->orig_typ = def->clientLogicColumnRef->orig_typname->typeOid;
    ce_heap_info->orig_mod = def->clientLogicColumnRef->orig_typname->typemod;
    Oid global_key_id(0);
    ce_heap_info->cek_id = get_column_key_id(def->clientLogicColumnRef->column_key_name, GetUserId(), global_key_id);
    if (ce_heap_info->cek_id == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("object does not exist. column encryption key: %s",
            NameListToString(def->clientLogicColumnRef->column_key_name))));
        return -1;
    }
    AclResult acl_res = gs_sec_cmk_aclcheck(global_key_id, GetUserId(), ACL_USAGE);
    if (acl_res != ACLCHECK_OK) {
        NameData cmk_name;
        (void)get_cmk_name(global_key_id, cmk_name);
        aclcheck_error(acl_res, ACL_KIND_GLOBAL_SETTING, cmk_name.data);
        return -1;
    }
    return 0;
}
void insert_gs_sec_encrypted_column_tuple(CeHeapInfo *ce_heap_info, Relation rel, const Oid rel_id,
    CatalogIndexState indstate)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
    if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("Un-support to add column encryption key when client encryption is disabled.")));
    }

    HeapTuple htup = NULL;

    bool encrypted_columns_nulls[Natts_gs_encrypted_columns];
    Datum encrypted_columns_values[Natts_gs_encrypted_columns];
    int rc = memset_s(encrypted_columns_values, sizeof(encrypted_columns_values), 0, sizeof(encrypted_columns_values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_columns_nulls, sizeof(encrypted_columns_nulls), false, sizeof(encrypted_columns_nulls));
    securec_check(rc, "\0", "\0");

    encrypted_columns_values[Anum_gs_encrypted_columns_rel_id - 1] = CStringGetDatum(rel_id);
    encrypted_columns_values[Anum_gs_encrypted_columns_column_name - 1] = NameGetDatum(&ce_heap_info->column_name);
    encrypted_columns_values[Anum_gs_encrypted_columns_column_key_id - 1] = ce_heap_info->cek_id;
    encrypted_columns_values[Anum_gs_sec_encrypted_columns_encryption_type - 1] = ce_heap_info->alg_type;
    encrypted_columns_values[Anum_gs_encrypted_columns_data_type_original_oid - 1] = ce_heap_info->orig_typ;
    encrypted_columns_values[Anum_gs_encrypted_columns_data_type_original_mod - 1] = ce_heap_info->orig_mod;
    encrypted_columns_values[Anum_gs_encrypted_columns_create_date - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /* finally insert the new tuple, update the indexes, and clean up */
    htup = heap_form_tuple(RelationGetDescr(rel), encrypted_columns_values, encrypted_columns_nulls);
    const Oid myOid = simple_heap_insert(rel, htup);

    if (indstate != NULL) {
        CatalogIndexInsert(indstate, htup);
    } else {
        CatalogUpdateIndexes(rel, htup);
    }
    heap_freetuple(htup);
    ObjectAddress pg_attr_addr;
    ObjectAddress column_addr;
    ObjectAddress cek_addr;
    pg_attr_addr.classId = RelationRelationId;
    pg_attr_addr.objectId = rel_id;
    pg_attr_addr.objectSubId = ce_heap_info->attnum;
    column_addr.classId = ClientLogicCachedColumnsId;
    column_addr.objectId = myOid;
    column_addr.objectSubId = 0;
    cek_addr.classId = ClientLogicColumnSettingsId;
    cek_addr.objectId = ce_heap_info->cek_id;
    cek_addr.objectSubId = 0;

    recordDependencyOn(&column_addr, &cek_addr, DEPENDENCY_NORMAL);
    recordDependencyOn(&column_addr, &pg_attr_addr, DEPENDENCY_AUTO);

    ce_cache_refresh_type |= 4; /* 4: COLUMNS type */
    pfree_ext(ce_heap_info);
}

static const int MAX_ARGS_SIZE = 1024;
static bool process_global_settings_flush_args(Oid global_key_id, const char *global_function, const char *key,
    size_t keySize, const char *value, size_t valueSize)
{
    char key_str[MAX_ARGS_SIZE] = {0};
    char value_str[MAX_ARGS_SIZE] = {0};
    errno_t rc = EOK;
    rc = strncpy_s(key_str, MAX_ARGS_SIZE, key, keySize);
    securec_check(rc, "\0", "\0");
    key_str[keySize] = '\0';
    rc = strncpy_s(value_str, MAX_ARGS_SIZE, value, valueSize);
    securec_check(rc, "\0", "\0");
    value_str[valueSize] = '\0';

    /* reset catalog variables */
    bool client_master_keys_args_nulls[Natts_gs_client_global_keys_args];
    Datum client_master_keys_args_values[Natts_gs_client_global_keys_args];
    rc = memset_s(client_master_keys_args_values, sizeof(client_master_keys_args_values), 0,
        sizeof(client_master_keys_args_values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(client_master_keys_args_nulls, sizeof(client_master_keys_args_nulls), false,
        sizeof(client_master_keys_args_nulls));
    securec_check(rc, "\0", "\0");

    /* fill catalog variable */
    client_master_keys_args_values[Anum_gs_client_global_keys_args_global_key_id - 1] = ObjectIdGetDatum(global_key_id);
    client_master_keys_args_values[Anum_gs_client_global_keys_args_function_name - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(global_function));
    client_master_keys_args_values[Anum_gs_client_global_keys_args_key - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(key_str));
    client_master_keys_args_values[Anum_gs_client_global_keys_args_value - 1] =
        DirectFunctionCall1(byteain, CStringGetDatum(value_str));

    /* write to catalog table */
    Relation rel = heap_open(ClientLogicGlobalSettingsArgsId, RowExclusiveLock);
    HeapTuple htup = heap_form_tuple(rel->rd_att, client_master_keys_args_values, client_master_keys_args_nulls);
    const Oid args_id = finish_processing(&rel, &htup);
    ObjectAddress cmk_addr;
    ObjectAddress args_addr;
    cmk_addr.classId = ClientLogicGlobalSettingsId;
    cmk_addr.objectId = global_key_id;
    cmk_addr.objectSubId = 0;
    args_addr.classId = ClientLogicGlobalSettingsArgsId;
    args_addr.objectId = args_id;
    args_addr.objectSubId = 0;
    recordDependencyOn(&args_addr, &cmk_addr, DEPENDENCY_INTERNAL);
    return true;
}

static int process_global_settings_args(CreateClientLogicGlobal *parsetree, Oid global_key_id)
{
    /* get arguments */
    const char *global_function(NULL);
    ListCell *key_item(NULL);
    StringArgsVec string_args;
    foreach (key_item, parsetree->global_setting_params) {
        ClientLogicGlobalParam *global_param = static_cast<ClientLogicGlobalParam *> lfirst(key_item);
        switch (global_param->key) {
            case ClientLogicGlobalProperty::CLIENT_GLOBAL_FUNCTION:
                global_function = global_param->value;
                break;
            case ClientLogicGlobalProperty::CMK_KEY_STORE: {
                string_args.set("KEY_STORE", global_param->value);
                break;
            }
            case ClientLogicGlobalProperty::CMK_KEY_PATH: {
                string_args.set("KEY_PATH", global_param->value);
                break;
            }
            case ClientLogicGlobalProperty::CMK_ALGORITHM: {
                string_args.set("ALGORITHM", global_param->value);
                break;
            }
            default:
                break;
        }
    }

    if (global_function == NULL || strlen(global_function) == 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("global function is missing")));
        return 1;
    }

    for (size_t i = 0; i < string_args.Size(); ++i) {
        const char *key = string_args.at(i)->key;
        const char *value = string_args.at(i)->value;
        const size_t valsize = string_args.at(i)->valsize;
        if (!process_global_settings_flush_args(global_key_id, global_function, key, strlen(key), value, valsize)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("failed to flush args")));
            return 1;
        }
    }
    return 0;
}

int process_global_settings(CreateClientLogicGlobal *parsetree)
{
    /* permissions */
    char *keyname = NULL;
    const Oid namespace_id = check_namespace(parsetree->global_key_name, reinterpret_cast<Node *>(parsetree), &keyname);
    const Oid user_id = check_user(namespace_id);
    Acl *key_acl = get_user_default_acl(ACL_OBJECT_GLOBAL_SETTING, user_id, namespace_id);

    /* reset catalog variables */
    Datum client_master_keys_values[Natts_gs_client_global_keys];
    bool client_master_keys_nulls[Natts_gs_client_global_keys];
    errno_t rc = EOK;
    rc = memset_s(client_master_keys_values, sizeof(client_master_keys_values), 0, sizeof(client_master_keys_values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(client_master_keys_nulls, sizeof(client_master_keys_nulls), false, sizeof(client_master_keys_nulls));
    securec_check(rc, "\0", "\0");

    /* fill catalog variable */
    client_master_keys_values[Anum_gs_client_global_keys_global_key_name - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(keyname));
    client_master_keys_values[Anum_gs_client_global_keys_key_namespace - 1] = ObjectIdGetDatum(namespace_id);
    client_master_keys_values[Anum_gs_client_global_keys_key_owner - 1] = ObjectIdGetDatum(user_id);
    if (key_acl != NULL)
        client_master_keys_values[Anum_gs_client_global_keys_key_acl - 1] = PointerGetDatum(key_acl);
    else
        client_master_keys_nulls[Anum_gs_client_global_keys_key_acl - 1] = true;
    client_master_keys_values[Anum_gs_client_global_keys_create_date - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /* write to catalog table */
    Relation rel = heap_open(ClientLogicGlobalSettingsId, RowExclusiveLock);
    HeapTuple htup = heap_form_tuple(rel->rd_att, client_master_keys_values, client_master_keys_nulls);
    /*
     * try to report a nicer error message rather than 'duplicate key'
     */
    MemoryContext old_context = CurrentMemoryContext;	
    Oid global_key_id = InvalidOid;
    PG_TRY();
    {
        /*
         * NOTE: we could get a unique-index failure here, in case someone else is
         * creating the same key name in parallel but hadn't committed yet when
         * we checked for a duplicate name above. in such case we try to emit a
         * nicer error message using try...catch
         */
        global_key_id = finish_processing(&rel, &htup);
    } 
    PG_CATCH();
    {
        ErrorData* edata = NULL;
        (void*)MemoryContextSwitchTo(old_context);
        edata = CopyErrorData();
        if (edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION) {
            FlushErrorState();
            /* the old edata is no longer used */
            FreeErrorData(edata);
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_KEY),
                    errmodule(MOD_SEC_FE),
                    errmsg("client master key \"%s\" already exists", keyname)));
        } else {
            /* we still reporting other error messages */
            ReThrowError(edata);
        }
    }
    PG_END_TRY();

    /* update dependency tables */
    ObjectAddress nsp_addr;
    ObjectAddress cmk_addr;
    nsp_addr.classId = NamespaceRelationId;
    nsp_addr.objectId = namespace_id;
    nsp_addr.objectSubId = 0;
    cmk_addr.classId = ClientLogicGlobalSettingsId;
    cmk_addr.objectId = global_key_id;
    cmk_addr.objectSubId = 0;
    recordDependencyOn(&cmk_addr, &nsp_addr, DEPENDENCY_NORMAL);

    /* write to arguments table */
    (void)process_global_settings_args(parsetree, global_key_id);

    ce_cache_refresh_type |= 1; /* 1: CMK type */

    Oid rlsPolicyId = get_rlspolicy_oid(ClientLogicGlobalSettingsId, "gs_client_global_keys_rls", true);
    if (OidIsValid(rlsPolicyId) == false) {
        CreateRlsPolicyForSystem("pg_catalog", "gs_client_global_keys", "gs_client_global_keys_rls",
            "has_cmk_privilege", "oid", "usage");
        rel = relation_open(ClientLogicGlobalSettingsId, ShareUpdateExclusiveLock);
        ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
        relation_close(rel, ShareUpdateExclusiveLock);
    }
    rlsPolicyId = get_rlspolicy_oid(ClientLogicGlobalSettingsArgsId, "gs_client_global_keys_args_rls", true);
    if (OidIsValid(rlsPolicyId) == false) {
        CreateRlsPolicyForSystem("pg_catalog", "gs_client_global_keys_args", "gs_client_global_keys_args_rls",
            "has_cmk_privilege", "global_key_id", "usage");
        rel = relation_open(ClientLogicGlobalSettingsArgsId, ShareUpdateExclusiveLock);
        ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
        relation_close(rel, ShareUpdateExclusiveLock);
    }

    return 0;
}

static Oid get_cmk_oid(const char *key_name, const Oid namespace_id)
{
    HeapTuple rtup = NULL;
    Form_gs_client_global_keys rel_data = NULL;
    Oid cmk_oid = InvalidOid;
    rtup = SearchSysCache2(GLOBALSETTINGNAME, PointerGetDatum(key_name), ObjectIdGetDatum(namespace_id));
    if (rtup != NULL) {
        rel_data = (Form_gs_client_global_keys)GETSTRUCT(rtup);
        if (rel_data != NULL && namespace_id == rel_data->key_namespace &&
            strcmp(rel_data->global_key_name.data, key_name) == 0) {
            cmk_oid = HeapTupleGetOid(rtup);
            ReleaseSysCache(rtup);
            return cmk_oid;
        }
        ReleaseSysCache(rtup);
    }
    return cmk_oid;
}

static Oid get_cmk_oid(const List *key_name, const Oid user_id)
{
    Oid cmk_oid = InvalidOid;
    char *keyname = NULL;
    ListCell *l = NULL;
    char *schemaname = NULL;
    Oid namespace_id = InvalidOid;
    DeconstructQualifiedName(key_name, &schemaname, &keyname);
    if (schemaname != NULL) {
        namespace_id = SchemaNameGetSchemaOid(schemaname);
        Assert(OidIsValid(namespace_id));
        cmk_oid = get_cmk_oid(keyname, namespace_id);
        if (cmk_oid != InvalidOid) {
            AclResult acl_res = gs_sec_cmk_aclcheck(cmk_oid, user_id, ACL_USAGE);
            if (acl_res != ACLCHECK_OK) {
                aclcheck_error(acl_res, ACL_KIND_GLOBAL_SETTING, NameListToString(key_name));
            }
            return cmk_oid;
        }
    } else { /* key_name doesn't include schema */
        recomputeNamespacePath();
        List *temp_active_search_path = NIL;
        temp_active_search_path = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, temp_active_search_path) {
            namespace_id = lfirst_oid(l);
            cmk_oid = get_cmk_oid(keyname, namespace_id);
            if (cmk_oid != InvalidOid) {
                AclResult acl_res = gs_sec_cmk_aclcheck(cmk_oid, user_id, ACL_USAGE);
                if (acl_res != ACLCHECK_OK) {
                    aclcheck_error(acl_res, ACL_KIND_GLOBAL_SETTING, NameListToString(key_name));
                    break;
                }
                list_free_ext(temp_active_search_path);
                return cmk_oid;
            }
        }
        list_free_ext(temp_active_search_path);
    }
    return cmk_oid;
}

static bool get_cmk_name(const Oid global_key_id, NameData &cmk_name)
{
    HeapTuple rtup = NULL;
    Relation relation = heap_open(ClientLogicGlobalSettingsId, RowExclusiveLock);
    Form_gs_client_global_keys rel_data = NULL;
    TableScanDesc scan = heap_beginscan(relation, SnapshotNow, 0, NULL);
    while ((rtup = heap_getnext(scan, ForwardScanDirection))) {
        if (HeapTupleGetOid(rtup) != global_key_id) {
            continue;
        }
        rel_data = (Form_gs_client_global_keys)GETSTRUCT(rtup);
        if (rel_data == NULL) {
            continue;
        }
        cmk_name = rel_data->global_key_name;
        heap_endscan(scan);
        heap_close(relation, RowExclusiveLock);
        return true;
    }

    heap_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    return false;
}

static bool process_column_settings_flush_args(Oid column_key_id, const char *column_function, const char *key,
    size_t keySize, const char *value, size_t valueSize)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
    if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("Un-support to process column keys when client encryption is disabled.")));
    }
    char key_str[MAX_ARGS_SIZE];
    char value_str[MAX_ARGS_SIZE];
    errno_t rc = EOK;
    rc = strncpy_s(key_str, MAX_ARGS_SIZE, key, keySize);
    securec_check(rc, "\0", "\0");
    key_str[keySize] = '\0';
    rc = strncpy_s(value_str, MAX_ARGS_SIZE, value, valueSize);
    securec_check(rc, "\0", "\0");
    value_str[valueSize] = '\0';

    /* reset catalog variables */
    bool column_settings_args_nulls[Natts_gs_column_keys_args];
    Datum column_settings_args_values[Natts_gs_column_keys_args];
    rc = memset_s(column_settings_args_values, sizeof(column_settings_args_values), 0,
        sizeof(column_settings_args_values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(column_settings_args_nulls, sizeof(column_settings_args_nulls), false,
        sizeof(column_settings_args_nulls));
    securec_check(rc, "\0", "\0");

    /* fill catalog variable */
    column_settings_args_values[Anum_gs_column_keys_args_column_key_id - 1] =
        ObjectIdGetDatum(column_key_id);
    column_settings_args_values[Anum_gs_column_keys_args_function_name - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(column_function));
    column_settings_args_values[Anum_gs_column_keys_args_key - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(key_str));
    column_settings_args_values[Anum_gs_column_keys_args_value - 1] =
        DirectFunctionCall1(byteain, CStringGetDatum(value_str));

    /* write to catalog table */
    Relation rel = heap_open(ClientLogicColumnSettingsArgsId, RowExclusiveLock);
    HeapTuple htup = heap_form_tuple(rel->rd_att, column_settings_args_values, column_settings_args_nulls);
    const Oid cek_args_oid = finish_processing(&rel, &htup);
    if (cek_args_oid == 0) {
        return false;
    }
    ObjectAddress cek_addr;
    ObjectAddress args_addr;
    cek_addr.classId = ClientLogicColumnSettingsId;
    cek_addr.objectId = column_key_id;
    cek_addr.objectSubId = 0;
    args_addr.classId = ClientLogicColumnSettingsArgsId;
    args_addr.objectId = cek_args_oid;
    args_addr.objectSubId = 0;
    recordDependencyOn(&args_addr, &cek_addr, DEPENDENCY_INTERNAL);
    return true;
}

static int process_column_settings_args(CreateClientLogicColumn *parsetree, Oid column_key_id)
{
    /* get arguments */
    const char *column_function(NULL);
    ListCell *key_item(NULL);
    StringArgsVec string_args;
    foreach (key_item, parsetree->column_setting_params) {
        ClientLogicColumnParam *columnParam = static_cast<ClientLogicColumnParam *> lfirst(key_item);
        switch (columnParam->key) {
            case ClientLogicColumnProperty::COLUMN_COLUMN_FUNCTION:
                column_function = columnParam->value;
                break;
            case ClientLogicColumnProperty::CEK_ALGORITHM: {
                ColumnEncryptionAlgorithm  column_encryption_algorithm = 
                    get_cek_algorithm_from_string(columnParam->value);
                if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid algorithm")));
                }
                string_args.set("ALGORITHM", columnParam->value);
                break;
            }
            case ClientLogicColumnProperty::CEK_EXPECTED_VALUE: {
                if (columnParam->value == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid encrypted value")));
                }
                size_t encrypted_value_size = strlen(columnParam->value);
                if (encrypted_value_size < ENCRYPTED_VALUE_MIN_LENGTH || 
                    encrypted_value_size > ENCRYPTED_VALUE_MAX_LENGTH) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid encrypted value")));
                }
                string_args.set("ENCRYPTED_VALUE", columnParam->value);
                break;
            }
            default:
                break;
        }
    }

    if (column_function == NULL || strlen(column_function) == 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("column function is missing")));
        return 1;
    }

    for (size_t i = 0; i < string_args.Size(); ++i) {
        const char *key = string_args.at(i)->key;
        const char *value = string_args.at(i)->value;
        const size_t valsize = string_args.at(i)->valsize;
        if (!process_column_settings_flush_args(column_key_id, column_function, key, strlen(key), value, valsize)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("failed to flush column args")));
            return 1;
        }
    }
    return 0;
}

int process_column_settings(CreateClientLogicColumn *parsetree)
{
    /* permissions */
    char *cek_name = NULL;
    char *schema_name = NULL;
    const Oid namespace_id =
        check_namespace(parsetree->column_key_name, reinterpret_cast<Node *>(parsetree), &cek_name);
    const Oid user_id = check_user(namespace_id);
    Acl *key_acl = get_user_default_acl(ACL_OBJECT_COLUMN_SETTING, user_id, namespace_id);

    schema_name = get_namespace_name(namespace_id);
    /* retrieve client master key id (foreign key) */
    ListCell *key_item(NULL);
    Oid global_key_id(InvalidOid);
    const char *global_key_name = NULL;
    foreach (key_item, parsetree->column_setting_params) {
        ClientLogicColumnParam *kp = static_cast<ClientLogicColumnParam *> lfirst(key_item);
        switch (kp->key) {
            case ClientLogicColumnProperty::CLIENT_GLOBAL_SETTING: {
                global_key_name = NameListToString(kp->qualname);
                global_key_id = get_cmk_oid(kp->qualname, user_id);
                break;
            }
            default:
                break;
        }
    }
    if (global_key_id == InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("object does not exist. client master key: %s", global_key_name)));
        return 1;
    }

    /* validate arguments in hook ? */
    const unsigned char buf_size = 2 * NAMEDATALEN + 1;
    char fqdn[buf_size] = {0};

    errno_t rc = EOK;
    rc = strncpy_s(fqdn, buf_size, schema_name, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    rc = strncat_s(fqdn, buf_size, ".", 1);
    securec_check(rc, "\0", "\0");
    rc = strncat_s(fqdn, buf_size, cek_name, NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    /* reset catalog variables */
    Datum column_settings_values[Natts_gs_column_keys];
    bool column_settings_nulls[Natts_gs_column_keys];
    rc = memset_s(column_settings_values, sizeof(column_settings_values), 0, sizeof(column_settings_values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(column_settings_nulls, sizeof(column_settings_nulls), false, sizeof(column_settings_nulls));
    securec_check(rc, "\0", "\0");

    /* fill catalog variable */
    column_settings_values[Anum_gs_column_keys_column_key_name - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(cek_name));
    Oid distid = hash_any((unsigned char*)fqdn, strlen(fqdn));
    column_settings_values[Anum_gs_column_keys_column_key_distributed_id - 1] = ObjectIdGetDatum(distid);
    column_settings_values[Anum_gs_column_keys_global_key_id - 1] = ObjectIdGetDatum(global_key_id);
    column_settings_values[Anum_gs_column_keys_key_namespace - 1] = ObjectIdGetDatum(namespace_id);
    if (key_acl != NULL)
        column_settings_values[Anum_gs_column_keys_key_acl - 1] = PointerGetDatum(key_acl);
    else
        column_settings_nulls[Anum_gs_column_keys_key_acl - 1] = true;
    column_settings_values[Anum_gs_column_keys_key_owner - 1] = ObjectIdGetDatum(user_id);
    column_settings_values[Anum_gs_column_keys_create_date - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /* write to catalog table */
    Relation rel = heap_open(ClientLogicColumnSettingsId, RowExclusiveLock);
    HeapTuple htup = heap_form_tuple(rel->rd_att, column_settings_values, column_settings_nulls);
    /*
     * try to report a nicer error message rather than 'duplicate key'
     */
    MemoryContext old_context = CurrentMemoryContext;	
    Oid column_key_id = InvalidOid;
    PG_TRY();
    {
        /*
         * NOTE: we could get a unique-index failure here, in case someone else is
         * creating the same key name in parallel but hadn't committed yet when
         * we checked for a duplicate name above. in such case we try to emit a
         * nicer error message using try...catch
         */
        column_key_id = finish_processing(&rel, &htup);
    } 
    PG_CATCH();
    {
        ErrorData* edata = NULL;
        (void*)MemoryContextSwitchTo(old_context);
        edata = CopyErrorData();
        if (edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION) {
            FlushErrorState();
            /* the old edata is no longer used */
            FreeErrorData(edata);
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_KEY),
                    errmodule(MOD_SEC_FE),
                    errmsg("column encryption key \"%s\" already exists", cek_name)));
        } else {
            /* we still reporting other error messages */
            ReThrowError(edata);
        }
    }
    PG_END_TRY();

    /* update dependency table */
    ObjectAddress cmk_addr;
    ObjectAddress cek_addr;
    cmk_addr.classId = ClientLogicGlobalSettingsId;
    cmk_addr.objectId = global_key_id;
    cmk_addr.objectSubId = 0;
    cek_addr.classId = ClientLogicColumnSettingsId;
    cek_addr.objectId = column_key_id;
    cek_addr.objectSubId = 0;
    ObjectAddress nsp_addr;
    nsp_addr.classId = NamespaceRelationId;
    nsp_addr.objectId = namespace_id;
    nsp_addr.objectSubId = 0;
    recordDependencyOn(&cek_addr, &cmk_addr, DEPENDENCY_NORMAL);
    recordDependencyOn(&cek_addr, &nsp_addr, DEPENDENCY_NORMAL);

    /* write to arguments table */
    (void)process_column_settings_args(parsetree, column_key_id);

    ce_cache_refresh_type |= 2; /* 2: CEK type */

    Oid rlsPolicyId = get_rlspolicy_oid(ClientLogicColumnSettingsId, "gs_column_keys_rls", true);
    if (OidIsValid(rlsPolicyId) == false) {
        CreateRlsPolicyForSystem(
            "pg_catalog", "gs_column_keys", "gs_column_keys_rls", "has_cek_privilege", "oid", "usage");
        rel = relation_open(ClientLogicColumnSettingsId, ShareUpdateExclusiveLock);
        ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
        relation_close(rel, ShareUpdateExclusiveLock);
    }
    rlsPolicyId = get_rlspolicy_oid(ClientLogicColumnSettingsArgsId, "gs_column_keys_args_rls", true);
    if (OidIsValid(rlsPolicyId) == false) {
        CreateRlsPolicyForSystem("pg_catalog", "gs_column_keys_args",
            "gs_column_keys_args_rls", "has_cek_privilege", "column_key_id", "usage");
        rel = relation_open(ClientLogicColumnSettingsArgsId, ShareUpdateExclusiveLock);
        ATExecEnableDisableRls(rel, RELATION_RLS_ENABLE, ShareUpdateExclusiveLock);
        relation_close(rel, ShareUpdateExclusiveLock);
    }
    return 0;
}

int drop_global_settings(DropStmt *stmt)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
    if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("Un-support to drop client master key when client encryption is disabled.")));
    }
    
    char *global_key_name = NULL;
    ListCell *cell = NULL;
    foreach (cell, stmt->objects) {
        Oid cmk_oid = InvalidOid;
        Oid namespace_id = InvalidOid;
        char *schema_name = NULL;
        List *namespace_list = NIL;
        ListCell *nslc = NULL;
        DeconstructQualifiedName((List *)lfirst(cell), &schema_name, &global_key_name);
        if (schema_name != NULL) {
            namespace_id = LookupExplicitNamespace(schema_name);
            namespace_list = list_make1_oid(namespace_id);
        } else {
            namespace_list = fetch_search_path(false);
        }

        foreach (nslc, namespace_list) {
            namespace_id = lfirst_oid(nslc);
            cmk_oid = get_cmk_oid(global_key_name, namespace_id);
            if (cmk_oid != InvalidOid) {
                /* Check we have creation rights in target namespace */
                AclResult aclresult = pg_namespace_aclcheck(namespace_id, GetUserId(), ACL_CREATE);
                if (aclresult != ACLCHECK_OK) {
                    aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespace_id));
                }
                break;
            }
        }
        list_free_ext(namespace_list);
        HeapTuple rtup = SearchSysCache1(GLOBALSETTINGOID, ObjectIdGetDatum(cmk_oid));
        if (!rtup) {
            if (!stmt->missing_ok) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY),
                    errmsg("client master key \"%s\" does not exist", global_key_name)));
            } else {
                ereport(NOTICE, (errmsg("client master key \"%s\" does not exist", global_key_name)));
                return 0;
            }
        } else {
            Form_gs_client_global_keys rel_data = NULL;
            rel_data = (Form_gs_client_global_keys)GETSTRUCT(rtup);
            const Oid global_key_id = HeapTupleGetOid(rtup);
            ObjectAddress cmk_addr;
            cmk_addr.classId = ClientLogicGlobalSettingsId;
            cmk_addr.objectId = global_key_id;
            cmk_addr.objectSubId = 0;

            performDeletion(&cmk_addr, stmt->behavior, 0);
        }

        ReleaseSysCache(rtup);
    }
    return 0;
}

int drop_column_settings(DropStmt *stmt)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
    if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("Un-support to drop column encryption key when client encryption is disabled.")));
    }
    char *cek_name = NULL;
    ListCell *cell = NULL;
    foreach (cell, stmt->objects) {
        Oid cek_oid = InvalidOid;
        Oid namespace_id = InvalidOid;
        char *schema_name = NULL;
        List *namespace_list = NIL;
        ListCell *nslc = NULL;
        DeconstructQualifiedName((List *)lfirst(cell), &schema_name, &cek_name);
        if (schema_name != NULL) {
            namespace_id = LookupExplicitNamespace(schema_name);
            namespace_list = list_make1_oid(namespace_id);
        } else {
            namespace_list = fetch_search_path(false);
        }
        Oid cmk_oid = InvalidOid;
        foreach (nslc, namespace_list) {
            namespace_id = lfirst_oid(nslc);
            cek_oid = get_column_key_id_by_namespace(cek_name, namespace_id, cmk_oid);
            if (cek_oid != InvalidOid) {
                /* Check we have creation rights in target namespace */
                AclResult aclresult = pg_namespace_aclcheck(namespace_id, GetUserId(), ACL_CREATE);
                if (aclresult != ACLCHECK_OK) {
                    aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespace_id));
                }
                break;
            }
        }
        list_free_ext(namespace_list);
        HeapTuple rtup = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(cek_oid));
        Form_gs_column_keys rel_data = NULL;
        if (!rtup) {
            if (!stmt->missing_ok)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_KEY), errmsg("column encryption key \"%s\" does not exist", cek_name)));
            else {
                ereport(NOTICE, (errmsg("column encryption key \"%s\" does not exist", cek_name)));
                return 0;
            }
        } else {
            rel_data = (Form_gs_column_keys)GETSTRUCT(rtup);
            if (rel_data->key_namespace == namespace_id && strcmp(rel_data->column_key_name.data, cek_name) == 0) {
                const Oid column_key_id = HeapTupleGetOid(rtup);
                ObjectAddress cek_addr;
                cek_addr.classId = ClientLogicColumnSettingsId;
                cek_addr.objectId = column_key_id;
                cek_addr.objectSubId = 0;

                performDeletion(&cek_addr, stmt->behavior, 0);
            }
            ReleaseSysCache(rtup);
        }
    }
    return 0;
}
void remove_cek_by_id(Oid id)
{
    Relation cek_rel = heap_open(ClientLogicColumnSettingsId, RowExclusiveLock);
    HeapTuple tup = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(id));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for column encryption key")));
    Form_gs_column_keys rel_data = (Form_gs_column_keys)GETSTRUCT(tup);

    AclResult acl_res = gs_sec_cek_aclcheck(id, GetUserId(), ACL_DROP);
    if (acl_res == ACLCHECK_OK || pg_namespace_ownercheck(rel_data->key_namespace, GetUserId()) ||
        GetUserId() == rel_data->key_owner) {
            simple_heap_delete(cek_rel, &tup->t_self);
    } else {
        aclcheck_error(acl_res, ACL_KIND_COLUMN_SETTING, rel_data->column_key_name.data);
    }

    ReleaseSysCache(tup);
    heap_close(cek_rel, RowExclusiveLock);
}
void remove_cmk_by_id(Oid id)
{
    Relation cek_rel = heap_open(ClientLogicGlobalSettingsId, RowExclusiveLock);
    HeapTuple tup = SearchSysCache1(GLOBALSETTINGOID, ObjectIdGetDatum(id));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for client master key")));
    Form_gs_client_global_keys rel_data = (Form_gs_client_global_keys)GETSTRUCT(tup);

    AclResult acl_res = gs_sec_cmk_aclcheck(id, GetUserId(), ACL_DROP);
    if (acl_res == ACLCHECK_OK || pg_namespace_ownercheck(rel_data->key_namespace, GetUserId()) ||
        GetUserId() == rel_data->key_owner) {
            simple_heap_delete(cek_rel, &tup->t_self);
    } else {
        aclcheck_error(acl_res, ACL_KIND_GLOBAL_SETTING, rel_data->global_key_name.data);
    }

    ReleaseSysCache(tup);
    heap_close(cek_rel, RowExclusiveLock);
}
void remove_encrypted_col_by_id(Oid id)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
    if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("Un-support to remove encrypted column when client encryption is disabled.")));
    }
    Relation ce_rel = heap_open(ClientLogicCachedColumnsId, RowExclusiveLock);
    HeapTuple tup = SearchSysCache1(CEOID, ObjectIdGetDatum(id));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for encrypted column %u", id)));
    simple_heap_delete(ce_rel, &tup->t_self);
    ReleaseSysCache(tup);
    heap_close(ce_rel, RowExclusiveLock);
}
void remove_cmk_args_by_id(Oid id)
{
    Relation ce_rel = heap_open(ClientLogicGlobalSettingsArgsId, RowExclusiveLock);
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple tuple;

    ScanKeyInit(&scankey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id));
    scan = systable_beginscan(ce_rel, ClientLogicGlobalSettingsArgsOidIndexId, true, NULL, 1, &scankey);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for cast %u", id)));
    simple_heap_delete(ce_rel, &tuple->t_self);

    systable_endscan(scan);
    heap_close(ce_rel, RowExclusiveLock);
}
void remove_cek_args_by_id(Oid id)
{
    Relation ce_rel = heap_open(ClientLogicColumnSettingsArgsId, RowExclusiveLock);
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple tuple;

    ScanKeyInit(&scankey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(id));
    scan = systable_beginscan(ce_rel, ClientLogicColumnSettingsArgsOidIndexId, true, NULL, 1, &scankey);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for cast %u", id)));
    simple_heap_delete(ce_rel, &tuple->t_self);

    systable_endscan(scan);
    heap_close(ce_rel, RowExclusiveLock);
}

void get_global_setting_description(StringInfo buffer, const ObjectAddress *object)
{
    Relation cmKeys = NULL;
    ScanKeyData skey[1];
    SysScanDesc rcscan = NULL;
    HeapTuple tup = NULL;
    Form_gs_client_global_keys cmk_form = NULL;

    cmKeys = heap_open(ClientLogicGlobalSettingsId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    rcscan = systable_beginscan(cmKeys, ClientLogicGlobalSettingsOidIndexId, true, NULL, 1, skey);
    tup = systable_getnext(rcscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(rcscan);
        heap_close(cmKeys, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for cmk %u", object->objectId)));
    }
    cmk_form = (Form_gs_client_global_keys)GETSTRUCT(tup);
    appendStringInfo(buffer, _("client master key: %s"), cmk_form->global_key_name.data);
    systable_endscan(rcscan);
    heap_close(cmKeys, AccessShareLock);
}

void get_column_setting_description(StringInfo buffer, const ObjectAddress *object)
{
    Relation column_setting_rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc rcscan = NULL;
    HeapTuple tup = NULL;
    Form_gs_column_keys cek_form = NULL;

    column_setting_rel = heap_open(ClientLogicColumnSettingsId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    rcscan = systable_beginscan(column_setting_rel, ClientLogicColumnSettingsOidIndexId, true, NULL, 1, skey);
    tup = systable_getnext(rcscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(rcscan);
        heap_close(column_setting_rel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for cek %u", object->objectId)));
    }
    cek_form = (Form_gs_column_keys)GETSTRUCT(tup);
    appendStringInfo(buffer, _("column encryption key: %s"), cek_form->column_key_name.data);
    systable_endscan(rcscan);
    heap_close(column_setting_rel, AccessShareLock);
}

void get_cached_column_description(StringInfo buffer, const ObjectAddress *object)
{
    Relation encColumns = NULL;
    ScanKeyData skey[1];
    SysScanDesc rcscan = NULL;
    HeapTuple tup = NULL;
    Form_gs_encrypted_columns ec_form = NULL;
    encColumns = heap_open(ClientLogicCachedColumnsId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    rcscan = systable_beginscan(encColumns, GsSecEncryptedColumnsOidIndexId, true, SnapshotNow, 1, skey);
    tup = systable_getnext(rcscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(rcscan);
        heap_close(encColumns, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for encrypted column %u", object->objectId)));
    }
    ec_form = (Form_gs_encrypted_columns)GETSTRUCT(tup);
    appendStringInfo(buffer, _("encrypted column: %s"), ec_form->column_name.data);
    systable_endscan(rcscan);
    heap_close(encColumns, AccessShareLock);
}

void get_global_setting_args_description(StringInfo buffer, const ObjectAddress *object)
{
    Relation rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc rcscan = NULL;
    HeapTuple tup = NULL;
    Form_gs_client_global_keys_args cmk_args = NULL;
    rel = heap_open(ClientLogicGlobalSettingsArgsId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    rcscan = systable_beginscan(rel, ClientLogicGlobalSettingsArgsOidIndexId, true, NULL, 1, skey);
    tup = systable_getnext(rcscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(rcscan);
        heap_close(rel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for encrypted column %u", object->objectId)));
    }
    cmk_args = (Form_gs_client_global_keys_args)GETSTRUCT(tup);
    appendStringInfo(buffer, _("ckm args: %s"), cmk_args->function_name.data);
    systable_endscan(rcscan);
    heap_close(rel, AccessShareLock);
}

void get_column_setting_args_description(StringInfo buffer, const ObjectAddress *object)
{
    Relation rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc rcscan = NULL;
    HeapTuple tup = NULL;
    Form_gs_column_keys_args cek_args = NULL;
    rel = heap_open(ClientLogicColumnSettingsArgsId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    rcscan = systable_beginscan(rel, ClientLogicColumnSettingsArgsOidIndexId, true, NULL, 1, skey);
    tup = systable_getnext(rcscan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(rcscan);
        heap_close(rel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find tuple for encrypted column %u", object->objectId)));
    }
    cek_args = (Form_gs_column_keys_args)GETSTRUCT(tup);
    appendStringInfo(buffer, _("cek args: %s"), cek_args->function_name.data);
    systable_endscan(rcscan);
    heap_close(rel, AccessShareLock);
}

bool is_exist_encrypted_column(const ObjectAddresses *targetObjects)
{
    for (int i = 0; i < targetObjects->numrefs; i++) {
        ObjectAddress *object = targetObjects->refs + i;
        if (getObjectClass(object) == OCLASS_CL_CACHED_COLUMN) {
            return true;
        }
    }
    return false;
}

bool is_enc_type(Oid type_oid)
{
    Oid enc_type[] = {
        BYTEAWITHOUTORDERWITHEQUALCOLOID,
        BYTEAWITHOUTORDERCOLOID,
        BYTEAWITHOUTORDERWITHEQUALCOLARRAYOID,
        BYTEAWITHOUTORDERCOLARRAYOID,
    };

    for (size_t i = 0; i < sizeof(enc_type) / sizeof(enc_type[0]); i++) {
        if (type_oid == enc_type[i]) {
            return true;
        }
    }

    return false;
}

bool is_enc_type(const char *type_name)
{
    const char *enc_type[] = {
        "byteawithoutordercol",
        "byteawithoutorderwithequalcol",
        "_byteawithoutordercol",
        "_byteawithoutorderwithequalcol",
    };

    for (size_t i = 0; i < sizeof(enc_type) / sizeof(enc_type[0]); i++) {
        if (strcmp(type_name, enc_type[i]) == 0) {
            return true;
        }
    }
    return false;
}

bool IsFullEncryptedRel(char* objSchema, char* objName)
{
    bool is_encrypted = false;
    Oid namespaceId = get_namespace_oid((const char*)objSchema, false);
    Oid relnameId = get_relname_relid((const char*)objName, namespaceId);
    CatCList *catlist = SearchSysCacheList1(CERELIDCOUMNNAME, ObjectIdGetDatum(relnameId));
    if (catlist != NULL && catlist->n_members > 0) {
        is_encrypted = true;
    }
    ReleaseSysCacheList(catlist);
    return is_encrypted;
}
 
bool IsFuncProcOnEncryptedRel(char* objSchema, char* objName)
{
    bool is_encrypted = false;
    CatCList* catlist_funcs = NULL;
    catlist_funcs = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(objName));
    Oid namespaceId = get_namespace_oid((const char*)objSchema, false);
    for (int i = 0; i < catlist_funcs->n_members; i++) {
        if (is_encrypted) {
            break;
        }
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist_funcs, i);
        if (HeapTupleIsValid(proctup)) {
            Form_pg_proc pform = (Form_pg_proc)GETSTRUCT(proctup);
            Oid oldTupleOid = HeapTupleGetOid(proctup);
            /* compare function's namespace */
            if (pform->pronamespace != namespaceId) {
                continue;
            }
            HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(oldTupleOid));
            if (!HeapTupleIsValid(gs_oldtup)) {
                continue;
            }
            if (gs_oldtup->t_len > 0) {
                is_encrypted = true;
            }
            ReleaseSysCache(gs_oldtup);
        }
    }
    ReleaseSysCacheList(catlist_funcs);
    return is_encrypted;
}

bool is_full_encrypted_rel(Relation rel)
{
    if (rel == NULL || rel->rd_att == NULL || rel->rd_rel->relkind != RELKIND_RELATION) {
        return false;
    }
    TupleDesc tup_desc = rel->rd_att;
    for (int i = 0; i < tup_desc->natts; i++) {
        if (is_enc_type(tup_desc->attrs[i].atttypid)) {
            return true;
        }
    }
    return false;
}

/*
 * if a column is encrypted, we will rewrite its type and
 *  (1) store its source col_type in catalog 'gs_encrypted_columns'
 *  (2) store its new col_type in catalog 'pg_attribute'
 * 
 * while process 'CREATE TABLE AS' / 'SELECT INTO' statement,
 * we need get the source col_type from  'gs_encrypted_columns', then rewrite the parse tree
 */
ClientLogicColumnRef *get_column_enc_def(Oid rel_oid, const char *col_name)
{
    ClientLogicColumnRef *enc_def = makeNode(ClientLogicColumnRef);
    HeapTuple enc_col_tup;
    Form_gs_encrypted_columns enc_col;
    HeapTuple cek_tup;
    Form_gs_column_keys cek;

    /* search CEK oid from catalog 'gs_encrypted_columns' */
    enc_col_tup = SearchSysCache2(CERELIDCOUMNNAME, ObjectIdGetDatum(rel_oid), CStringGetDatum(col_name));
    if (!HeapTupleIsValid(enc_col_tup)) {
        return NULL;
    }
    enc_col = (Form_gs_encrypted_columns)GETSTRUCT(enc_col_tup);

    /* search CEK name from catalog 'gs_column_keys' */
    cek_tup = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(enc_col->column_key_id));
    if (!HeapTupleIsValid(cek_tup)) {
        ReleaseSysCache(enc_col_tup);
        return NULL;
    }
    cek = (Form_gs_column_keys) GETSTRUCT(cek_tup);

    enc_def->columnEncryptionAlgorithmType = (EncryptionType)enc_col->encryption_type;
    enc_def->column_key_name = list_make1(makeString(NameStr(cek->column_key_name)));
    enc_def->orig_typname = makeTypeNameFromOid(enc_col->data_type_original_oid,  enc_col->data_type_original_mod);
    enc_def->dest_typname = NULL;
    enc_def->location = -1; /* not used */
    
    ReleaseSysCache(enc_col_tup);
    ReleaseSysCache(cek_tup);

    return enc_def;
}

Datum get_client_info(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    const int COLUMN_NUM = 2;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupdesc = CreateTemplateTupleDesc(COLUMN_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "sid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "client_info", TEXTOID, -1, 0);
 
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = BlessTupleDesc(tupdesc);
 
    (void)MemoryContextSwitchTo(oldcontext);
    if (ENABLE_THREAD_POOL) {
        g_threadPoolControler->GetSessionCtrl()->getSessionClientInfo(rsinfo->setResult, rsinfo->setDesc);
    }
 
    tuplestore_donestoring(rsinfo->setResult);
    return (Datum)0;
}

const char *get_typename_by_id(Oid typeOid)
{
    if (typeOid == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
        return "byteawithoutorderwithequal";
    } else if (typeOid == BYTEAWITHOUTORDERCOLOID) {
        return "byteawithoutorder";
    }
    return NULL;
}

const char *get_encryption_type_name(EncryptionType algorithm_type)
{
    if (algorithm_type == EncryptionType::DETERMINISTIC_TYPE) {
        return "DETERMINISTIC";
    } else if (algorithm_type == EncryptionType::RANDOMIZED_TYPE) {
        return "RANDOMIZED";
    }
    return NULL;
}