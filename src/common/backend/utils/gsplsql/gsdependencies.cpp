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
* ---------------------------------------------------------------------------------------
*
* gsdependencies.cpp
*
*
* IDENTIFICATION
*        src/common/backend/utils/gsplsql/gsdependencies.cpp
*
* ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/plpgsql.h"
#include "utils/builtins.h"
#include "catalog/gs_dependencies_fn.h"
#include "utils/lsyscache.h"
#include "catalog/gs_dependencies.h"
#include "utils/pl_package.h"
#include "utils/fmgroids.h"
#include "catalog/indexing.h"
#include "storage/lmgr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_object.h"
#include "commands/dbcommands.h"
#include "catalog/pg_enum.h"
#include "catalog/heap.h"
#include "catalog/pg_type_fn.h"
#include "catalog/gs_package.h"
#include "catalog/gs_dependencies_obj.h"
#include "catalog/pg_proc_fn.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "storage/lock/lock.h"

static void complete_process_variables(PLpgSQL_package* pkg, bool isCreate, bool isRecompile);
static bool datumIsVariable(PLpgSQL_datum* datum);
static void do_complete_process_variables(PLpgSQL_package* pkg, bool isCreate, bool isRecompile, PLpgSQL_datum* datum);
static Oid gsplsql_parse_exist_pkg_and_build_obj_desc(GsDependObjDesc* obj, List* var_name, ListCell** attr_list);
static inline void gsplsql_free_var_mem_for_gs_dependencies(GsDependObjDesc* obj);
static inline bool gsplsql_is_proc_head_depend_type(const GsDependObjDesc* obj_desc, const GsDependObjDesc* ref_obj_desc);
static void gsplsql_insert_gs_dependency(Relation relation, NameData* schema_name_data,
                                         NameData* pkg_name_data, StringInfo object_name,
                                         int ref_obj_pos, Oid ref_obj_oid);
static DependenciesType *gsplsql_make_dependencies_type(Oid typOid);
static inline void gsplsql_delete_type(const char *schemaName, const char *pkgName, char *typName);
static bool gsplsql_invalidate_obj(GsDependObjDesc* obj, Datum refobjpos_datum,
                                    bool is_ref_remove, Oid curr_compile_oid);
static List* GetPkgFuncOids(Oid schemaOid, Oid pkgOid);
static bool gsplsql_get_depend_body_desc(GsDependParamBody* gs_depend_param_body);

static DependenciesDatum *gsplsql_make_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined);
static DependenciesDatum *gsplsql_make_row_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined);
static DependenciesDatum *gsplsql_make_record_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined);

static bool gsplsql_invalidate_pkg(const char *schemaName, const char *pkgName, bool isSpec, Oid currCompileOid);
static bool gsplsql_invalidate_func(Oid func_oid, Oid curr_compile_oid);
static bool gsplsql_check_var_attrs(PLpgSQL_datum *datum, ListCell *attr_list);

static inline Oid gsplsql_parse_pkg_var_and_attrs4(GsDependObjDesc* obj, List* var_name, ListCell** attr_list);
static inline Oid gsplsql_parse_pkg_var_and_attrs3(GsDependObjDesc* obj, ListCell* var_name, ListCell** attr_list);
static inline Oid gsplsql_parse_pkg_var_obj2(GsDependObjDesc* obj, ListCell* var_name);

static bool gsplsql_exist_func_object_in_dependencies_obj(char* nsp_name, char* pkg_name,
    const char* old_func_head_name, const char* old_func_name);
static bool gsplsql_exist_func_object_in_dependencies(char* nsp_name, char* pkg_name, const char* old_func_head_name);

void gsplsql_do_autonomous_compile(Oid objoid, bool is_pkg)
{
    if (u_sess->plsql_cxt.during_compile) {
        return;
    }
    if (u_sess->plsql_cxt.is_exec_autonomous || u_sess->is_autonomous_session) {
        return;
    }
    if (unlikely(g_instance.attr.attr_storage.max_concurrent_autonomous_transactions <= 0)) {
        ereport(WARNING, (errcode(ERRCODE_PLPGSQL_ERROR),
                          errmsg("The %s cannot be %s.", "compile function/package operator", "called in autonomous transaction"),
                          errdetail("The value of max_concurrent_autonomous_transactions must be greater than 0."),
                          errcause("%s depends on autonomous transaction.", "compile invalid function/package in dml"),
                          erraction("set max_concurrent_autonomous_transactions to a large value.")));
        return;
    }
    bool pushed = SPI_push_conditional();
    MemoryContext tmp_context = AllocSetContextCreate(u_sess->temp_mem_cxt,
                                                      "autonomous compile tmp context",
                                                      ALLOCSET_DEFAULT_MINSIZE,
                                                      ALLOCSET_DEFAULT_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_cxt = MemoryContextSwitchTo(tmp_context);
    ResourceOwner old_owner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* release lock for compiled object */
    bool found = false;
    GSPLSQLLockedObjKey key;
    if (is_pkg) {
        key = {1, objoid, u_sess->proc_cxt.MyDatabaseId};
    } else {
        key = {0, objoid, u_sess->proc_cxt.MyDatabaseId};
    }
    GSPLSQLLockedObjEntry* entry = NULL;
    if (u_sess->plsql_cxt.plpgsql_lock_objects) {
        entry = (GSPLSQLLockedObjEntry*)hash_search(u_sess->plsql_cxt.plpgsql_lock_objects, &key, HASH_FIND, &found);
        if (found && entry->has_locked) {
            if (!is_pkg) {
                UnlockProcedureIdForSession(objoid, u_sess->proc_cxt.MyDatabaseId, AccessShareLock);
            } else {
                UnlockPackageIdForSession(objoid, u_sess->proc_cxt.MyDatabaseId, AccessShareLock);
            }
            entry->has_locked = false;
        }
    }

    StringInfoData str;
    char* pkg_name = NULL;
    char* proc_name = NULL;
    char* obj_type = NULL;
    char* schema_name = NULL;
    initStringInfo(&str);
    appendStringInfoString(&str,
        "declare\n"
        "PRAGMA AUTONOMOUS_TRANSACTION;\n"
        "begin\n");
    /* check guc value*/
    appendStringInfo(&str, "set %s=%s;\n", "behavior_compat_options", "\'plpgsql_dependency\'");
    
    if (is_pkg) {
        schema_name = GetPackageSchemaName(objoid);
        obj_type = "package";
        pkg_name = GetPackageName(objoid);
        appendStringInfo(&str, "alter %s %s.%s ", obj_type, schema_name, quote_identifier(pkg_name));
    } else {
        char res = get_func_prokind(objoid);
        if (PROC_IS_PRO(res)) {
            obj_type = "procedure";
        } else if (PROC_IS_FUNC(res)) {
            obj_type = "function";
        } else {
            ereport(WARNING, (errcode(ERRCODE_PLPGSQL_ERROR),
                               errmsg("get wrong func type by func oid %u", objoid)));
        }
        schema_name = get_namespace_name(get_func_namespace(objoid));
        proc_name = format_procedure_no_visible(objoid);
        appendStringInfo(&str, "alter %s %s.%s ", obj_type, schema_name, proc_name);
    }
    appendStringInfoString(&str, "compile;\n");
    appendStringInfoString(&str, "end;");

    int save_compile_status = getCompileStatus();
    List* save_compile_list = u_sess->plsql_cxt.compile_context_list;
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    u_sess->plsql_cxt.during_compile = true;
    PG_TRY();
    {
        List* raw_parse_list = raw_parser(str.data, NULL);
        DoStmt* stmt = (DoStmt*)linitial(raw_parse_list);
        (void)CompileStatusSwtichTo(NONE_STATUS);
        u_sess->plsql_cxt.curr_compile_context = NULL;
        u_sess->plsql_cxt.compile_context_list = NULL;
        ExecuteDoStmt(stmt, true);
        t_thrd.utils_cxt.CurrentResourceOwner = old_owner;
    }
    PG_CATCH();
    {
        (void)CompileStatusSwtichTo(save_compile_status);
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_context_list = save_compile_list;
        u_sess->plsql_cxt.during_compile = false;
        MemoryContextSwitchTo(old_cxt);
        t_thrd.utils_cxt.CurrentResourceOwner = old_owner;

        ErrorData* errdata = CopyErrorData();
        FlushErrorState();
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("Executed %s %s is invalid, and do compile ddl %s.", obj_type,
                               is_pkg ? pkg_name : proc_name, str.data),
                        errdetail("COMPLIE fail reason: (%s,%d) %s.", errdata->funcname,
                                  errdata->lineno, errdata->message),
                        erraction("Please call ALTER PACKAGE/FUNCTION COMPILE to validate %s %s.",
                                  obj_type, is_pkg ? pkg_name : proc_name)));
    }
    PG_END_TRY();
    (void)CompileStatusSwtichTo(save_compile_status);
    u_sess->plsql_cxt.curr_compile_context = save_compile_context;
    u_sess->plsql_cxt.compile_context_list = save_compile_list;
    u_sess->plsql_cxt.during_compile = false;
    MemoryContextSwitchTo(old_cxt);
    MemoryContextDelete(tmp_context);
    SPI_pop_conditional(pushed);
    /* hold lock again */
    if (u_sess->plsql_cxt.plpgsql_lock_objects) {
        entry = (GSPLSQLLockedObjEntry*)hash_search(u_sess->plsql_cxt.plpgsql_lock_objects, &key, HASH_FIND, &found);
        if (found && !entry->has_locked) {
            if (!is_pkg) {
                LockProcedureIdForSession(objoid, u_sess->proc_cxt.MyDatabaseId, AccessShareLock);
            } else {
                LockPackageIdForSession(objoid, u_sess->proc_cxt.MyDatabaseId, AccessShareLock);
            }
            entry->has_locked = true;
        }
    }
}

bool gsplsql_exists_func_obj(Oid nsp_oid, Oid pkg_oid, const char* old_func_head_name, const char* old_func_name)
{
    char* nsp_name = get_namespace_name(nsp_oid);
    char* pkg_name = pstrdup("null");
    if (OidIsValid(pkg_oid)) {
        pfree_ext(pkg_name);
        pkg_name = GetPackageName(pkg_oid);
    }
    bool exists_func = gsplsql_exist_func_object_in_dependencies_obj(nsp_name, pkg_name, old_func_head_name, old_func_name) ||
                       gsplsql_exist_func_object_in_dependencies(nsp_name, pkg_name, old_func_head_name);
    pfree_ext(pkg_name);
    pfree_ext(nsp_name);
    return exists_func;
}

bool gsplsql_exists_schema_name(const char* schema_name)
{
    bool has_schema = false;
    Assert(schema_name != NULL);
    // check gs_dependencies_obj
    int key_num = 0;
    ScanKeyData key[1];
    ScanKeyInit(&key[key_num++], Anum_gs_dependencies_obj_schemaname, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(schema_name));
    bool is_null = false;
    HeapTuple tuple;
    Relation obj_rel = heap_open(DependenciesObjRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(obj_rel, DependenciesObjNameIndexId, true, SnapshotSelf, key_num, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        has_schema = true;
        break;
    }
    systable_endscan(scan);
    heap_close(obj_rel, AccessShareLock);
    if (has_schema) {
        return has_schema;
    }
    // check gs_dependencies
    int key_num_dep = 0;
    ScanKeyData key_dep[1];
    ScanKeyInit(&key_dep[key_num_dep++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(schema_name));
    HeapTuple tuple_dep;
    Relation dep_rel = heap_open(DependenciesRelationId, AccessShareLock);
    SysScanDesc scan_dep = systable_beginscan(dep_rel, DependenciesNameIndexId, true, SnapshotSelf,
                                              key_num_dep, key_dep);
    while (HeapTupleIsValid(tuple_dep = systable_getnext(scan_dep))) {
        has_schema = true;
        break;
    }
    systable_endscan(scan_dep);
    heap_close(dep_rel, AccessShareLock);
    return has_schema;
}

void gsplsql_init_gs_depend_obj_desc(GsDependObjDesc* object)
{
    object->schemaName = NULL;
    object->packageName = NULL;
    object->name = NULL;
    object->type = GSDEPEND_OBJECT_TYPE_INVALID;
    object->refPosType = GSDEPEND_REFOBJ_POS_INVALID;
}

bool gsplsql_need_build_gs_dependency(GsDependParamBody* gsdependParamBody, const ObjectAddress* referenced, bool is_pinned)
{
    if (NULL == gsdependParamBody) {
        return false;
    }
    if (UNDEFINEDOID == referenced->objectId) {
        return true;
    }
    if (is_pinned) {
        if (NULL != gsdependParamBody->dependExtend) {
            if (NULL != gsdependParamBody->dependExtend->objectName) {
                return true;
            } else if (OidIsValid(gsdependParamBody->dependExtend->typeOid)) {
                return !IsPinnedObject(TypeRelationId, gsdependParamBody->dependExtend->typeOid);
            } else {
                return false;
            }
        }
    }
    return true;
}

bool gsplsql_remove_gs_dependency(const GsDependObjDesc* obj_desc)
{
    Relation rel = heap_open(DependenciesRelationId, RowExclusiveLock);
    List* list = gsplsql_delete_objs(rel, obj_desc);
    gsplsql_delete_unrefer_depend_obj_in_list(list, true);
    list_free(list);
    heap_close(rel, RowExclusiveLock);
    return true;
}

bool gsplsql_build_gs_type_in_body_dependency(PLpgSQL_type* type)
{
    GsDependParamBody gs_depend_param_body;
    gsplsql_init_gs_depend_param_body(&gs_depend_param_body);
    bool ret = gsplsql_get_depend_body_desc(&gs_depend_param_body);
    if (!ret) {
        return false;
    }
    gs_depend_param_body.dependExtend = type->dependExtend;
    gs_depend_param_body.type = GSDEPEND_OBJECT_TYPE_TYPE;
    ObjectAddress ref;
    ref.classId = TypeRelationId;
    ref.objectId =type->typoid;
    ref.objectSubId = 0;
    bool needBuild = gsplsql_need_build_gs_dependency(&gs_depend_param_body, &ref,
        IsPinnedObject(ref.classId, ref.objectId));
    if (!needBuild) {
        return false;
    }
    return gsplsql_build_gs_type_dependency(&gs_depend_param_body, &ref);
}

static bool gsplsql_get_depend_body_desc(GsDependParamBody* gs_depend_param_body)
{
    int cw = CompileWhich();
    if (cw == PLPGSQL_COMPILE_PACKAGE || cw == PLPGSQL_COMPILE_PACKAGE_PROC) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        gs_depend_param_body->dependNamespaceOid = pkg->namespaceOid;
        gs_depend_param_body->dependPkgOid = pkg->pkg_oid;
        gs_depend_param_body->dependPkgName = pkg->pkg_signature;
        gs_depend_param_body->dependName = pkg->pkg_signature;
        gs_depend_param_body->refPosType = pkg->is_spec_compiling ?
            GSDEPEND_REFOBJ_POS_IN_PKGSPEC : GSDEPEND_REFOBJ_POS_IN_PKGBODY;
        return true;
    } else if (cw == PLPGSQL_COMPILE_PROC) {
        PLpgSQL_function* func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
        gs_depend_param_body->dependNamespaceOid = func->namespaceOid;
        gs_depend_param_body->dependName = format_procedure_no_visible(func->fn_oid);
        gs_depend_param_body->refPosType = GSDEPEND_REFOBJ_POS_IN_PROCBODY;
        return true;
    }
    return false;
}

static char* gsplsql_get_obj_name_by_obj_addr(const ObjectAddress* depender, Oid pkgOid)
{
    switch (depender->classId) {
        case ProcedureRelationId: {
            return pstrdup(format_procedure_no_visible(depender->objectId));
        } break;
        case TypeRelationId: {
            char* name = get_typename(depender->objectId);
            if (name != NULL && OidIsValid(pkgOid)) {
                char* real_name = ParseTypeName(name, pkgOid);
                if (NULL != real_name) {
                    pfree_ext(name);
                    name = real_name;
                }
            }
            return name;
        } break;
        case RelationRelationId: {
            char* name = get_rel_name(depender->objectId);
            if (name != NULL && OidIsValid(pkgOid)) {
                char* real_name = ParseTypeName(name, pkgOid);
                if (NULL != real_name) {
                    pfree_ext(name);
                    name = real_name;
                }
            }
            return name;
        } break;
        default:
            break;
    }

    return NULL;
}

static bool gsplsql_make_ref_type_depend_obj_desc(const ObjectAddress* referenced, GsDependParamBody* gsdependParamBody,
    GsDependObjDesc* ref_obj_desc, TypeDependExtend* depend_extend, ObjectAddress* ref_assemble)
{
    bool dep_is_null = depend_extend == NULL;
    ref_obj_desc->name = (dep_is_null ? NULL :depend_extend->objectName);
    if (NULL == ref_obj_desc->name) {
        ref_obj_desc->packageName = NULL;
        Oid ref_typ_oid = dep_is_null ? InvalidOid : depend_extend->typeOid;
        if (ref_typ_oid == InvalidOid) {
            ref_typ_oid = referenced->objectId;
            if (referenced->classId == RelationRelationId) {
                ref_typ_oid = get_rel_type_id(ref_typ_oid);
            }
        } else {
            ref_assemble->classId = TypeRelationId;
            ref_assemble->objectId = ref_typ_oid;
        }
        if (ref_assemble->objectId == UNDEFINEDOID) {
            return false;
        }
        Oid elem_oid_for_arr = get_array_internal_depend_type_oid(ref_assemble->objectId);
        if (OidIsValid(elem_oid_for_arr)) {
            ref_assemble->objectId = elem_oid_for_arr;
        }
        Oid ref_pkg_oid = GetTypePackageOid(ref_typ_oid);
        if (gsdependParamBody->dependPkgOid != InvalidOid &&
            gsdependParamBody->dependPkgOid == ref_pkg_oid) {
            return gsdependParamBody->refPosType == GSDEPEND_REFOBJ_POS_IN_PROCHEAD;
        }
        if (OidIsValid(ref_pkg_oid)) {
            char* pkg_name = GetPackageName(ref_pkg_oid);
            if (pkg_name != NULL) {
                ref_obj_desc->packageName = pstrdup(pkg_name);
                pfree_ext(pkg_name);
            }
        }
        AssertEreport(TypeRelationId == ref_assemble->classId, MOD_PLSQL,
                      "ref_assemble->classId must be type.");
        ref_obj_desc->schemaName = get_typenamespace(ref_assemble->objectId);
        ref_obj_desc->name = gsplsql_get_obj_name_by_obj_addr(ref_assemble, ref_pkg_oid);
        ref_obj_desc->type = gsdependParamBody->type;
    } else {
        ref_obj_desc->schemaName = pstrdup(depend_extend->schemaName);
        ref_obj_desc->packageName = pstrdup(depend_extend->packageName);
        ref_obj_desc->type = GSDEPEND_OBJECT_TYPE_VARIABLE;
    }
    return true;
}

static void gsplsql_adjust_ref_pos_type(GsDependObjDesc* obj_desc)
{
    switch (CompileWhich()) {
        case PLPGSQL_COMPILE_PACKAGE_PROC: {
            PLpgSQL_package* pkg = 
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            if (!pkg->is_spec_compiling) {
                obj_desc->refPosType = GSDEPEND_REFOBJ_POS_IN_PKGBODY;
                obj_desc->name = pkg->pkg_signature;
            }
        } break;
        case PLPGSQL_COMPILE_PROC: {
            PLpgSQL_function* func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
            *obj_desc = gsplsql_construct_func_head_obj(func->fn_oid,
                func->namespaceOid, func->pkg_oid);
            obj_desc->refPosType = GSDEPEND_REFOBJ_POS_IN_PROCBODY;
            obj_desc->name = func->fn_signature;
        } break;
        default:
            break;
    }
}

static bool gsplsql_build_gs_type_dependency(GsDependObjDesc* obj_desc, GsDependObjDesc* ref_obj_desc,
                                             const Oid typOid, bool* depend_undefined)
{
    DependenciesDatum* depend_datum = NULL;
    if (NULL != ref_obj_desc->packageName) {
        List* typNameList = NIL;
        if (NULL != ref_obj_desc->name) {
            typNameList = lcons(makeString(ref_obj_desc->name), typNameList);
        }
        if (NULL != ref_obj_desc->packageName) {
            typNameList = lcons(makeString(ref_obj_desc->packageName), typNameList);
        }
        Oid namespace_oid = InvalidOid;
        if (NULL != ref_obj_desc->schemaName) {
            typNameList = lcons(makeString(ref_obj_desc->schemaName), typNameList);
            namespace_oid = get_namespace_oid(ref_obj_desc->schemaName, true);
        }
        Oid pkg_oid = PackageNameGetOid(ref_obj_desc->packageName, namespace_oid);
        if (ref_obj_desc->type == GSDEPEND_OBJECT_TYPE_VARIABLE) {
            PLpgSQL_datum* datum = NULL;
            if (OidIsValid(pkg_oid)) {
                datum = GetPackageDatum(typNameList);
            }
            depend_datum = gsplsql_make_depend_datum_from_plsql_datum(datum,
                &ref_obj_desc->schemaName, depend_undefined);
        } else {
            if (!GetPgObjectValid(pkg_oid, OBJECT_TYPE_PKGSPEC)) {
                if (GetCurrCompilePgObjStatus()) {
                    ereport(WARNING,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Package %u is invalid.", pkg_oid)));
                }
                InvalidateCurrCompilePgObj();
            }
            Oid type_oid = LookupTypeInPackage(typNameList, ref_obj_desc->name,
                pkg_oid, namespace_oid);
            depend_datum = gsplsql_make_depend_datum_by_type_oid(type_oid, depend_undefined);
        }
        list_free_ext(typNameList);
    } else {
        depend_datum = gsplsql_make_depend_datum_by_type_oid(typOid, depend_undefined);
    }
    return gsplsql_build_gs_dependency(obj_desc, ref_obj_desc, depend_datum);
}

static bool gsplsql_match_obj_desc(const GsDependObjDesc* lhs, const GsDependObjDesc* rhs, bool match_name)
{
    if (NULL != lhs->schemaName || NULL != rhs->schemaName) {
        if (NULL != lhs->schemaName && NULL != rhs->schemaName &&
            0 != strcmp(lhs->schemaName, rhs->schemaName)) {
            return false;
        } else {
            return false;
        }
    }
    if (NULL != lhs->packageName || NULL != rhs->packageName) {
        if (NULL != lhs->packageName && NULL != rhs->packageName &&
            0 != strcmp(lhs->packageName, rhs->packageName)) {
            return false;
        } else {
            return false;
        }
    }
    if (match_name) {
        if (NULL != lhs->name || NULL != rhs->name) {
            if (NULL != lhs->name && NULL != rhs->name) {
                return 0 == strcmp(lhs->packageName, rhs->packageName);
            } else {
                return false;
            }
        }
    }
    return true;
}

bool gsplsql_undefine_in_same_pkg(GsDependObjDesc* obj_desc, Oid undefObjOid)
{
    if (NULL == obj_desc || NULL == obj_desc->packageName) {
        return false;
    }
    GsDependObjDesc ref_obj_desc;
    bool ret = gsplsql_search_depend_obj_by_oid(undefObjOid, &ref_obj_desc);
    if (!ret) {
        ereport(WARNING,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("dependencies object %u does not exist.", undefObjOid)));
        return false;
    }
    ret = gsplsql_match_obj_desc(obj_desc, &ref_obj_desc, false) && (NULL != ref_obj_desc.packageName);
    free_gs_depend_obj_desc(&ref_obj_desc);
    if (ret) {
        gsplsql_delete_unrefer_depend_obj_oid(undefObjOid, false);
        return true;
    }
    return false;
}

bool gsplsql_build_gs_type_dependency(GsDependParamBody* gsdependParamBody, const ObjectAddress* referenced)
{
    Oid namespace_oid = gsdependParamBody->dependNamespaceOid;
    GsDependObjDesc obj_desc;
    GsDependObjDesc ref_obj_desc = {NULL, NULL, NULL, GSDEPEND_OBJECT_TYPE_INVALID, 0};
    obj_desc.schemaName = get_namespace_name(namespace_oid);
    obj_desc.packageName = gsdependParamBody->dependPkgName;
    obj_desc.refPosType = gsdependParamBody->refPosType;
    obj_desc.name = gsdependParamBody->dependName;
    if (GSDEPEND_REFOBJ_POS_IN_TYPE == obj_desc.refPosType) {
        gsplsql_adjust_ref_pos_type(&obj_desc);
    }
    TypeDependExtend* depend_extend = gsdependParamBody->dependExtend;
    if (referenced->objectId == UNDEFINEDOID) {
        if (GetCurrCompilePgObjStatus()) {
            ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("Type %s depends on an undefined type.", obj_desc.name)));
        }
        InvalidateCurrCompilePgObj();
        if (NULL == depend_extend) {
            return false;
        }
        depend_extend->dependUndefined = true;
        if (OidIsValid(depend_extend->undefDependObjOid)) {
            bool ret = gsplsql_undefine_in_same_pkg(&obj_desc, depend_extend->undefDependObjOid);
            if (ret) {
                return false;
            }
        }
        /*
         * pkg.v1%type
         */
        if (OidIsValid(depend_extend->undefDependObjOid)) {
            if (gsplsql_search_depend_obj_by_oid(depend_extend->undefDependObjOid, &ref_obj_desc)) {
                free_gs_depend_obj_desc(&ref_obj_desc);
            }
            return gsplsql_build_gs_dependency(&obj_desc, depend_extend->undefDependObjOid);
        }
    }
    ObjectAddress ref_assemble = *referenced;
    bool ret = gsplsql_make_ref_type_depend_obj_desc(referenced, gsdependParamBody, &ref_obj_desc, depend_extend, &ref_assemble);
    if (!ret) {
        return false;
    }
    if (GSDEPEND_OBJECT_TYPE_TYPE == gsdependParamBody->type) {
        bool dep_undefined = false;
        ret = gsplsql_build_gs_type_dependency(&obj_desc, &ref_obj_desc, ref_assemble.objectId, &dep_undefined);
        if (NULL != depend_extend && dep_undefined) {
            depend_extend->dependUndefined = dep_undefined;
        }
    }
    pfree_ext(ref_obj_desc.schemaName);
    return ret;
}

Oid gsplsql_try_build_exist_schema_undef_table(RangeVar* rel_var)
{
    if (NULL != rel_var->schemaname &&
        !OidIsValid(get_namespace_oid(rel_var->schemaname, true))) {
        return InvalidOid;
    }
    GsDependObjDesc obj_desc;
    char* active_schema_name = NULL;
    if (NULL != rel_var->schemaname) {
        obj_desc.schemaName = rel_var->schemaname;
    } else {
        active_schema_name = get_namespace_name(get_compiled_object_nspoid());
        obj_desc.schemaName = active_schema_name;
    }
    obj_desc.packageName = NULL;
    obj_desc.name = rel_var->relname;
    obj_desc.type = GSDEPEND_OBJECT_TYPE_TYPE;
    return gsplsql_flush_undef_ref_depend_obj(&obj_desc);
}

Oid gsplsql_try_build_exist_pkg_undef_var(List* dtnames)
{
    ListCell* attr_list = NULL;
    GsDependObjDesc obj_desc;
    gsplsql_init_gs_depend_obj_desc(&obj_desc);
    if (!OidIsValid(gsplsql_parse_exist_pkg_and_build_obj_desc(&obj_desc, dtnames, &attr_list))) {
        pfree_ext(obj_desc.schemaName);
        pfree_ext(obj_desc.packageName);
        pfree_ext(obj_desc.name);
        return InvalidOid;
    }
    return gsplsql_flush_undef_ref_depend_obj(&obj_desc);
}

Oid gsplsql_flush_undef_ref_depend_obj(const GsDependObjDesc* object)
{
    DependenciesUndefined* nodeUndefined = makeNode(DependenciesUndefined);
    Oid objOid = gsplsql_update_object_ast(object, (const DependenciesDatum*)nodeUndefined);
    pfree_ext(nodeUndefined);
    return objOid;
}

bool gsplsql_exist_dependency(GsDependObjDesc* obj_desc)
{
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    int keyNum = 0;
    ScanKeyData key[3];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(&schema_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(&pkg_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_refobjpos, BTEqualStrategyNumber,
        F_INT4EQ, Int32GetDatum(obj_desc->refPosType));
    bool is_null = false;
    HeapTuple tuple;
    Relation dep_rel = heap_open(DependenciesRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(dep_rel, DependenciesNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum obj_datum_tmp = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                   RelationGetDescr(dep_rel), &is_null);
        char* obj_name_tmp = TextDatumGetCString(obj_datum_tmp);
        if (object_name_data.data != NULL && strcmp(object_name_data.data, obj_name_tmp) == 0) {
            pfree_ext(obj_name_tmp);
            systable_endscan(scan);
            heap_close(dep_rel, AccessShareLock);
            FreeStringInfo(&object_name_data);
            return true;
        }
        pfree_ext(obj_name_tmp);
    }
    systable_endscan(scan);
    heap_close(dep_rel, AccessShareLock);
    FreeStringInfo(&object_name_data);
    return false;
}

void gsplsql_construct_non_empty_obj(const GsDependObjDesc* object, Name schema_name_data,
                                    Name pkg_name_data, StringInfo object_name)
{
    if (object->schemaName == NULL || strlen(object->schemaName) == 0) {
        (void)namestrcpy(schema_name_data, "null");
    } else {
        (void)namestrcpy(schema_name_data, object->schemaName);
    }
    if (object->packageName == NULL || strlen(object->packageName) == 0) {
        (void)namestrcpy(pkg_name_data, "null");
    } else {
        (void)namestrcpy(pkg_name_data, object->packageName);
    }
    initStringInfo(object_name);
    if (object->name == NULL || strlen(object->name) == 0) {
        if (object->packageName != NULL && strlen(object->packageName) != 0) {
            appendStringInfoString(object_name, object->packageName);
        } else {
            appendStringInfoString(object_name, "null");
        }
    } else {
        appendStringInfoString(object_name, object->name);
    }
}

GsDependObjDesc gsplsql_construct_func_head_obj(Oid procOid, Oid procNamespace, Oid proPackageId)
{
    GsDependObjDesc obj_desc;
    gsplsql_init_gs_depend_obj_desc(&obj_desc);
    obj_desc.schemaName = get_namespace_name(procNamespace);
    if (OidIsValid(proPackageId)) {
        obj_desc.packageName = GetPackageName(proPackageId);
    } else {
        obj_desc.packageName = pstrdup("null");
    }
    MemoryContext save_context = CurrentMemoryContext;
    PG_TRY();
    {
        obj_desc.name = format_procedure_no_visible(procOid);
    }
    PG_CATCH();
    {
        ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
        if (edata->sqlerrcode == ERRCODE_CACHE_LOOKUP_FAILED) {
            (void)MemoryContextSwitchTo(save_context);
            obj_desc.name = get_func_name(procOid);
            ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("The oid of the input parameter type of function %s does not exist.", obj_desc.name)));
            FlushErrorState();
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return obj_desc;
}

bool gsplsql_set_pkg_func_status(Oid schema_oid, Oid pkg_oid, bool status)
{
    bool result = false;
    List* list = GetPkgFuncOids(schema_oid, pkg_oid);
    ListCell* cell = NULL;
    foreach(cell, list) {
        Oid func_oid = lfirst_oid(cell);
        result = SetPgObjectValid(func_oid, OBJECT_TYPE_PROC, status) || result;
        if (!status) {
            CacheInvalidateFunction(func_oid, pkg_oid);
        }
    }
    list_free(list);
    return result;
}

void gsplsql_build_gs_variable_dependency(List* var_name)
{
    ListCell* attr_list = NULL;
    GsDependObjDesc ref_obj_desc;
    GsDependObjDesc obj_desc;
    gsplsql_init_gs_depend_obj_desc(&ref_obj_desc);
    gsplsql_init_gs_depend_obj_desc(&obj_desc);
    if (!OidIsValid(gsplsql_parse_exist_pkg_and_build_obj_desc(&ref_obj_desc, var_name, &attr_list))) {
        gsplsql_free_var_mem_for_gs_dependencies(&ref_obj_desc);
        return;
    }
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PACKAGE || rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        obj_desc.schemaName = get_namespace_name(pkg->namespaceOid);
        obj_desc.packageName = pkg->pkg_signature;
        obj_desc.name = pkg->pkg_signature;
        if (pkg->is_spec_compiling) {
            obj_desc.refPosType = GSDEPEND_REFOBJ_POS_IN_PKGSPEC;
        } else {
            obj_desc.refPosType = GSDEPEND_REFOBJ_POS_IN_PKGBODY;
        }
    } else if (rc == PLPGSQL_COMPILE_PROC) {
        PLpgSQL_function* func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
        obj_desc = gsplsql_construct_func_head_obj(func->fn_oid, func->namespaceOid, func->pkg_oid);
        obj_desc.refPosType = GSDEPEND_REFOBJ_POS_IN_PROCBODY;
    } else {
        gsplsql_free_var_mem_for_gs_dependencies(&ref_obj_desc);
        return;
    }
    List* new_var_name = list_make3(makeString(ref_obj_desc.schemaName), makeString(ref_obj_desc.packageName),
                                  makeString(ref_obj_desc.name));
    PLpgSQL_datum* datum = GetPackageDatum(new_var_name);
    list_free_ext(new_var_name);
    if (datum == NULL) {
        if (!enable_plpgsql_undefined()) {
            ereport(ERROR,  (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                              errmsg("%s.%s.%s must be declared.",
                                     ref_obj_desc.schemaName, ref_obj_desc.packageName,
                                     ref_obj_desc.name)));
        } else {
            ereport(WARNING,  (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("%s.%s.%s must be declared.",
                                   ref_obj_desc.schemaName, ref_obj_desc.packageName,
                                   ref_obj_desc.name)));
        }
        ref_obj_desc.type = GSDEPEND_OBJECT_TYPE_UNDEFIND;
    } else {
        if (!datumIsVariable(datum)) {
            return;
        }
        if (!gsplsql_check_var_attrs(datum, attr_list)) {
            if (!enable_plpgsql_undefined()) {
                ereport(ERROR,  (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                                  errmsg("%s.%s.%s datum does not have %s attribute.",
                                         ref_obj_desc.schemaName, ref_obj_desc.packageName,
                                         ref_obj_desc.name, strVal(lfirst(attr_list)))));
            } else {
                if (GetCurrCompilePgObjStatus()) {
                    ereport(WARNING,  (errmodule(MOD_PLSQL), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("%s.%s.%s datum does not have %s attribute.",
                            ref_obj_desc.schemaName, ref_obj_desc.packageName,
                            ref_obj_desc.name, strVal(lfirst(attr_list)))));
                }
                InvalidateCurrCompilePgObj();
            }
        }
    }
    bool type_depend_undefined = false;
    gsplsql_build_gs_dependency(&obj_desc, &ref_obj_desc,
                                gsplsql_make_depend_datum_from_plsql_datum(datum, NULL, &type_depend_undefined));
    if (type_depend_undefined) {
        if (GetCurrCompilePgObjStatus()) {
            ereport(WARNING,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                              errdetail("dependencies %s attribute must be declared.", ref_obj_desc.name)));
        }
        InvalidateCurrCompilePgObj();
    }
    pfree_ext(obj_desc.schemaName);
    gsplsql_free_var_mem_for_gs_dependencies(&ref_obj_desc);
}

bool gsplsql_build_gs_dependency(const GsDependObjDesc* obj_desc, const GsDependObjDesc* ref_obj_desc,
                                   const DependenciesDatum* ref_obj_ast)
{
    if (!gsplsql_is_proc_head_depend_type(obj_desc, ref_obj_desc)) {
        if (NULL != obj_desc->schemaName && NULL != ref_obj_desc->schemaName &&
            0 == strcmp(obj_desc->schemaName, ref_obj_desc->schemaName)) {
            if (NULL != obj_desc->packageName && NULL != ref_obj_desc->packageName &&
                0 == strcmp(obj_desc->packageName, ref_obj_desc->packageName)) {
                return false;
            }
        }
    }
    Oid ref_obj_oid = gsplsql_update_object_ast(ref_obj_desc, ref_obj_ast);
    return gsplsql_build_gs_dependency(obj_desc, ref_obj_oid);
}

void gsplsql_init_gs_depend_param_body(GsDependParamBody* param_body)
{
    if (NULL == param_body) {
        return;
    }
    param_body->dependNamespaceOid = InvalidOid;
    param_body->dependPkgOid = InvalidOid;
    param_body->dependPkgName = NULL;
    param_body->dependName = NULL;
    param_body->dependExtend = NULL;
    param_body->type = GSDEPEND_OBJECT_TYPE_INVALID;
    param_body->refPosType = GSDEPEND_REFOBJ_POS_INVALID;
    param_body->hasDependency = false;
}

bool gsplsql_build_gs_dependency(const GsDependObjDesc* obj_desc, const Oid ref_obj_oid)
{
    if (InvalidOid == ref_obj_oid) {
        return false;
    }
    ScanKeyData key;
    HeapTuple tuple = NULL;
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PACKAGE || rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        pkg->preRefObjectOidList = list_delete_oid(pkg->preRefObjectOidList, ref_obj_oid);
    }
    bool find = false;
    bool is_null = false;
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    Relation relation = heap_open(DependenciesRelationId, RowExclusiveLock);
    ScanKeyInit(&key, Anum_gs_dependencies_refobjoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ref_obj_oid));
    SysScanDesc scan = systable_beginscan(relation, DependenciesRefOidIndexId, true, SnapshotSelf, 1, &key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum schema_name_datum = heap_getattr(tuple, Anum_gs_dependencies_schemaname,
                                               RelationGetDescr(relation),&is_null);
        AssertEreport(!is_null, MOD_PLSQL, "schemaName must not be null");
        Datum pkg_name_datum = heap_getattr(tuple, Anum_gs_dependencies_packagename,
                                               RelationGetDescr(relation),&is_null);
        AssertEreport(!is_null, MOD_PLSQL, "pkgName must not be null");
        Datum name_datum = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                               RelationGetDescr(relation),&is_null);
        AssertEreport(!is_null, MOD_PLSQL, "name must not be null");
        char* obj_name = TextDatumGetCString(name_datum);
        if (0 != strcmp(schema_name_data.data, NameStr(*DatumGetName(schema_name_datum))) ||
            0 != strcmp(pkg_name_data.data, NameStr(*DatumGetName(pkg_name_datum))) ||
            0 != strcmp(object_name_data.data, obj_name)) {
            pfree_ext(obj_name);
            continue;
        }
        pfree_ext(obj_name);
        Datum ref_obj_pos_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjpos,
                                        RelationGetDescr(relation),&is_null);
        AssertEreport(!is_null, MOD_PLSQL, "refobjpos must not be null");
        if (obj_desc->refPosType == DatumGetInt32(ref_obj_pos_datum)) {
            find = true;
            break;
        }
    }
    if (!find) {
        gsplsql_insert_gs_dependency(relation, &schema_name_data, &pkg_name_data, &object_name_data,
                                     obj_desc->refPosType, ref_obj_oid);
    }
    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    FreeStringInfo(&object_name_data);
    return true;
}

void gsplsql_invalidate_objs(Oid oid, bool is_ref_remove, Oid curr_compile_oid)
{
    Relation relation;
    List* list = NIL;
    List* list_tmp = NIL;
    relation = heap_open(DependenciesRelationId, AccessShareLock);
    list_tmp = gsplsql_search_gs_depend_rel_by_oid(relation, oid);
    if (list_tmp == NULL) {
        heap_close(relation, AccessShareLock);
        return;
    }
    list = list_concat(list, list_tmp);
    ListCell* cell = NULL;
    foreach (cell, list) {
        bool is_null = false;
        GsDependObjDesc obj_desc;
        gsplsql_init_gs_depend_obj_desc(&obj_desc);
        HeapTuple tuple = (HeapTuple)lfirst(cell);
        Datum schema_name_datum = heap_getattr(tuple, Anum_gs_dependencies_schemaname,
                                               RelationGetDescr(relation), &is_null);
        if (!is_null) {
            obj_desc.schemaName = NameStr(*(DatumGetName(schema_name_datum)));
        }
        Datum package_name_datum = heap_getattr(tuple, Anum_gs_dependencies_packagename,
                                                RelationGetDescr(relation), &is_null);
        if (!is_null) {
            obj_desc.packageName = NameStr(*(DatumGetName(package_name_datum)));
        }
        Datum obj_name_datum = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                            RelationGetDescr(relation), &is_null);
        if (!is_null) {
            obj_desc.name = TextDatumGetCString(obj_name_datum);
        }
        Datum refobjpos_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjpos,
                                             RelationGetDescr(relation), &is_null);
        if (!gsplsql_invalidate_obj(&obj_desc, refobjpos_datum, is_ref_remove, curr_compile_oid)) {
            continue;
        }
        pfree_ext(obj_desc.name);
        heap_freetuple(tuple);
    }
    list_free(list);
    heap_close(relation, AccessShareLock);
}

List *gsplsql_search_gs_depend_rel_by_oid(Relation relation, Oid ref_obj_oid)
{
    if (!OidIsValid(ref_obj_oid)) {
        return NULL;
    }
    ScanKeyData key;
    List* list = NIL;
    HeapTuple tuple = NULL;
    ScanKeyInit(&key, Anum_gs_dependencies_refobjoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ref_obj_oid));
    SysScanDesc scan = systable_beginscan(relation, DependenciesRefOidIndexId, true, SnapshotSelf, 1, &key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        list = lappend(list, heap_copytuple(tuple));
    }
    systable_endscan(scan);
    return list;
}

DependenciesDatum* gsplsql_make_depend_datum_from_plsql_datum(const PLpgSQL_datum *datum, char** schema_name,
                                                              bool* depend_undefined)
{
    if (datum == NULL) {
        *depend_undefined = true;
        if (GetCurrCompilePgObjStatus()) {
            ereport(WARNING,  (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The dependent variable is undefined.")));
        }
        InvalidateCurrCompilePgObj();
        return (DependenciesDatum*)makeNode(DependenciesUndefined);
    }
    Oid namespaceOid = InvalidOid;
    DependenciesDatum* ret = NULL;
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            ret = gsplsql_make_var_depend_datum(datum, &namespaceOid, depend_undefined);
        } break;
        case PLPGSQL_DTYPE_RECORD:
        case PLPGSQL_DTYPE_ROW: {
            ret = gsplsql_make_row_var_depend_datum(datum, &namespaceOid, depend_undefined);
        } break;
        case PLPGSQL_DTYPE_REC: {
            ret = gsplsql_make_record_var_depend_datum(datum, &namespaceOid, depend_undefined);
        } break;
        default:
            return NULL;
    }
    return ret;
}

bool gsplsql_check_type_depend_undefined(const char* schemaName, const char* pkg_name, const char* typname, bool isDeleteType)
{
    const GsDependObjDesc obj_desc = {
        (char*)schemaName, (char*)pkg_name, (char*)typname,
        GSDEPEND_OBJECT_TYPE_INVALID, GSDEPEND_REFOBJ_POS_IN_TYPE
    };
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(&obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    int keyNum = 0;
    ScanKeyData key[3];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&schema_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&pkg_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_refobjpos, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(obj_desc.refPosType));
    DependenciesUndefined* undefinedNode = makeNode(DependenciesUndefined);
    char* undefinedAst = nodeToString(undefinedNode);
    pfree_ext(undefinedNode);
    bool is_null = false;
    bool hasUndefine = false;
    HeapTuple tuple;
    Relation dep_rel = heap_open(DependenciesRelationId, AccessShareLock);
    Relation obj_rel = heap_open(DependenciesObjRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(dep_rel, DependenciesNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum obj_datum_tmp = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                   RelationGetDescr(dep_rel), &is_null);
        char* obj_name_tmp = TextDatumGetCString(obj_datum_tmp);
        Datum ref_obj_oid_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjoid,
                                           RelationGetDescr(dep_rel), &is_null);
        if (isDeleteType) {
            hasUndefine = gsplsql_check_type_depend_ast_equal(obj_rel, DatumGetObjectId(ref_obj_oid_datum), undefinedAst);
            if (hasUndefine) {
                gsplsql_delete_type(schemaName, pkg_name, obj_name_tmp);
            }
        } else if (object_name_data.data != NULL && strcmp(object_name_data.data, obj_name_tmp) == 0){
            hasUndefine = gsplsql_check_type_depend_ast_equal(obj_rel, DatumGetObjectId(ref_obj_oid_datum), undefinedAst);
            if (hasUndefine) {
                pfree_ext(obj_name_tmp);
                break;
            }
        }
        pfree_ext(obj_name_tmp);
    }
    systable_endscan(scan);
    heap_close(dep_rel, AccessShareLock);
    heap_close(obj_rel, AccessShareLock);
    FreeStringInfo(&object_name_data);
    pfree_ext(undefinedAst);
    return hasUndefine;
}

DependenciesDatum* gsplsql_make_depend_datum_by_type_oid(const Oid typOid, bool* depend_undefined)
{
    if (!get_typisdefined(typOid)) {
        if (GetCurrCompilePgObjStatus()) {
            ereport(WARNING,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                              errmsg("Type %u does not exist.", typOid)));
        }
        InvalidateCurrCompilePgObj();
        *depend_undefined = true;
        return (DependenciesDatum*)makeNode(DependenciesUndefined);
    }
    bool hasUndefined = false;
    DependenciesType* typNode = gsplsql_make_dependencies_type(typOid);
    if (typNode->typType == TYPTYPE_ENUM) {
        typNode->attrInfo = SerializeEnumAttr(typOid);
    } else if (typNode->typType == TYPTYPE_COMPOSITE) {
        typNode->attrInfo = heap_serialize_row_attr(get_typ_typrelid(typOid), &hasUndefined);
        if (hasUndefined) {
            *depend_undefined = hasUndefined;
            return (DependenciesDatum*)makeNode(DependenciesUndefined);
        }
        typNode->isRel = type_is_relation(typOid);
    } else {
        PLpgSQL_type* typ = NULL;
        if (NULL == u_sess->plsql_cxt.curr_compile_context ||
            NULL == u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile) {
            typ = plpgsql_build_datatype(typOid, -1, 0);
        } else {
            typ = plpgsql_build_datatype(typOid, -1,
                  u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation);
        }
        if (typNode->typType == TYPTYPE_TABLEOF) {
            typNode->elemTypName = MakeTypeNamesStrForTypeOid(get_element_type(typ->typoid), &hasUndefined);
            if (OidIsValid(typ->tableOfIndexType)) {
                typNode->idxByTypName = MakeTypeNamesStrForTypeOid(typ->tableOfIndexType, &hasUndefined);
            }
        } else if (typNode->typType == TYPTYPE_BASE) {
            if (typNode->typCategory == TYPCATEGORY_ARRAY) {
                typNode->elemTypName = MakeTypeNamesStrForTypeOid(get_element_type(typ->typoid), &hasUndefined);
            }
        }
        pfree_ext(typ);
        if (hasUndefined) {
            *depend_undefined = hasUndefined;
            pfree_ext(typNode->elemTypName);
            pfree_ext(typNode->idxByTypName);
            pfree_ext(typNode);
            return (DependenciesDatum*)makeNode(DependenciesUndefined);
        }
    }
    return (DependenciesDatum*)typNode;
}

static List* GetPkgFuncOids(Oid schemaOid, Oid pkgOid)
{
    HeapTuple tuple;
    int keyNum = 0;
    ScanKeyData key[2];
    ScanKeyInit(&key[keyNum++], Anum_pg_proc_pronamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(schemaOid));
    ScanKeyInit(&key[keyNum++], Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pkgOid));
    List* list = NIL;
    Relation relation = heap_open(ProcedureRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, ProcedureNameAllArgsNspIndexId, true, NULL, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        list = lappend_oid(list, HeapTupleGetOid(tuple));
    }
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
    return list;
}

List* gsplsql_delete_objs(Relation relation, const GsDependObjDesc* obj_desc)
{
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    if (obj_desc->refPosType == GSDEPEND_REFOBJ_POS_IN_PKGALL_OBJ) {
        FreeStringInfo(&object_name_data);
    }
    int keyNum = 0;
    ScanKeyData key[2];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&schema_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&pkg_name_data));
    List* list = NIL;
    HeapTuple tuple;
    bool is_null;
    SysScanDesc scan = systable_beginscan(relation, DependenciesNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum ref_pos_type_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjpos,
                                   RelationGetDescr(relation), &is_null);
        if (!((DatumGetInt32(ref_pos_type_datum) & obj_desc->refPosType))) {
            continue;
        }
        Datum obj_datum_tmp = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                                RelationGetDescr(relation), &is_null);
        char* obj_name_tmp = TextDatumGetCString(obj_datum_tmp);
        if (object_name_data.data != NULL && 0 != strcmp(object_name_data.data, obj_name_tmp)) {
            pfree_ext(obj_name_tmp);
            continue;
        }
        pfree_ext(obj_name_tmp);
        Datum ref_obj_oid_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjoid,
                                           RelationGetDescr(relation), &is_null);
        if (!list_member_oid(list, DatumGetObjectId(ref_obj_oid_datum))) {
            list = lappend_oid(list, DatumGetObjectId(ref_obj_oid_datum));
        }
        simple_heap_delete(relation, &tuple->t_self, 0, true);
    }
    systable_endscan(scan);
    FreeStringInfo(&object_name_data);
    CommandCounterIncrement();
    return list;
}

void gsplsql_make_desc_from_gs_dependencies_tuple(HeapTuple gs_depend_tuple, TupleDesc tup_desc,
                                                  GsDependObjDesc *obj_desc)
{
    bool is_null = false;
    Datum schema_name_datum = heap_getattr(gs_depend_tuple, Anum_gs_dependencies_schemaname, tup_desc, &is_null);
    AssertEreport(!is_null, MOD_PLSQL, "schema name must not be null.");
    obj_desc->schemaName = NameStr(*(DatumGetName(schema_name_datum)));
    Datum package_name_datum = heap_getattr(gs_depend_tuple, Anum_gs_dependencies_packagename, tup_desc, &is_null);
    if (!is_null) {
        obj_desc->packageName = NameStr(*(DatumGetName(package_name_datum)));
    }
    Datum obj_name_datum = heap_getattr(gs_depend_tuple, Anum_gs_dependencies_objectname, tup_desc, &is_null);
    AssertEreport(!is_null, MOD_PLSQL, "object name must not be null.");
    obj_desc->name = TextDatumGetCString(obj_name_datum);
    Datum refobjpos_datum = heap_getattr(gs_depend_tuple, Anum_gs_dependencies_refobjpos, tup_desc, &is_null);
    AssertEreport(!is_null, MOD_PLSQL, "refobjpos must not be null.");
    obj_desc->refPosType = DatumGetInt32(refobjpos_datum);
}

void gsplsql_prepare_gs_depend_for_pkg_compile(PLpgSQL_package* pkg, bool isCreate)
{
    if (pkg == NULL) {
        return;
    }
    AssertEreport(!pkg->preRefObjectOidList, MOD_PLSQL, "preRefObjectOidList must not be null.");
    AssertEreport(!pkg->preSelfObjectList, MOD_PLSQL, "preSelfObjectList must not be null.");
    char* schema_name =get_namespace_name(pkg->namespaceOid);
    (void)gsplsql_check_type_depend_undefined(schema_name, pkg->pkg_signature, NULL, true);
    Relation relation = heap_open(DependenciesRelationId, RowExclusiveLock);
    GsDependObjDesc obj_desc = {
        schema_name,
        pkg->pkg_signature,
        NULL,
        GSDEPEND_OBJECT_TYPE_INVALID,
        isCreate ? GSDEPEND_REFOBJ_POS_IN_PKGALL_OBJ : GSDEPEND_REFOBJ_POS_IN_PKGRECOMPILE_OBJ
    };
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(&obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    uint32 hash_value = string_hash(object_name_data.data, object_name_data.len);
    LockDatabaseObject(DependenciesRelationId, hash_value, 0, AccessExclusiveLock);
    pkg->preRefObjectOidList = gsplsql_delete_objs(relation, &obj_desc);
    heap_close(relation, RowExclusiveLock);
    pkg->preSelfObjectList = gsplsql_get_depend_obj_list_by_specified_pkg(schema_name, pkg->pkg_signature,
        isCreate ? GSDEPEND_OBJECT_TYPE_PKG : GSDEPEND_OBJECT_TYPE_PKG_RECOMPILE);
    pfree_ext(schema_name);
}

void gsplsql_complete_gs_depend_for_pkg_compile(PLpgSQL_package* pkg, bool isCreate, bool isRecompile)
{
    if (pkg == NULL) {
        return;
    }
    complete_process_variables(pkg, isCreate, isRecompile);
    ListCell* lc = NULL;
    List* tmp_pre_self_obj_list = list_copy(pkg->preSelfObjectList);
    List* tmp_pre_ref_obj_oid_list = list_copy(pkg->preRefObjectOidList);
    list_free_ext(pkg->preSelfObjectList);
    list_free_ext(pkg->preRefObjectOidList);
    Oid pkg_oid = pkg->pkg_oid;
    foreach (lc, tmp_pre_self_obj_list) {
        Oid ref_obj_oid = lfirst_oid(lc);
        gsplsql_remove_depend_obj_by_specified_oid(ref_obj_oid, pkg_oid, list_member_oid(tmp_pre_ref_obj_oid_list, ref_obj_oid));
    }
    gsplsql_delete_unrefer_depend_obj_in_list(tmp_pre_ref_obj_oid_list, false);
    list_free_ext(tmp_pre_self_obj_list);
    list_free_ext(tmp_pre_ref_obj_oid_list);
}

bool gsplsql_parse_type_and_name_from_gsplsql_datum(const PLpgSQL_datum* datum, char** datum_name,
                                                            GsDependObjectType* datum_type)
{
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            *datum_name = var->refname;
            *datum_type = GSDEPEND_OBJECT_TYPE_VARIABLE;
            return var->addNamespace;
        } break;
        case PLPGSQL_DTYPE_RECORD:
        case PLPGSQL_DTYPE_ROW: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;
            *datum_name = row->refname;
            *datum_type = GSDEPEND_OBJECT_TYPE_VARIABLE;
            return row->addNamespace;
        } break;
        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;
            *datum_name = rec->refname;
            *datum_type = GSDEPEND_OBJECT_TYPE_VARIABLE;
            return rec->addNamespace;
        } break;
        default:
            return false;
    }
    return true;
}

Oid gsplsql_parse_pkg_var_obj4(GsDependObjDesc* obj, List* var_name)
{
    ListCell* attr_list = NULL;
    Oid oid = gsplsql_parse_pkg_var_and_attrs4(obj, var_name, &attr_list);
    return oid;
}

/*
 * static function
 */
static inline void gsplsql_delete_type(const char *schemaName, const char *pkgName, char *typName)
{
    ObjectAddress address;
    Oid typeOid = TypeNameGetOid(schemaName, pkgName, typName);
    address.classId = TypeRelationId;
    address.objectId = typeOid;
    address.objectSubId = 0;
    HeapTuple tuple = SearchSysCache1((int)TYPEOID, ObjectIdGetDatum(typeOid));
    if (HeapTupleIsValid(tuple)) {
        performDeletion(&address, DROP_CASCADE, PERFORM_DELETION_INTERNAL);
        ReleaseSysCache(tuple);
    }
}

static inline bool gsplsql_invalidate_func(Oid func_oid, Oid curr_compile_oid)
{
    if (!OidIsValid(func_oid) || func_oid == curr_compile_oid) {
        return false;
    }
    bool result = SetPgObjectValid(func_oid, OBJECT_TYPE_PROC, false);
    CacheInvalidateFunction(func_oid, InvalidOid);
    return result;
}

static bool gsplsql_invalidate_pkg(const char *schemaName, const char *pkgName, bool isSpec, Oid currCompileOid)
{
    Oid schemaOid = get_namespace_oid(schemaName, true);
    Oid pkgOid = InvalidOid;
    if (pkgName != NULL) {
        pkgOid = PackageNameGetOid(pkgName, schemaOid);
    }
    if (!OidIsValid(pkgOid) || pkgOid == currCompileOid) {
        return false;
    }
    bool result = false;
    if (isSpec) {
        result = SetPgObjectValid(pkgOid, OBJECT_TYPE_PKGSPEC, false) || result;
    }
    result = SetPgObjectValid(pkgOid, OBJECT_TYPE_PKGBODY, false) || result;
    result = gsplsql_set_pkg_func_status(schemaOid, pkgOid, false) || result;
    return result;
}

static bool gsplsql_invalidate_obj(GsDependObjDesc* obj, Datum refobjpos_datum,
                                    bool is_ref_remove, Oid curr_compile_oid)
{
    if (obj->schemaName == NULL || obj->name == NULL) {
        return false;
    }
    int ref_pos = DatumGetInt32(refobjpos_datum);
    switch (ref_pos) {
        case GSDEPEND_REFOBJ_POS_IN_TYPE:
            if (obj->packageName != NULL && strcmp(obj->packageName, "null") != 0) {
                return gsplsql_invalidate_pkg(obj->schemaName, obj->packageName, true, curr_compile_oid);
            }
            break;
        case GSDEPEND_REFOBJ_POS_IN_PKGSPEC:
            return gsplsql_invalidate_pkg(obj->schemaName, obj->packageName, true, curr_compile_oid);
        case GSDEPEND_REFOBJ_POS_IN_PKGBODY:
            return gsplsql_invalidate_pkg(obj->schemaName, obj->packageName, false, curr_compile_oid);
        case GSDEPEND_REFOBJ_POS_IN_PROCHEAD:
        case GSDEPEND_REFOBJ_POS_IN_PROCBODY: {
            Oid func_oid = gsplsql_get_proc_oid(obj->schemaName, obj->packageName, obj->name);
            HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
            if (!HeapTupleIsValid(tuple)) {
                return false;
            }
            bool is_null = false;
            Datum proisprivate_datum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proisprivate, &is_null);
            if (is_null) {
                proisprivate_datum = BoolGetDatum(false);
            }
            ReleaseSysCache(tuple);
            if (obj->packageName == NULL || strcmp(obj->packageName, "null") == 0) {
                return gsplsql_invalidate_func(func_oid, curr_compile_oid);
            } else if (DatumGetBool(proisprivate_datum)) {
                return gsplsql_invalidate_pkg(obj->schemaName, obj->packageName, false, curr_compile_oid); //pkg body
            } else {
                return gsplsql_invalidate_pkg(obj->schemaName, obj->packageName, true, curr_compile_oid);
            }
        }
        default:
            return false;
    }
    return false;
}

static bool gsplsql_check_var_attrs(PLpgSQL_datum *datum, ListCell *attr_list)
{
    if (attr_list == NULL) {
        return true;
    }
    char* first_attr_str = strVal(lfirst(attr_list));
    if (first_attr_str == NULL) {
        return true;
    }
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR: {
            return false;
        } break;
        case PLPGSQL_DTYPE_RECORD: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;
            for (int i = 0; i < row->nfields; i++) {
                if (row->fieldnames[i] != NULL && strcmp(first_attr_str, row->fieldnames[i]) == 0) {
                    return true;
                }
            }
            return false;
        } break;
        case PLPGSQL_DTYPE_ROW: {
            PLpgSQL_row* row = (PLpgSQL_row*)datum;
            if (row->rowtupdesc == NULL) {
                return false;
            }
            for (int i = 0; i < row->rowtupdesc->natts; i++) {
                if (strcmp(first_attr_str, row->rowtupdesc->attrs[i].attname.data) == 0) {
                    return true;
                }
            }
            return false;
        } break;
        case PLPGSQL_DTYPE_REC: {
            PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;
            if (rec->tupdesc == NULL) {
                return false;
            }
            for (int i = 0; i < rec->tupdesc->natts; i++) {
                if (strcmp(first_attr_str, rec->tupdesc->attrs[i].attname.data) == 0) {
                    return true;
                }
            }
            return false;
        } break;
        default:
            return true;
    }
}

static void gsplsql_insert_gs_dependency(Relation relation, NameData* schema_name_data,
                                         NameData* pkg_name_data, StringInfo object_name,
                                         int ref_obj_pos, Oid ref_obj_oid)
{
    Datum values[Natts_gs_dependencies];
    bool nulls[Natts_gs_dependencies] = {false};
    values[Anum_gs_dependencies_schemaname - 1] = NameGetDatum(schema_name_data);
    values[Anum_gs_dependencies_packagename - 1] = NameGetDatum(pkg_name_data);
    values[Anum_gs_dependencies_objectname - 1] = CStringGetTextDatum(object_name->data);
    values[Anum_gs_dependencies_refobjpos - 1] = NameGetDatum(ref_obj_pos);
    values[Anum_gs_dependencies_refobjoid - 1] = NameGetDatum(ref_obj_oid);
    HeapTuple tup = heap_form_tuple(RelationGetDescr(relation), values, nulls);
    (void)simple_heap_insert(relation, tup);
    CatalogUpdateIndexes(relation, tup);
    heap_freetuple(tup);
}

static inline void gsplsql_free_var_mem_for_gs_dependencies(GsDependObjDesc* obj)
{
    pfree_ext(obj->schemaName);
    pfree_ext(obj->packageName);
    pfree_ext(obj->name);
}

static inline bool gsplsql_is_proc_head_depend_type(const GsDependObjDesc* obj_desc, const GsDependObjDesc* ref_obj_desc)
{
    return obj_desc->refPosType == GSDEPEND_REFOBJ_POS_IN_PROCHEAD && ref_obj_desc->type == GSDEPEND_OBJECT_TYPE_TYPE;
}

static inline Oid gsplsql_parse_pkg_var_obj2(GsDependObjDesc* obj, ListCell* var_name)
{
    Oid schema_oid = get_compiled_object_nspoid();
    obj->schemaName = get_namespace_name(schema_oid);
    obj->packageName = pstrdup(strVal(lfirst(var_name)));
    obj->name= pstrdup(strVal(lfirst(lnext(var_name))));
    return schema_oid;
}

static inline Oid gsplsql_parse_pkg_var_and_attrs3(GsDependObjDesc* obj, ListCell* var_name, ListCell** attr_list)
{
    Oid schema_oid = InvalidOid;
    char* first_str = strVal(lfirst(var_name));
    schema_oid = get_namespace_oid(first_str, true);
    if (!OidIsValid(schema_oid)) {
        *attr_list = lnext(lnext(var_name));
        return gsplsql_parse_pkg_var_obj2(obj, var_name);
    }
    obj->schemaName = pstrdup(strVal(lfirst(var_name)));
    obj->packageName = pstrdup(strVal(lfirst(lnext(var_name))));
    obj->name= pstrdup(strVal(lfirst(lnext(lnext(var_name)))));
    schema_oid = get_namespace_oid(obj->schemaName, true);
    return schema_oid;
}

static inline Oid gsplsql_parse_pkg_var_and_attrs4(GsDependObjDesc* obj, List* var_name, ListCell** attr_list)
{
    Oid schema_oid = InvalidOid;
    char* first_str = strVal(linitial(var_name));
    schema_oid = get_namespace_oid(first_str, true);
    if (OidIsValid(schema_oid)) { //schema.pkg.row.col
        *attr_list = list_nth_cell(var_name, 3); // 3->attr start location
        return gsplsql_parse_pkg_var_and_attrs3(obj, list_head(var_name), attr_list);
    } else if (IsExistPackageName(first_str)) { //pkg.var.attr.attr
        *attr_list = list_nth_cell(var_name, 2); // 2->attr start location
        return gsplsql_parse_pkg_var_obj2(obj, list_head(var_name));
    } else {
        //check catalog name and then ignore it
        char* dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbname == NULL || strcmp(first_str, dbname) != 0) {
            ereport(ERROR,  (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                              errmsg("Cross-database references are not implemented: \"%s\".", NameListToString(var_name))));
        }
        char* second_str = strVal(lsecond(var_name));
        schema_oid = get_namespace_oid(second_str, true);
        if (OidIsValid(schema_oid)) {  // db.schema.pkg.var
            obj->schemaName = pstrdup(strVal(lsecond(var_name)));
            obj->packageName = pstrdup(strVal(lthird(var_name)));
            obj->name= pstrdup(strVal(lfourth(var_name)));
            return schema_oid;
        }
    }
    return schema_oid;
}

static void do_complete_process_variables(PLpgSQL_package* pkg, bool isCreate, bool isRecompile, PLpgSQL_datum* datum)
{
    if (isCreate || isRecompile) {
        (void)gsplsql_build_ref_dependency(pkg->namespaceOid, datum, pkg->pkg_signature,
                                           NULL != pkg->preSelfObjectList ? &pkg->preSelfObjectList : NULL, pkg->pkg_oid);
    } else {
        GsDependObjDesc  ref_obj_desc;
        gsplsql_init_gs_depend_obj_desc(&ref_obj_desc);
        bool succeed = gsplsql_parse_type_and_name_from_gsplsql_datum(datum, &ref_obj_desc.name, &ref_obj_desc.type);
        if (!succeed) {
            return;
        }
        ref_obj_desc.schemaName = get_namespace_name(pkg->namespaceOid);
        ref_obj_desc.packageName = (char*)pkg->pkg_signature;
        ref_obj_desc.type = GSDEPEND_OBJECT_TYPE_TYPE;
        HeapTuple tuple = gsplsql_search_object(&ref_obj_desc, true);
        if (!HeapTupleIsValid(tuple)) {
            return;
        }
        Oid refObjOid = HeapTupleGetOid(tuple);
        heap_freetuple(tuple);
        pkg->preSelfObjectList = list_delete_oid(pkg->preSelfObjectList, refObjOid);
    }
}

static void complete_process_variables(PLpgSQL_package* pkg, bool isCreate, bool isRecompile)
{
    for (int i = 0; i < pkg->ndatums; i++) {
        PLpgSQL_datum* datum = pkg->datums[i];
        if (datumIsVariable(datum)) {
            do_complete_process_variables(pkg, isCreate, isRecompile, datum);
        }
    }
}

static Oid gsplsql_parse_exist_pkg_and_build_obj_desc(GsDependObjDesc* obj, List* var_name, ListCell** attr_list)
{
    Oid schema_oid = InvalidOid;
    int length = list_length(var_name);
    if (length == 2) { // pkg.var
        schema_oid = gsplsql_parse_pkg_var_obj2(obj, list_head(var_name));
    } else if (length == 3) { //schema.pkg.var
        schema_oid = gsplsql_parse_pkg_var_and_attrs3(obj, list_head(var_name), attr_list);
    } else if (length == 4) { // a.b.c.d
        schema_oid = gsplsql_parse_pkg_var_and_attrs4(obj, var_name, attr_list);
    } else {
        ereport(WARNING,  (errmodule(MOD_PLSQL), errcode(ERRCODE_NONEXISTANT_VARIABLE),
                          errmsg("Unrecognized package variable.")));
        return InvalidOid;
    }
    obj->type = GSDEPEND_OBJECT_TYPE_VARIABLE;
    if (!OidIsValid(schema_oid)) {
        return InvalidOid;
    }
    Oid pkg_oid = PackageNameGetOid(obj->packageName, schema_oid);
    if (NULL != u_sess->plsql_cxt.curr_compile_context &&
        NULL != u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package) {
        if (pkg_oid == u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid) {
            return InvalidOid;
        }
    }
    return pkg_oid;
}

static bool datumIsVariable(PLpgSQL_datum* datum)
{
    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR:
        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_REC:
        case PLPGSQL_DTYPE_RECORD:
            return true;
        default:
            break;
    }
    return false;
}

static DependenciesType *gsplsql_make_dependencies_type(Oid typOid)
{
    DependenciesType* typNode = makeNode(DependenciesType);
    typNode->typType = get_typtype(typOid);
    typNode->typCategory = get_typecategory(typOid);
    typNode->isRel = false;
    typNode->attrInfo = NULL;
    typNode->elemTypName = NULL;
    typNode->idxByTypName = NULL;
    return typNode;
}

static DependenciesDatum *gsplsql_make_record_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined)
{
    DependenciesVariable* var_node = makeNode(DependenciesVariable);
    PLpgSQL_rec* rec = (PLpgSQL_rec*)datum;
    Oid typOid = InvalidOid;
    if (rec->tupdesc == NULL) {
        return NULL;
    }
    var_node->typName = MakeTypeNamesStrForTypeOid(typOid, depend_undefined);
    if (*depend_undefined) {
        pfree_ext(var_node->typName);
        return (DependenciesDatum*)makeNode(DependenciesUndefined);
    }
    var_node->typMod = rec->tupdesc->tdtypmod;
    var_node->extraInfo = NULL;
    if (NULL != rec->pkg) {
        *namespace_oid = rec->pkg->namespaceOid;
    }
    return (DependenciesDatum*)var_node;
}
static DependenciesDatum *gsplsql_make_row_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined)
{
    DependenciesVariable* var_node = makeNode(DependenciesVariable);
    PLpgSQL_row* row = (PLpgSQL_row*)datum;
    Oid typOid = InvalidOid;
    if (row->rowtupdesc == NULL) {
        return NULL;
    }
    if (datum->dtype == PLPGSQL_DTYPE_ROW) {
        typOid = row->rowtupdesc->tdtypeid;
    } else {
        typOid = row->recordVarTypOid;
    }
    if (!OidIsValid(typOid)) {
        return NULL;
    }
    var_node->typName = MakeTypeNamesStrForTypeOid(typOid, depend_undefined);
    if (*depend_undefined) {
        pfree_ext(var_node->typName);
        return (DependenciesDatum*)makeNode(DependenciesUndefined);
    }
    var_node->typMod = row->rowtupdesc->tdtypmod;
    var_node->extraInfo = NULL;
    if (NULL != row->pkg) {
        *namespace_oid = row->pkg->namespaceOid;
    }
    return (DependenciesDatum*)var_node;
}
static DependenciesDatum *gsplsql_make_var_depend_datum(const PLpgSQL_datum *datum, Oid *namespace_oid, bool *depend_undefined)
{
    DependenciesVariable* var_node = makeNode(DependenciesVariable);
    PLpgSQL_var* var = (PLpgSQL_var*)datum;
    if (var->datatype == NULL) {
        return NULL;
    }
    var_node->typName = MakeTypeNamesStrForTypeOid(var->datatype->typoid, depend_undefined);
    if (*depend_undefined) {
        pfree_ext(var_node->typName);
        return (DependenciesDatum*)makeNode(DependenciesUndefined);
    }
    var_node->typMod = var->datatype->atttypmod;
    if (var->cursor_explicit_expr != NULL && var->cursor_explicit_expr->query != NULL) {
        var_node->extraInfo = pstrdup(var->cursor_explicit_expr->query);
    }
    if (NULL != var->pkg) {
        *namespace_oid = var->pkg->namespaceOid;
    }
    return (DependenciesDatum*)var_node;
}

static bool gsplsql_exist_func_object_in_dependencies_obj(char* nsp_name, char* pkg_name,
    const char* old_func_head_name, const char* old_func_name)
{
    int keyNum = 0;
    Assert(nsp_name != NULL && pkg_name != NULL);
    ScanKeyData key[2];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_schemaname, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(nsp_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_packagename, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(pkg_name));
    bool is_null = false;
    HeapTuple tuple;
    Relation obj_rel = heap_open(DependenciesObjRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(obj_rel, DependenciesObjNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum name_datum = heap_getattr(tuple, Anum_gs_dependencies_obj_name,
                                        RelationGetDescr(obj_rel), &is_null);
        Assert(name_datum != 0);
        char* obj_name = TextDatumGetCString(name_datum);
        if (strcmp(old_func_head_name, obj_name) == 0 || strcmp(old_func_name, obj_name) == 0) {
            Datum type_datum = heap_getattr(tuple, Anum_gs_dependencies_obj_type,
                                            RelationGetDescr(obj_rel), &is_null);
            int type = DatumGetInt32(type_datum);
            if (type == GSDEPEND_OBJECT_TYPE_FUNCTION ||
                type == GSDEPEND_OBJECT_TYPE_PROCHEAD || type == GSDEPEND_OBJECT_TYPE_UNDEFIND) {
                pfree_ext(obj_name);
                systable_endscan(scan);
                heap_close(obj_rel, AccessShareLock);
                return true;
            }
        }
        pfree_ext(obj_name);
    }
    systable_endscan(scan);
    heap_close(obj_rel, AccessShareLock);
    return false;
}

static bool gsplsql_exist_func_object_in_dependencies(char* nsp_name, char* pkg_name, const char* old_func_head_name)
{
    int keyNum = 0;
    Assert(nsp_name != NULL && pkg_name != NULL);
    ScanKeyData key[2];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(nsp_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber,
        F_NAMEEQ, NameGetDatum(pkg_name));
    bool is_null = false;
    HeapTuple tuple;
    Relation dep_rel = heap_open(DependenciesRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(dep_rel, DependenciesNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum name_datum = heap_getattr(tuple, Anum_gs_dependencies_objectname,
                                   RelationGetDescr(dep_rel), &is_null);
        Assert(name_datum != 0);
        char* obj_name = TextDatumGetCString(name_datum);
        if (strcmp(old_func_head_name, obj_name) == 0) {
            Datum refobj_pos_datum = heap_getattr(tuple, Anum_gs_dependencies_refobjpos,
                                                  RelationGetDescr(dep_rel), &is_null);
            int refobj_pos = DatumGetInt32(refobj_pos_datum);
            if (refobj_pos == GSDEPEND_REFOBJ_POS_IN_PROCHEAD ||
                refobj_pos == GSDEPEND_REFOBJ_POS_IN_PROCBODY) {
                pfree_ext(obj_name);
                systable_endscan(scan);
                heap_close(dep_rel, AccessShareLock);
                return true;
            }
        }
        pfree_ext(obj_name);
    }
    systable_endscan(scan);
    heap_close(dep_rel, AccessShareLock);
    return false;
}
